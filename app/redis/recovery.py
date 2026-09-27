"""Process-local Redis recovery and protection admission coordination."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID, uuid4

from app.redis.circuit import CircuitState
from app.redis.local_degraded import LocalDegradedProtection

logger = logging.getLogger(__name__)


class TrafficMode(StrEnum):
    NORMAL = "NORMAL"
    DEGRADED_REDIS = "DEGRADED_REDIS"
    RECOVERING_REDIS = "RECOVERING_REDIS"


class Backend(StrEnum):
    REDIS = "REDIS"
    LOCAL = "LOCAL"


class CircuitCompletionDisposition(StrEnum):
    APPLIED = "APPLIED"
    STALE = "STALE"
    UNKNOWN_HANDLE = "UNKNOWN_HANDLE"


@dataclass(frozen=True, slots=True)
class CircuitCompletionResult:
    applied: bool
    resulting_state: CircuitState | None
    disposition: CircuitCompletionDisposition

    def __post_init__(self) -> None:
        valid = (
            (
                self.disposition is CircuitCompletionDisposition.APPLIED
                and self.applied
                and isinstance(self.resulting_state, CircuitState)
            )
            or (
                self.disposition is CircuitCompletionDisposition.STALE
                and not self.applied
                and isinstance(self.resulting_state, CircuitState)
            )
            or (
                self.disposition is CircuitCompletionDisposition.UNKNOWN_HANDLE
                and not self.applied
                and self.resulting_state is None
            )
        )
        if not valid:
            raise ValueError("Circuit completion result is inconsistent.")


class RecoveryCoordinator:
    """Own one process-wide new-admission backend and local generation chain."""

    def __init__(
        self,
        redis_runtime,
        subscriber,
        reconciler,
        registry,
        *,
        local_factory: Callable[[], LocalDegradedProtection] = LocalDegradedProtection,
        initial_local: LocalDegradedProtection | None = None,
        clock: Callable[[], float] = time.monotonic,
        lease_duration_ms: int = 30000,
        cadence_seconds: float = 1.0,
        reconcile_timeout_seconds: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if cadence_seconds < 1.0:
            raise ValueError("Recovery cadence must be at least one second.")
        if reconcile_timeout_seconds <= 0:
            raise ValueError("Reconciliation timeout must be positive.")
        self._redis_runtime = redis_runtime
        self._subscriber = subscriber
        self._reconciler = reconciler
        self._registry = registry
        self._local_factory = local_factory
        self._initial_local = initial_local
        self._clock = clock
        self._lease_duration_seconds = lease_duration_ms / 1000
        self._cadence_seconds = cadence_seconds
        self._reconcile_timeout_seconds = reconcile_timeout_seconds
        self._sleep = sleep
        self._mode = TrafficMode.NORMAL
        self._active: LocalDegradedProtection | None = None
        self._active_id: int | None = None
        self._retired: dict[int, LocalDegradedProtection] = {}
        self._normal_deadlines: dict[int, float] = {}
        self._handle_deadlines: dict[int, dict[UUID, float]] = {}
        self._next_generation = 1
        self._owner_id: UUID = uuid4()
        self._admission_lock = asyncio.Lock()
        self._attempt_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    @property
    def mode(self) -> TrafficMode:
        return self._mode

    @property
    def active_generation(self) -> LocalDegradedProtection | None:
        return self._active

    @property
    def active_generation_id(self) -> int | None:
        return self._active_id

    @property
    def retired_generations(self) -> dict[int, LocalDegradedProtection]:
        return dict(self._retired)

    @property
    def owner_id(self) -> UUID:
        return self._owner_id

    def clock(self) -> float:
        return self._clock()

    @property
    def lease_duration_seconds(self) -> float:
        return self._lease_duration_seconds

    def note_local_completion(
        self, generation_id: int, deadline: float, handle_id: UUID | None = None
    ) -> None:
        if handle_id is None:
            self._normal_deadlines[generation_id] = max(
                deadline, self._normal_deadlines.get(generation_id, 0.0)
            )
        else:
            self._handle_deadlines.setdefault(generation_id, {})[handle_id] = deadline

    def forget_local_completion(self, generation_id: int, handle_id: UUID) -> None:
        self._handle_deadlines.get(generation_id, {}).pop(handle_id, None)

    def cleanup_retired(self) -> None:
        """Retire only after every issued completion bound has elapsed."""
        now = self._clock()
        for generation_id in tuple(self._retired):
            handles = self._handle_deadlines.get(generation_id, {})
            for handle_id, deadline in tuple(handles.items()):
                if now >= deadline:
                    del handles[handle_id]
            if now >= self._normal_deadlines.get(generation_id, 0.0) and not handles:
                del self._retired[generation_id]
                self._normal_deadlines.pop(generation_id, None)
                self._handle_deadlines.pop(generation_id, None)

    def local_generation(self, generation_id: int) -> LocalDegradedProtection | None:
        if generation_id == self._active_id:
            return self._active
        return self._retired.get(generation_id)

    def mark_unreachable(self) -> None:
        """Called on a narrowly classified M3-R7 dependency failure."""
        if self._mode is TrafficMode.NORMAL:
            self._active = self._initial_local or self._local_factory()
            self._initial_local = None
            self._active_id = self._next_generation
            self._next_generation += 1
            self._active.mark_unreachable()
        self._mode = TrafficMode.DEGRADED_REDIS

    @asynccontextmanager
    async def admission(self) -> AsyncIterator[LocalDegradedProtection | None]:
        """Serialize a complete new decision with the process-wide cutover."""
        async with self._admission_lock:
            yield self._active

    async def attempt_recovery(self) -> str:
        if self._attempt_lock.locked():
            return "recovery_in_progress"
        async with self._attempt_lock:
            if self._mode is not TrafficMode.DEGRADED_REDIS:
                return "not_degraded"
            if not (await self._redis_runtime.check()).available:
                return "redis_not_ready"
            async with self._admission_lock:
                if self._mode is not TrafficMode.DEGRADED_REDIS:
                    return "not_degraded"
                active = self._active
                self._mode = TrafficMode.RECOVERING_REDIS
            try:
                if not self._subscriber.ready:
                    return "subscriber_not_ready"
                subscription_epoch = getattr(self._subscriber, "subscription_epoch", 0)
                members = self._registry.tenant_ids()
                try:
                    outcome = await asyncio.wait_for(
                        self._reconciler.reconcile_once(),
                        timeout=self._reconcile_timeout_seconds,
                    )
                except TimeoutError:
                    return "config_reconciliation_timeout"
                if outcome == "pass_error" or any(
                    not self._registry.is_tenant_reconciled(tenant)
                    for tenant in members
                ):
                    return "config_not_reconciled"
                epoch = self._registry.invalidation_epoch
                async with self._admission_lock:
                    if not (await self._redis_runtime.check()).available:
                        return "final_redis_check_failed"
                    if not self._subscriber.ready:
                        return "subscriber_not_ready"
                    if (
                        getattr(self._subscriber, "subscription_epoch", 0)
                        != subscription_epoch
                    ):
                        return "subscriber_reconnected"
                    if (
                        self._registry.invalidation_epoch != epoch
                        or self._registry.tenant_ids() != members
                        or any(
                            not self._registry.is_tenant_reconciled(tenant)
                            for tenant in members
                        )
                    ):
                        return "certificate_invalidated"
                    if (
                        self._mode is not TrafficMode.RECOVERING_REDIS
                        or self._active is not active
                    ):
                        return "recovery_interrupted"
                    assert self._active_id is not None
                    self._retired[self._active_id] = active
                    self._active = None
                    self._active_id = None
                    self._mode = TrafficMode.NORMAL
                    return "recovery_succeeded"
            finally:
                if self._mode is TrafficMode.RECOVERING_REDIS:
                    self._mode = TrafficMode.DEGRADED_REDIS

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="redis-recovery")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while True:
            await self._sleep(self._cadence_seconds)
            self.cleanup_retired()
            if self._mode is TrafficMode.DEGRADED_REDIS:
                try:
                    await self.attempt_recovery()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - bounded retry, no raw details
                    logger.warning("Redis recovery attempt failed")
