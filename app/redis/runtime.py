from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from redis.asyncio import Redis

from app.redis.telemetry import BoundedEvent, bounded_dimension

logger = logging.getLogger(__name__)


class RedisAvailability(StrEnum):
    UNKNOWN = "UNKNOWN"
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class RedisTelemetryEvent:
    operation: str
    outcome: str
    latency_ms: float


@dataclass(frozen=True, slots=True)
class RedisHealthResult:
    available: bool
    state: RedisAvailability
    latency_ms: float


class RedisTelemetry(Protocol):
    def record_operation(self, event: RedisTelemetryEvent) -> None: ...

    def record_transition(
        self,
        previous: RedisAvailability,
        current: RedisAvailability,
    ) -> None: ...


class LoggingRedisTelemetry:
    """Emit bounded dependency telemetry without connection or command details."""

    def record_operation(self, event: RedisTelemetryEvent) -> None:
        safe = BoundedEvent(
            "redis",
            {
                "operation": bounded_dimension("operation", event.operation),
                "outcome": bounded_dimension("outcome", event.outcome),
            },
            latency_ms=event.latency_ms,
        )
        logger.info(
            "Redis dependency operation",
            extra={
                "dependency": "redis",
                "operation_class": safe.dimensions["operation"],
                "operation_outcome": safe.dimensions["outcome"],
                "latency_ms": safe.latency_ms,
            },
        )

    def record_transition(
        self,
        previous: RedisAvailability,
        current: RedisAvailability,
    ) -> None:
        logger.info(
            "Redis dependency state transition",
            extra={
                "dependency": "redis",
                "previous_state": previous.value,
                "current_state": current.value,
            },
        )


RedisFactory = Callable[..., Redis]


class RedisDisposalError(RuntimeError):
    """Fixed resource names retain failed obligations without dependency details."""

    def __init__(self, resources: tuple[str, ...]) -> None:
        self.resources = resources
        super().__init__("Redis disposal unconfirmed: " + ", ".join(resources))


class RedisRuntime:
    """Own one reconnect-capable async Redis client for an application lifespan."""

    def __init__(
        self,
        redis_url: str,
        timeout_seconds: float,
        *,
        factory: RedisFactory = Redis.from_url,
        telemetry: RedisTelemetry | None = None,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._redis_url = redis_url
        self._timeout_seconds = timeout_seconds
        self._factory = factory
        self._telemetry = telemetry or LoggingRedisTelemetry()
        self._monotonic = monotonic
        self._client: Redis | None = None
        self._guarded_renewal_transport = None
        self._state = RedisAvailability.UNKNOWN
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._cleanup_scope = None
        self._disposal_states: dict[str, str] = {}
        self._close_error: RedisDisposalError | None = None
        self._legacy_pool_disposal = None
        self._late_managed_binding = False
        self._lifecycle_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

    @property
    def state(self) -> RedisAvailability:
        return self._state

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("Redis runtime has not been started.")
        return self._client

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._client is not None:
                return
            if self._closed:
                raise RuntimeError("Redis runtime has already been closed.")
            self._client = self._factory(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=self._timeout_seconds,
                socket_timeout=self._timeout_seconds,
            )
            from redis.asyncio.connection import ConnectionPool

            from app.redis.guarded_renewal import observe_pool_ownership

            pool = getattr(self._client, "connection_pool", None)
            if isinstance(pool, ConnectionPool):
                observe_pool_ownership(pool)

    @property
    def guarded_renewal_transport(self):
        from app.redis.guarded_renewal import (
            GuardedRenewalTransport,
            GuardedTransportClosed,
        )

        if self._closed:
            raise GuardedTransportClosed()
        if self._guarded_renewal_transport is None:
            self._guarded_renewal_transport = GuardedRenewalTransport(
                self.client, self._timeout_seconds
            )
        self._guarded_renewal_transport.assert_owner()
        return self._guarded_renewal_transport

    async def check(self) -> RedisHealthResult:
        client = self.client
        started_at = self._monotonic()
        available = False
        try:
            async with asyncio.timeout(self._timeout_seconds):
                available = bool(await client.ping())
        except Exception:  # noqa: BLE001 - dependency failures become safe status
            available = False

        latency_ms = max(0.0, (self._monotonic() - started_at) * 1000)
        current = (
            RedisAvailability.AVAILABLE if available else RedisAvailability.UNAVAILABLE
        )
        self._telemetry.record_operation(
            RedisTelemetryEvent(
                operation="ping",
                outcome="success" if available else "failure",
                latency_ms=latency_ms,
            )
        )
        await self._set_state(current)
        return RedisHealthResult(available, current, latency_ms)

    @property
    def disposal_states(self) -> dict[str, str]:
        return dict(self._disposal_states)

    def _can_start_disposal(self) -> bool:
        return self._cleanup_scope is None or not self._cleanup_scope.expired

    async def close(self, *, cleanup_scope=None) -> None:
        async with self._lifecycle_lock:
            if self._guarded_renewal_transport is not None:
                self._guarded_renewal_transport.assert_owner()
            # Retain the original live authority; a later caller cannot reset it.
            if self._cleanup_scope is None and cleanup_scope is not None:
                # One owning loop: fence both pools before any disposal handoff.
                pool = getattr(self._client, "connection_pool", None)
                owners = [getattr(pool, "_gateway_managed_owner", None)]
                if self._guarded_renewal_transport is not None:
                    owners.append(
                        getattr(self._guarded_renewal_transport, "ownership", None)
                    )
                for owner in owners:
                    if owner is not None:
                        owner.validate_scope(cleanup_scope)
                for owner in owners:
                    if owner is not None:
                        owner.begin_close(cleanup_scope)
                self._late_managed_binding = self._close_task is not None
                self._cleanup_scope = cleanup_scope
            if self._close_error is not None:
                raise self._close_error
            if self._close_task is None:
                self._closed = True
                if self._guarded_renewal_transport is not None:
                    self._guarded_renewal_transport.latch_close()
                    self._disposal_states["guarded"] = "not_attempted"
                if self._client is not None:
                    self._disposal_states["legacy"] = "not_attempted"
                if self._disposal_states and not self._can_start_disposal():
                    self._close_error = RedisDisposalError(tuple(self._disposal_states))
                    raise self._close_error
                self._close_task = asyncio.create_task(self._dispose())
                self._close_task.add_done_callback(self._observe_close)
            disposal = self._close_task
        # The lifecycle owner bounds its own wait; cancellation cannot erase disposal.
        await asyncio.shield(disposal)
        if self._late_managed_binding:
            # An already-running unmanaged disposal cannot retroactively certify
            # managed ownership/task-entry checks. Admission remains fenced.
            for resource in self._disposal_states:
                self._disposal_states[resource] = "unresolved"
            raise RedisDisposalError(tuple(self._disposal_states))

    async def _dispose(self) -> None:
        from app.redis.guarded_renewal import GuardedDisposalNotStarted

        failed = []
        resources = []
        if self._guarded_renewal_transport is not None:
            resources.append(("guarded", self._close_guarded))
        if self._client is not None:
            resources.append(("legacy", self._close_legacy))
        for name, close in resources:
            if not self._can_start_disposal():
                failed.append(name)
                continue
            self._disposal_states[name] = "pending"
            try:
                await close()
            except GuardedDisposalNotStarted:
                self._disposal_states[name] = "not_attempted"
                failed.append(name)
            except (Exception, asyncio.CancelledError):  # noqa: BLE001 -- aggregate safe disposal evidence
                # A cancelled owned disposal is unconfirmed, not successful closure.
                # Still attempt the independent resource; never retain raw exceptions.
                failed.append(name)
                self._disposal_states[name] = "failed"
            else:
                self._disposal_states[name] = "completed"
        if failed:
            raise RedisDisposalError(tuple(failed))

    async def _close_guarded(self) -> None:
        if self._cleanup_scope is None:
            await self._guarded_renewal_transport.close()
        else:
            await self._guarded_renewal_transport.close(
                can_start=self._can_start_disposal
            )

    async def _close_legacy(self) -> None:
        from redis.asyncio.connection import ConnectionPool

        from app.redis.guarded_renewal import ManagedPoolDisposal

        pool = getattr(self.client, "connection_pool", None)
        if (
            self._cleanup_scope is not None
            and isinstance(pool, ConnectionPool)
            and self._legacy_pool_disposal is None
        ):
            self._legacy_pool_disposal = ManagedPoolDisposal(
                pool, self._can_start_disposal
            )
        await self.client.aclose(close_connection_pool=True)

    @staticmethod
    def _observe_close(task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()

    async def _set_state(self, current: RedisAvailability) -> None:
        async with self._state_lock:
            previous = self._state
            self._state = current
            if previous is not current:
                self._telemetry.record_transition(previous, current)
