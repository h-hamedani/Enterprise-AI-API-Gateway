"""Bounded, process-local protection used only after classified Redis failure."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol
from uuid import UUID, uuid4

from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitContractError,
    CircuitIdentity,
    CircuitState,
    EligibilityResult,
    NormalEligibilityToken,
    OutcomeResult,
)
from app.redis.concurrency import (
    AcquireResult,
    ConcurrencyPolicyError,
    RedisConcurrencySemaphore,
    ReleaseResult,
    RenewResult,
    ResolvedConcurrencyPolicy,
)
from app.redis.rate_limit import (
    RateLimitResult,
    RedisTokenBucket,
    ResolvedRatePolicy,
)

logger = logging.getLogger(__name__)


class LocalDegradedTelemetry(Protocol):
    def record(self, store: str, outcome: str) -> None: ...


class LoggingLocalDegradedTelemetry:
    def record(self, store: str, outcome: str) -> None:
        logger.warning(
            "local degraded protection event protection_store=%s protection_outcome=%s",
            store,
            outcome,
        )


def _capacity(configured: int, factor: Decimal | None) -> int:
    multiplier = Decimal("0.25") if factor is None else Decimal(str(factor))
    return max(1, int(Decimal(configured) * multiplier))


@dataclass(slots=True)
class _Bucket:
    units: int
    last_ms: int
    touched_ms: int
    capacity: int
    window_ms: int


class LocalRateStore:
    def __init__(
        self,
        max_entries: int,
        clock: Callable[[], float],
        telemetry: LocalDegradedTelemetry,
        store: str = "rate",
    ) -> None:
        self._max = max_entries
        self._clock = clock
        self._entries: dict[tuple, _Bucket] = {}
        self._lock = asyncio.Lock()
        self._telemetry = telemetry
        self._store = store

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    async def evaluate(self, policies: Sequence[ResolvedRatePolicy]) -> RateLimitResult:
        ordered = RedisTokenBucket._validate_and_order(policies)
        if not ordered:
            return RateLimitResult(True, 0)
        now = int(self._clock() * 1000)
        async with self._lock:
            self._purge(now)
            keys = [
                (p.tenant_id, p.scope_type, p.scope_id, p.key_override) for p in ordered
            ]
            if not self._reserve(
                sum(key not in self._entries for key in keys), set(keys), now
            ):
                self._telemetry.record(self._store, "local_capacity_exhausted")
                return RateLimitResult(False, max(p.window_ms for p in ordered))
            retries = []
            states = []
            for key, policy in zip(keys, ordered, strict=True):
                capacity = _capacity(policy.requests_per_window, policy.degraded_factor)
                window = policy.window_ms
                bucket = self._entries.get(key)
                if (
                    bucket is None
                    or bucket.capacity != capacity
                    or bucket.window_ms != window
                ):
                    bucket = _Bucket(capacity * window, now, now, capacity, window)
                    self._entries[key] = bucket
                elapsed = max(0, now - bucket.last_ms)
                bucket.units = min(capacity * window, bucket.units + elapsed * capacity)
                bucket.last_ms = bucket.touched_ms = now
                if bucket.units < window:
                    retries.append((window - bucket.units + capacity - 1) // capacity)
                states.append(bucket)
            if retries:
                return RateLimitResult(False, max(retries))
            for bucket in states:
                bucket.units -= bucket.window_ms
            return RateLimitResult(True, 0)

    def _purge(self, now: int) -> None:
        for key, bucket in list(self._entries.items()):
            if now - bucket.touched_ms >= 2 * bucket.window_ms:
                del self._entries[key]

    def _reserve(self, needed: int, protected: set[tuple], now: int) -> bool:
        if len(self._entries) + needed <= self._max:
            return True
        safe = sorted(
            (
                (bucket.touched_ms, key)
                for key, bucket in self._entries.items()
                if key not in protected
                and bucket.units + max(0, now - bucket.last_ms) * bucket.capacity
                >= bucket.capacity * bucket.window_ms
            ),
            key=lambda pair: pair[0],
        )
        while len(self._entries) + needed > self._max and safe:
            _, key = safe.pop(0)
            del self._entries[key]
        return len(self._entries) + needed <= self._max


class LocalPreAuthStore:
    def __init__(
        self,
        max_entries: int,
        clock: Callable[[], float],
        telemetry: LocalDegradedTelemetry,
    ) -> None:
        self._rate = LocalRateStore(max_entries, clock, telemetry, "pre_auth")

    @property
    def entry_count(self) -> int:
        return self._rate.entry_count

    async def evaluate(self, identity: UUID) -> RateLimitResult:
        if not isinstance(identity, UUID):
            raise TypeError("Pre-auth identity is invalid.")
        policy = ResolvedRatePolicy(
            identity,
            UUID(int=0),
            RateScopeType.ADMIN_TOKEN,
            identity,
            5,
            60,
            degraded_factor=Decimal(1),
        )
        return await self._rate.evaluate([policy])


@dataclass(slots=True)
class _Scope:
    owners: dict[UUID, int] = field(default_factory=dict)
    touched_ms: int = 0


class LocalConcurrencyStore:
    def __init__(
        self,
        max_entries: int,
        clock: Callable[[], float],
        lease_duration_ms: int,
        telemetry: LocalDegradedTelemetry,
    ) -> None:
        if (
            type(lease_duration_ms) is not int
            or not 5000 <= lease_duration_ms <= 120000
        ):
            raise ConcurrencyPolicyError("Lease duration is outside the frozen range.")
        self._max = max_entries
        self._clock = clock
        self._duration = lease_duration_ms
        self._entries: dict[tuple, _Scope] = {}
        self._lock = asyncio.Lock()
        self._telemetry = telemetry

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def _clean(self, now: int) -> None:
        for key, scope in list(self._entries.items()):
            for owner, expiry in list(scope.owners.items()):
                if expiry <= now:
                    del scope.owners[owner]
                    scope.touched_ms = now
            if not scope.owners and now - scope.touched_ms >= 2 * self._duration:
                del self._entries[key]

    def _reserve(self, needed: int, protected: set[tuple]) -> bool:
        safe = sorted(
            (
                (scope.touched_ms, key)
                for key, scope in self._entries.items()
                if key not in protected and not scope.owners
            ),
            key=lambda pair: pair[0],
        )
        while len(self._entries) + needed > self._max and safe:
            _, key = safe.pop(0)
            del self._entries[key]
        return len(self._entries) + needed <= self._max

    async def acquire(
        self, policies: Sequence[ResolvedConcurrencyPolicy]
    ) -> AcquireResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        if not ordered:
            return AcquireResult(True, None)
        now = int(self._clock() * 1000)
        keys = [(p.tenant_id, p.scope_type, p.scope_id) for p in ordered]
        async with self._lock:
            self._clean(now)
            if not self._reserve(
                sum(key not in self._entries for key in keys), set(keys)
            ):
                self._telemetry.record("concurrency", "local_capacity_exhausted")
                return AcquireResult(False, None)
            for key, policy in zip(keys, ordered, strict=True):
                scope = self._entries.get(key)
                if scope is not None and len(scope.owners) >= _capacity(
                    policy.max_concurrency, policy.degraded_factor
                ):
                    scope.touched_ms = now
                    return AcquireResult(False, None)
            lease = uuid4()
            for key in keys:
                scope = self._entries.setdefault(key, _Scope())
                scope.owners[lease] = now + self._duration
                scope.touched_ms = now
            return AcquireResult(True, lease)

    async def release(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> ReleaseResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        if not isinstance(lease_id, UUID):
            raise ConcurrencyPolicyError("Lease ID must be a UUID.")
        now = int(self._clock() * 1000)
        async with self._lock:
            self._clean(now)
            scopes = [
                self._entries.get((p.tenant_id, p.scope_type, p.scope_id))
                for p in ordered
            ]
            complete = bool(scopes) and all(
                scope is not None and lease_id in scope.owners for scope in scopes
            )
            for scope in scopes:
                if scope is not None and lease_id in scope.owners:
                    del scope.owners[lease_id]
                    scope.touched_ms = now
            return ReleaseResult(complete)

    async def renew(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> RenewResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        if not isinstance(lease_id, UUID):
            raise ConcurrencyPolicyError("Lease ID must be a UUID.")
        now = int(self._clock() * 1000)
        async with self._lock:
            self._clean(now)
            scopes = [
                self._entries.get((p.tenant_id, p.scope_type, p.scope_id))
                for p in ordered
            ]
            if not scopes or any(
                scope is None or lease_id not in scope.owners for scope in scopes
            ):
                return RenewResult(False)
            for scope in scopes:
                scope.owners[lease_id] = now + self._duration
                scope.touched_ms = now
            return RenewResult(True)


@dataclass(slots=True)
class _Circuit:
    state: CircuitState
    touched_ms: int
    config: CircuitConfig
    incarnation: UUID = field(default_factory=uuid4)
    generation: int = 1
    probe_id: UUID | None = None
    probe_expires_ms: int = 0
    open_until_ms: int = 0
    failures: list[int] = field(default_factory=list)


class LocalCircuitStore:
    def __init__(
        self,
        max_entries: int,
        clock: Callable[[], float],
        telemetry: LocalDegradedTelemetry,
    ) -> None:
        self._max = max_entries
        self._clock = clock
        self._entries: dict[CircuitIdentity, _Circuit] = {}
        self._lock = asyncio.Lock()
        self._telemetry = telemetry

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @staticmethod
    def _ttl(config: CircuitConfig) -> int:
        return 2 * max(
            config.failure_window_ms,
            config.open_duration_ms,
            config.probe_lease_duration_ms,
        )

    @staticmethod
    def _quiescent(item: _Circuit, now: int) -> bool:
        item.failures = [
            t for t in item.failures if t > now - item.config.failure_window_ms
        ]
        return (
            item.state is CircuitState.CLOSED
            and not item.failures
            and item.probe_id is None
        )

    async def check_or_claim_eligibility(
        self, identity: CircuitIdentity, config: CircuitConfig
    ) -> EligibilityResult:
        if not isinstance(identity, CircuitIdentity) or not isinstance(
            config, CircuitConfig
        ):
            raise CircuitContractError("Circuit identity or configuration is invalid.")
        now = int(self._clock() * 1000)
        async with self._lock:
            item = self._entries.get(identity)
            if item is None:
                for key, candidate in list(self._entries.items()):
                    if self._quiescent(
                        candidate, now
                    ) and now - candidate.touched_ms >= self._ttl(candidate.config):
                        del self._entries[key]
                if len(self._entries) >= self._max:
                    safe = sorted(
                        (
                            (candidate.touched_ms, key)
                            for key, candidate in self._entries.items()
                            if self._quiescent(candidate, now)
                        ),
                        key=lambda pair: pair[0],
                    )
                    if safe:
                        del self._entries[safe[0][1]]
                if len(self._entries) >= self._max:
                    self._telemetry.record("circuit", "local_capacity_exhausted")
                    return EligibilityResult(False, CircuitState.DEGRADED_HALF_OPEN)
                item = _Circuit(CircuitState.DEGRADED_HALF_OPEN, now, config)
                self._entries[identity] = item
            elif item.config != config:
                raise CircuitContractError(
                    "Circuit configuration changed during degraded mode."
                )
            item.touched_ms = now
            if item.state is CircuitState.CLOSED:
                return EligibilityResult(
                    True,
                    CircuitState.CLOSED,
                    NormalEligibilityToken(identity, item.incarnation, item.generation),
                )
            if item.state is CircuitState.OPEN:
                if now < item.open_until_ms:
                    return EligibilityResult(False, CircuitState.OPEN)
                item.state = CircuitState.DEGRADED_HALF_OPEN
                item.probe_id = None
            if item.probe_id is not None and now < item.probe_expires_ms:
                return EligibilityResult(False, item.state)
            item.probe_id = uuid4()
            item.probe_expires_ms = now + config.probe_lease_duration_ms
            return EligibilityResult(True, item.state, probe_id=item.probe_id)

    async def record_success(
        self,
        identity: CircuitIdentity,
        token: NormalEligibilityToken | UUID,
        config: CircuitConfig,
    ) -> OutcomeResult:
        return await self._outcome(identity, token, config, True)

    async def record_failure(
        self,
        identity: CircuitIdentity,
        token: NormalEligibilityToken | UUID,
        config: CircuitConfig,
    ) -> OutcomeResult:
        return await self._outcome(identity, token, config, False)

    async def _outcome(
        self,
        identity: CircuitIdentity,
        token: NormalEligibilityToken | UUID,
        config: CircuitConfig,
        success: bool,
    ) -> OutcomeResult:
        if not isinstance(identity, CircuitIdentity) or not isinstance(
            config, CircuitConfig
        ):
            raise CircuitContractError("Circuit identity or configuration is invalid.")
        if not isinstance(token, (UUID, NormalEligibilityToken)):
            raise CircuitContractError("Circuit outcome token is invalid.")
        now = int(self._clock() * 1000)
        async with self._lock:
            item = self._entries.get(identity)
            if item is None:
                return OutcomeResult(False, CircuitState.DEGRADED_HALF_OPEN)
            if item.config != config:
                raise CircuitContractError(
                    "Circuit configuration changed during degraded mode."
                )
            probe = isinstance(token, UUID)
            if probe:
                valid = (
                    item.state
                    in (CircuitState.HALF_OPEN, CircuitState.DEGRADED_HALF_OPEN)
                    and token == item.probe_id
                    and now < item.probe_expires_ms
                )
            else:
                valid = (
                    item.state is CircuitState.CLOSED
                    and token.identity == identity
                    and token.incarnation_id == item.incarnation
                    and token.generation == item.generation
                )
            if not valid:
                return OutcomeResult(False, item.state)
            item.touched_ms = now
            if probe:
                item.probe_id = None
                item.probe_expires_ms = 0
                if success:
                    item.state = CircuitState.CLOSED
                    item.failures.clear()
                    item.generation += 1
                else:
                    item.state = CircuitState.OPEN
                    item.open_until_ms = now + config.open_duration_ms
            elif success:
                item.failures.clear()
            else:
                item.failures = [
                    t for t in item.failures if t > now - config.failure_window_ms
                ]
                item.failures.append(now)
                if len(item.failures) >= config.failure_threshold:
                    item.state = CircuitState.OPEN
                    item.open_until_ms = now + config.open_duration_ms
                    item.failures.clear()
            return OutcomeResult(True, item.state)


class LocalDegradedProtection:
    def __init__(
        self,
        *,
        max_entries: int = 10000,
        clock: Callable[[], float] = time.monotonic,
        lease_duration_ms: int = 30000,
        telemetry: LocalDegradedTelemetry | None = None,
    ) -> None:
        if type(max_entries) is not int or not 1 <= max_entries <= 1_000_000:
            raise ValueError("Local degraded entry bound is invalid.")
        observer = telemetry or LoggingLocalDegradedTelemetry()
        self.rate = LocalRateStore(max_entries, clock, observer)
        self.concurrency = LocalConcurrencyStore(
            max_entries, clock, lease_duration_ms, observer
        )
        self.circuit = LocalCircuitStore(max_entries, clock, observer)
        self.pre_auth = LocalPreAuthStore(max_entries, clock, observer)
        self.mode_degraded = False
        self.reason: str | None = None

    def mark_unreachable(self) -> None:
        self.mode_degraded = True
        self.reason = "redis_unreachable"
