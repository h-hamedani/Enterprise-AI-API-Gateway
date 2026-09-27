"""M3.9 process-local provenance-aware protection facades."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from app.redis.circuit import (
    CircuitConfig,
    CircuitDependencyError,
    CircuitIdentity,
    CircuitState,
    EligibilityResult,
    NormalEligibilityToken,
    OutcomeResult,
)
from app.redis.concurrency import (
    AcquireResult,
    ConcurrencyDependencyError,
    ReleaseResult,
    RenewResult,
)
from app.redis.rate_limit import RateLimitDependencyError, RedisTokenBucket
from app.redis.recovery import (
    Backend,
    CircuitCompletionDisposition,
    CircuitCompletionResult,
    RecoveryCoordinator,
)


class RecoveryRateLimiter:
    def __init__(self, normal, coordinator: RecoveryCoordinator) -> None:
        self._normal = normal
        self._coordinator = coordinator

    async def evaluate(self, policies):
        ordered = RedisTokenBucket._validate_and_order(policies)
        async with self._coordinator.admission() as local:
            if local is not None:
                return await local.rate.evaluate(ordered)
            try:
                return await self._normal.evaluate(ordered)
            except RateLimitDependencyError:
                self._coordinator.mark_unreachable()
                return await self._coordinator.active_generation.rate.evaluate(ordered)


@dataclass(frozen=True, slots=True)
class ConcurrencyHandle:
    raw_lease_id: UUID
    backend: Backend
    owner_id: UUID
    local_generation_id: int | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.raw_lease_id, UUID)
            or not isinstance(self.owner_id, UUID)
            or self.backend not in (Backend.REDIS, Backend.LOCAL)
            or (self.backend is Backend.REDIS and self.local_generation_id is not None)
            or (
                self.backend is Backend.LOCAL
                and (
                    type(self.local_generation_id) is not int
                    or self.local_generation_id < 1
                )
            )
        ):
            raise ValueError("Concurrency handle provenance is invalid.")


@dataclass(frozen=True, slots=True)
class ProtectedAcquireResult:
    acquired: bool
    handle: ConcurrencyHandle | None


@dataclass(frozen=True, slots=True)
class CircuitCompletionHandle:
    raw_token: NormalEligibilityToken | UUID
    backend: Backend
    owner_id: UUID
    local_generation_id: int | None
    identity: CircuitIdentity | None
    completion_deadline: float | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.raw_token, (NormalEligibilityToken, UUID))
            or not isinstance(self.owner_id, UUID)
            or not isinstance(self.identity, CircuitIdentity)
            or self.backend not in (Backend.REDIS, Backend.LOCAL)
            or (
                self.backend is Backend.REDIS
                and (
                    self.local_generation_id is not None
                    or self.completion_deadline is not None
                )
            )
            or (
                self.backend is Backend.LOCAL
                and (
                    type(self.local_generation_id) is not int
                    or self.local_generation_id < 1
                    or (
                        isinstance(self.raw_token, NormalEligibilityToken)
                        and self.completion_deadline is None
                    )
                )
            )
        ):
            raise ValueError("Circuit handle provenance is invalid.")


@dataclass(frozen=True, slots=True)
class ProtectedEligibilityResult:
    eligible: bool
    state: CircuitState
    completion_handle: CircuitCompletionHandle | None


class RecoveryConcurrencySemaphore:
    def __init__(self, normal, coordinator: RecoveryCoordinator) -> None:
        self._normal = normal
        self._coordinator = coordinator

    async def acquire(self, policies) -> ProtectedAcquireResult:
        async with self._coordinator.admission() as local:
            if local is None:
                try:
                    result: AcquireResult = await self._normal.acquire(policies)
                except ConcurrencyDependencyError:
                    self._coordinator.mark_unreachable()
                    local = self._coordinator.active_generation
                else:
                    return ProtectedAcquireResult(
                        result.acquired,
                        ConcurrencyHandle(
                            result.lease_id, Backend.REDIS, self._coordinator.owner_id
                        )
                        if result.acquired and result.lease_id is not None
                        else None,
                    )
            result = await local.concurrency.acquire(policies)
            if result.acquired and result.lease_id is not None:
                generation = self._coordinator.active_generation_id
                self._coordinator.note_local_completion(
                    generation,
                    self._coordinator.clock()
                    + self._coordinator.lease_duration_seconds,
                    result.lease_id,
                )
                return ProtectedAcquireResult(
                    True,
                    ConcurrencyHandle(
                        result.lease_id,
                        Backend.LOCAL,
                        self._coordinator.owner_id,
                        generation,
                    ),
                )
            return ProtectedAcquireResult(result.acquired, None)

    async def renew(self, policies, handle: object) -> RenewResult:
        if not self._valid(handle):
            return RenewResult(False)
        if handle.backend is Backend.REDIS:
            try:
                return await self._normal.renew(policies, handle.raw_lease_id)
            except ConcurrencyDependencyError:
                self._coordinator.mark_unreachable()
                raise
        local = self._coordinator.local_generation(handle.local_generation_id)
        if local is None:
            return RenewResult(False)
        result = await local.concurrency.renew(policies, handle.raw_lease_id)
        if result.renewed:
            self._coordinator.note_local_completion(
                handle.local_generation_id,
                self._coordinator.clock() + self._coordinator.lease_duration_seconds,
                handle.raw_lease_id,
            )
        return result

    async def release(self, policies, handle: object) -> ReleaseResult:
        if not self._valid(handle):
            return ReleaseResult(False)
        if handle.backend is Backend.REDIS:
            try:
                return await self._normal.release(policies, handle.raw_lease_id)
            except ConcurrencyDependencyError:
                self._coordinator.mark_unreachable()
                raise
        local = self._coordinator.local_generation(handle.local_generation_id)
        if local is None:
            return ReleaseResult(False)
        result = await local.concurrency.release(policies, handle.raw_lease_id)
        if result.released:
            self._coordinator.forget_local_completion(
                handle.local_generation_id, handle.raw_lease_id
            )
        return result

    def _valid(self, handle: object) -> bool:
        return (
            isinstance(handle, ConcurrencyHandle)
            and handle.owner_id == self._coordinator.owner_id
        )


class RecoveryCircuitStore:
    def __init__(
        self,
        normal,
        coordinator: RecoveryCoordinator,
        config: CircuitConfig,
        *,
        normal_ttl_ms: int = 300000,
    ) -> None:
        if type(normal_ttl_ms) is not int or not 5000 <= normal_ttl_ms <= 3600000:
            raise ValueError("Local normal completion TTL is invalid.")
        self._normal = normal
        self._coordinator = coordinator
        self._config = config
        self._normal_ttl_ms = normal_ttl_ms

    async def check_or_claim_eligibility(
        self, identity: CircuitIdentity
    ) -> ProtectedEligibilityResult:
        async with self._coordinator.admission() as local:
            if local is None:
                try:
                    result = await self._normal.check_or_claim_eligibility(identity)
                except CircuitDependencyError:
                    self._coordinator.mark_unreachable()
                    local = self._coordinator.active_generation
                else:
                    return self._wrap(result, identity, Backend.REDIS, None)
            result = await local.circuit.check_or_claim_eligibility(
                identity, self._config
            )
            return self._wrap(
                result, identity, Backend.LOCAL, self._coordinator.active_generation_id
            )

    def _wrap(
        self,
        result: EligibilityResult,
        identity: CircuitIdentity,
        backend: Backend,
        generation: int | None,
    ) -> ProtectedEligibilityResult:
        token = result.normal_token or result.probe_id
        if not result.eligible or token is None:
            return ProtectedEligibilityResult(result.eligible, result.state, None)
        deadline = None
        if backend is Backend.LOCAL:
            duration = (
                self._normal_ttl_ms
                if isinstance(token, NormalEligibilityToken)
                else self._config.probe_lease_duration_ms
            )
            deadline = self._coordinator.clock() + duration / 1000
            self._coordinator.note_local_completion(
                generation,
                deadline,
                token if isinstance(token, UUID) else None,
            )
        return ProtectedEligibilityResult(
            True,
            result.state,
            CircuitCompletionHandle(
                token,
                backend,
                self._coordinator.owner_id,
                generation,
                identity,
                deadline if isinstance(token, NormalEligibilityToken) else None,
            ),
        )

    async def record_success(
        self, identity: CircuitIdentity, handle: object
    ) -> CircuitCompletionResult:
        return await self._complete(True, identity, handle)

    async def record_failure(
        self, identity: CircuitIdentity, handle: object
    ) -> CircuitCompletionResult:
        return await self._complete(False, identity, handle)

    async def _complete(
        self, success: bool, identity: CircuitIdentity, handle: object
    ) -> CircuitCompletionResult:
        unknown = CircuitCompletionResult(
            False, None, CircuitCompletionDisposition.UNKNOWN_HANDLE
        )
        if (
            not isinstance(handle, CircuitCompletionHandle)
            or handle.owner_id != self._coordinator.owner_id
            or handle.identity != identity
        ):
            return unknown
        if handle.backend is Backend.REDIS:
            backend = self._normal
            args = (identity, handle.raw_token)
        else:
            if (
                isinstance(handle.raw_token, NormalEligibilityToken)
                and self._coordinator.clock() >= handle.completion_deadline
            ):
                return unknown
            generation = self._coordinator.local_generation(handle.local_generation_id)
            if generation is None:
                return unknown
            backend = generation.circuit
            args = (identity, handle.raw_token, self._config)
        method = backend.record_success if success else backend.record_failure
        try:
            result: OutcomeResult = await method(*args)
        except CircuitDependencyError:
            if handle.backend is Backend.REDIS:
                self._coordinator.mark_unreachable()
            raise
        if handle.backend is Backend.LOCAL and isinstance(handle.raw_token, UUID):
            self._coordinator.forget_local_completion(
                handle.local_generation_id, handle.raw_token
            )
        return CircuitCompletionResult(
            result.applied,
            result.resulting_state,
            CircuitCompletionDisposition.APPLIED
            if result.applied
            else CircuitCompletionDisposition.STALE,
        )
