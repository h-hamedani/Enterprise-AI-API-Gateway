"""Normal-to-local boundaries; recovery is intentionally absent in M3.8."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from app.redis.circuit import (
    CircuitConfig,
    CircuitContractError,
    CircuitDependencyError,
    CircuitIdentity,
    EligibilityResult,
    NormalEligibilityToken,
    OutcomeResult,
    RedisCircuitStore,
)
from app.redis.concurrency import (
    AcquireResult,
    ConcurrencyDependencyError,
    RedisConcurrencySemaphore,
    ReleaseResult,
    RenewResult,
    ResolvedConcurrencyPolicy,
)
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RateLimitResult,
    RedisTokenBucket,
    ResolvedRatePolicy,
)


class DegradedRateLimiter:
    def __init__(
        self, normal: RedisTokenBucket, local: LocalDegradedProtection
    ) -> None:
        self._normal = normal
        self._local = local

    async def evaluate(self, policies: Sequence[ResolvedRatePolicy]) -> RateLimitResult:
        ordered = RedisTokenBucket._validate_and_order(policies)
        if self._local.mode_degraded:
            return await self._local.rate.evaluate(ordered)
        try:
            return await self._normal.evaluate(ordered)
        except RateLimitDependencyError:
            self._local.mark_unreachable()
            return await self._local.rate.evaluate(ordered)


class DegradedConcurrencySemaphore:
    def __init__(
        self, normal: RedisConcurrencySemaphore, local: LocalDegradedProtection
    ) -> None:
        self._normal = normal
        self._local = local

    async def acquire(
        self, policies: Sequence[ResolvedConcurrencyPolicy]
    ) -> AcquireResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        if self._local.mode_degraded:
            return await self._local.concurrency.acquire(ordered)
        try:
            return await self._normal.acquire(ordered)
        except ConcurrencyDependencyError:
            self._local.mark_unreachable()
            return await self._local.concurrency.acquire(ordered)

    async def release(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> ReleaseResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        RedisConcurrencySemaphore._check_lease_id(lease_id)
        if self._local.mode_degraded:
            return await self._local.concurrency.release(ordered, lease_id)
        try:
            return await self._normal.release(ordered, lease_id)
        except ConcurrencyDependencyError:
            self._local.mark_unreachable()
            return await self._local.concurrency.release(ordered, lease_id)

    async def renew(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> RenewResult:
        ordered = RedisConcurrencySemaphore._ordered(policies)
        RedisConcurrencySemaphore._check_lease_id(lease_id)
        if self._local.mode_degraded:
            return await self._local.concurrency.renew(ordered, lease_id)
        try:
            return await self._normal.renew(ordered, lease_id)
        except ConcurrencyDependencyError:
            self._local.mark_unreachable()
            return await self._local.concurrency.renew(ordered, lease_id)


class DegradedCircuitStore:
    def __init__(
        self,
        normal: RedisCircuitStore,
        local: LocalDegradedProtection,
        config: CircuitConfig,
    ) -> None:
        if not isinstance(config, CircuitConfig):
            raise CircuitContractError("Circuit configuration is invalid.")
        self._normal = normal
        self._local = local
        self._config = config

    async def check_or_claim_eligibility(
        self, identity: CircuitIdentity
    ) -> EligibilityResult:
        if not isinstance(identity, CircuitIdentity):
            raise CircuitContractError("Circuit identity is invalid.")
        if self._local.mode_degraded:
            return await self._local.circuit.check_or_claim_eligibility(
                identity, self._config
            )
        try:
            return await self._normal.check_or_claim_eligibility(identity)
        except CircuitDependencyError:
            self._local.mark_unreachable()
            return await self._local.circuit.check_or_claim_eligibility(
                identity, self._config
            )

    async def record_success(
        self, identity: CircuitIdentity, token: NormalEligibilityToken | UUID
    ) -> OutcomeResult:
        return await self._outcome(True, identity, token)

    async def record_failure(
        self, identity: CircuitIdentity, token: NormalEligibilityToken | UUID
    ) -> OutcomeResult:
        return await self._outcome(False, identity, token)

    async def _outcome(
        self,
        success: bool,
        identity: CircuitIdentity,
        token: NormalEligibilityToken | UUID,
    ) -> OutcomeResult:
        if self._local.mode_degraded:
            method = (
                self._local.circuit.record_success
                if success
                else self._local.circuit.record_failure
            )
            return await method(identity, token, self._config)
        method = self._normal.record_success if success else self._normal.record_failure
        try:
            return await method(identity, token)
        except CircuitDependencyError:
            self._local.mark_unreachable()
            method = (
                self._local.circuit.record_success
                if success
                else self._local.circuit.record_failure
            )
            return await method(identity, token, self._config)
