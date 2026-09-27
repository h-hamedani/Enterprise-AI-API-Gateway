"""Provenance remains authoritative across recovery and local generations."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.control_plane.config_subscriber import InvalidationRegistry
from app.core.config import Settings
from app.persistence.models.enums import RateScopeType
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
from app.redis.protection_facade import (
    CircuitCompletionHandle,
    ConcurrencyHandle,
    RecoveryCircuitStore,
    RecoveryConcurrencySemaphore,
    RecoveryRateLimiter,
)
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RateLimitResult,
    ResolvedRatePolicy,
)
from app.redis.recovery import (
    Backend,
    CircuitCompletionDisposition,
    RecoveryCoordinator,
    TrafficMode,
)


class Clock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


class RedisBackend:
    def __init__(self, circuit_result=None):
        self.calls = []
        self.circuit_result = circuit_result

    async def acquire(self, policies):
        self.calls.append("acquire")
        return AcquireResult(True, uuid4())

    async def renew(self, policies, lease):
        self.calls.append("renew")
        return RenewResult(True)

    async def release(self, policies, lease):
        self.calls.append("release")
        return ReleaseResult(True)

    async def check_or_claim_eligibility(self, identity):
        self.calls.append("eligibility")
        return self.circuit_result

    async def record_success(self, identity, token):
        self.calls.append("success")
        return OutcomeResult(True, CircuitState.CLOSED)

    async def record_failure(self, identity, token):
        self.calls.append("failure")
        return OutcomeResult(False, CircuitState.OPEN)


class FailingCompletionBackend(RedisBackend):
    def __init__(self, failed_operation, circuit_result=None):
        super().__init__(circuit_result)
        self.failed_operation = failed_operation

    async def renew(self, policies, lease):
        if self.failed_operation == "renew":
            self.calls.append("renew")
            raise ConcurrencyDependencyError()
        return await super().renew(policies, lease)

    async def release(self, policies, lease):
        if self.failed_operation == "release":
            self.calls.append("release")
            raise ConcurrencyDependencyError()
        return await super().release(policies, lease)

    async def record_success(self, identity, token):
        if self.failed_operation == "success":
            self.calls.append("success")
            raise CircuitDependencyError()
        return await super().record_success(identity, token)

    async def record_failure(self, identity, token):
        if self.failed_operation == "failure":
            self.calls.append("failure")
            raise CircuitDependencyError()
        return await super().record_failure(identity, token)


def coordinator(clock):
    class Reconciler:
        async def reconcile_once(self):
            return "empty_membership"

    class Runtime:
        async def check(self):
            return SimpleNamespace(available=True)

    return RecoveryCoordinator(
        Runtime(),
        SimpleNamespace(ready=True),
        Reconciler(),
        InvalidationRegistry(),
        clock=clock,
    )


def policy():
    from app.redis.concurrency import ResolvedConcurrencyPolicy

    return ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 2
    )


@pytest.mark.asyncio
async def test_local_lease_renews_and_releases_to_issuing_generation_after_cutover():
    clock = Clock()
    owner = coordinator(clock)
    redis = RedisBackend()
    facade = RecoveryConcurrencySemaphore(redis, owner)
    owner.mark_unreachable()
    generation_id = owner.active_generation_id
    item = policy()
    acquired = await facade.acquire([item])
    assert acquired.acquired and acquired.handle.backend is Backend.LOCAL
    assert acquired.handle.local_generation_id == generation_id
    assert await owner.attempt_recovery() == "recovery_succeeded"
    assert (await facade.renew([item], acquired.handle)).renewed
    assert (await facade.release([item], acquired.handle)).released
    assert redis.calls == []


@pytest.mark.asyncio
async def test_unknown_lease_and_circuit_handle_never_call_backend():
    clock = Clock()
    owner = coordinator(clock)
    redis = RedisBackend()
    semaphore = RecoveryConcurrencySemaphore(redis, owner)
    circuit = RecoveryCircuitStore(
        redis, owner, CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    )
    item = policy()
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    assert not (await semaphore.renew([item], uuid4())).renewed
    assert not (await semaphore.release([item], uuid4())).released
    unknown = await circuit.record_success(identity, uuid4())
    assert unknown.disposition is CircuitCompletionDisposition.UNKNOWN_HANDLE
    assert unknown.resulting_state is None and not unknown.applied
    assert redis.calls == []


@pytest.mark.asyncio
async def test_local_normal_deadline_is_absolute_and_expiry_calls_neither_backend():
    clock = Clock()
    owner = coordinator(clock)
    owner.mark_unreachable()
    redis = RedisBackend()
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    facade = RecoveryCircuitStore(redis, owner, config, normal_ttl_ms=5000)
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    local = owner.active_generation
    # Establish CLOSED through the actual local probe transition.
    probe = await facade.check_or_claim_eligibility(identity)
    assert probe.completion_handle is not None
    await facade.record_success(identity, probe.completion_handle)
    normal = await facade.check_or_claim_eligibility(identity)
    handle = normal.completion_handle
    assert isinstance(handle.raw_token, NormalEligibilityToken)
    assert handle.completion_deadline == 105.0
    clock.value = 104.999
    assert (
        await facade.record_success(identity, handle)
    ).disposition is CircuitCompletionDisposition.APPLIED
    assert handle.completion_deadline == 105.0
    clock.value = 105.0
    outcome = await facade.record_success(identity, handle)
    assert outcome.disposition is CircuitCompletionDisposition.UNKNOWN_HANDLE
    assert outcome.resulting_state is None
    assert redis.calls == []
    assert local.circuit.entry_count == 1


def test_malformed_provenance_handles_are_rejected_without_backend_guessing():
    with pytest.raises(ValueError):
        ConcurrencyHandle(uuid4(), Backend.LOCAL, uuid4(), None)
    with pytest.raises(ValueError):
        CircuitCompletionHandle(uuid4(), Backend.REDIS, uuid4(), 2, None, None)


@pytest.mark.asyncio
async def test_redis_lease_remains_redis_owned_during_degradation():
    clock = Clock()
    owner = coordinator(clock)
    redis = RedisBackend()
    facade = RecoveryConcurrencySemaphore(redis, owner)
    item = policy()
    acquired = await facade.acquire([item])
    assert acquired.handle.backend is Backend.REDIS
    owner.mark_unreachable()
    assert (await facade.renew([item], acquired.handle)).renewed
    assert (await facade.release([item], acquired.handle)).released
    assert redis.calls == ["acquire", "renew", "release"]
    assert owner.active_generation.concurrency.entry_count == 0


@pytest.mark.asyncio
async def test_redis_circuit_result_maps_applied_and_stale_even_when_degraded():
    owner = coordinator(Clock())
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    token = NormalEligibilityToken(identity, uuid4(), 1)
    redis = RedisBackend(EligibilityResult(True, CircuitState.CLOSED, token))
    facade = RecoveryCircuitStore(
        redis, owner, CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    )
    admitted = await facade.check_or_claim_eligibility(identity)
    assert admitted.completion_handle.backend is Backend.REDIS
    owner.mark_unreachable()
    applied = await facade.record_success(identity, admitted.completion_handle)
    stale = await facade.record_failure(identity, admitted.completion_handle)
    assert (applied.disposition, applied.resulting_state) == (
        CircuitCompletionDisposition.APPLIED,
        CircuitState.CLOSED,
    )
    assert (stale.disposition, stale.resulting_state) == (
        CircuitCompletionDisposition.STALE,
        CircuitState.OPEN,
    )
    assert redis.calls == ["eligibility", "success", "failure"]
    assert owner.active_generation.circuit.entry_count == 0


@pytest.mark.asyncio
async def test_retired_generation_reclaims_after_last_fixed_normal_deadline():
    clock = Clock()
    owner = coordinator(clock)
    owner.mark_unreachable()
    generation_id = owner.active_generation_id
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    facade = RecoveryCircuitStore(
        RedisBackend(),
        owner,
        CircuitConfig(5, 60000, 30000, 1, 1, 30000),
        normal_ttl_ms=5000,
    )
    probe = await facade.check_or_claim_eligibility(identity)
    await facade.record_success(identity, probe.completion_handle)
    normal = await facade.check_or_claim_eligibility(identity)
    assert normal.completion_handle.completion_deadline == 105.0
    assert await owner.attempt_recovery() == "recovery_succeeded"
    clock.value = 104.0
    await facade.record_success(identity, normal.completion_handle)
    owner.cleanup_retired()
    assert generation_id in owner.retired_generations
    clock.value = 105.0
    owner.cleanup_retired()
    assert generation_id not in owner.retired_generations
    expired = await facade.record_success(identity, normal.completion_handle)
    assert expired.disposition is CircuitCompletionDisposition.UNKNOWN_HANDLE


def test_local_normal_completion_ttl_default_and_bounds():
    assert Settings().local_circuit_normal_completion_ttl_ms == 300000
    with pytest.raises(ValueError):
        Settings(local_circuit_normal_completion_ttl_ms=4999)
    with pytest.raises(ValueError):
        Settings(local_circuit_normal_completion_ttl_ms=3600001)


@pytest.mark.asyncio
async def test_rate_admission_stays_local_during_recovery_then_returns_to_redis():
    owner = coordinator(Clock())

    class RedisRate:
        def __init__(self):
            self.fail = True
            self.calls = 0

        async def evaluate(self, policies):
            self.calls += 1
            if self.fail:
                raise RateLimitDependencyError()
            return RateLimitResult(True, 0)

    redis = RedisRate()
    facade = RecoveryRateLimiter(redis, owner)
    item = ResolvedRatePolicy(uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 3, 60)
    assert (await facade.evaluate([item])).allowed
    assert owner.mode.value == "DEGRADED_REDIS"
    redis.fail = False
    assert not (await facade.evaluate([item])).allowed
    assert redis.calls == 1
    assert await owner.attempt_recovery() == "recovery_succeeded"
    assert (await facade.evaluate([item])).allowed
    assert redis.calls == 2


@pytest.mark.asyncio
async def test_later_redis_failure_creates_new_local_generation_without_reactivating_old():
    clock = Clock()
    owner = coordinator(clock)
    redis = RedisBackend()
    facade = RecoveryConcurrencySemaphore(redis, owner)
    item = policy()
    owner.mark_unreachable()
    first = await facade.acquire([item])
    first_generation = owner.active_generation_id
    assert await owner.attempt_recovery() == "recovery_succeeded"
    owner.mark_unreachable()
    second_generation = owner.active_generation_id
    second = await facade.acquire([item])
    assert second_generation != first_generation
    assert first.handle.local_generation_id == first_generation
    assert second.handle.local_generation_id == second_generation
    assert (await facade.release([item], first.handle)).released
    assert (await facade.release([item], second.handle)).released
    assert redis.calls == []
    assert owner.local_generation(first_generation) is not owner.active_generation


@pytest.mark.asyncio
async def test_pre_restart_handles_are_unknown_without_backend_calls():
    first_owner = coordinator(Clock())
    first_owner.mark_unreachable()
    first_backend = RedisBackend()
    first_facade = RecoveryConcurrencySemaphore(first_backend, first_owner)
    item = policy()
    old_handle = (await first_facade.acquire([item])).handle
    restarted_owner = coordinator(Clock())
    restarted_backend = RedisBackend()
    restarted_facade = RecoveryConcurrencySemaphore(restarted_backend, restarted_owner)
    assert not (await restarted_facade.renew([item], old_handle)).renewed
    assert not (await restarted_facade.release([item], old_handle)).released
    assert restarted_backend.calls == []
    assert restarted_owner.active_generation is None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["renew", "release"])
async def test_redis_lease_completion_failure_degrades_without_local_retry(operation):
    owner = coordinator(Clock())
    backend = FailingCompletionBackend(operation)
    facade = RecoveryConcurrencySemaphore(backend, owner)
    item = policy()
    handle = (await facade.acquire([item])).handle
    assert handle.backend is Backend.REDIS
    with pytest.raises(ConcurrencyDependencyError):
        await getattr(facade, operation)([item], handle)
    assert owner.mode is TrafficMode.DEGRADED_REDIS
    assert owner.active_generation is not None
    assert owner.active_generation.concurrency.entry_count == 0
    assert backend.calls == ["acquire", operation]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["success", "failure"])
async def test_redis_circuit_completion_failure_degrades_without_local_mutation(
    operation,
):
    owner = coordinator(Clock())
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    token = NormalEligibilityToken(identity, uuid4(), 1)
    backend = FailingCompletionBackend(
        operation, EligibilityResult(True, CircuitState.CLOSED, token)
    )
    facade = RecoveryCircuitStore(
        backend, owner, CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    )
    handle = (await facade.check_or_claim_eligibility(identity)).completion_handle
    assert handle.backend is Backend.REDIS
    with pytest.raises(CircuitDependencyError):
        await getattr(facade, f"record_{operation}")(identity, handle)
    assert owner.mode is TrafficMode.DEGRADED_REDIS
    assert owner.active_generation is not None
    assert owner.active_generation.circuit.entry_count == 0
    assert backend.calls == ["eligibility", operation]


@pytest.mark.asyncio
async def test_redis_owned_failure_while_degraded_preserves_active_generation():
    owner = coordinator(Clock())
    item = policy()
    backend = FailingCompletionBackend("release")
    facade = RecoveryConcurrencySemaphore(backend, owner)
    handle = (await facade.acquire([item])).handle
    owner.mark_unreachable()
    active = owner.active_generation
    active_id = owner.active_generation_id
    with pytest.raises(ConcurrencyDependencyError):
        await facade.release([item], handle)
    assert owner.mode is TrafficMode.DEGRADED_REDIS
    assert owner.active_generation is active
    assert owner.active_generation_id == active_id
    assert active.concurrency.entry_count == 0


@pytest.mark.asyncio
async def test_post_recovery_redis_completion_failure_creates_g_plus_one():
    owner = coordinator(Clock())
    owner.mark_unreachable()
    retired_id = owner.active_generation_id
    retired = owner.active_generation
    assert await owner.attempt_recovery() == "recovery_succeeded"
    item = policy()
    backend = FailingCompletionBackend("renew")
    facade = RecoveryConcurrencySemaphore(backend, owner)
    handle = (await facade.acquire([item])).handle
    assert handle.backend is Backend.REDIS
    with pytest.raises(ConcurrencyDependencyError):
        await facade.renew([item], handle)
    assert owner.mode is TrafficMode.DEGRADED_REDIS
    assert owner.active_generation_id != retired_id
    assert owner.active_generation is not retired
    assert owner.retired_generations[retired_id] is retired
    assert owner.active_generation.concurrency.entry_count == 0
