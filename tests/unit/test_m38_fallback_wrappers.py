import asyncio
from uuid import uuid4

import pytest
from redis.exceptions import AuthenticationError, ResponseError

from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitContractError,
    CircuitIdentity,
    CircuitProtocolError,
    CircuitState,
    CircuitStateError,
    EligibilityResult,
)
from app.redis.concurrency import (
    AcquireResult,
    ConcurrencyPolicyError,
    ConcurrencyProtocolError,
    ResolvedConcurrencyPolicy,
)
from app.redis.degraded import (
    DegradedCircuitStore,
    DegradedConcurrencySemaphore,
    DegradedRateLimiter,
)
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RateLimitProtocolError,
    RateLimitResult,
    ResolvedRatePolicy,
)


def policy():
    return ResolvedRatePolicy(uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4, 60)


class Normal:
    def __init__(self, error):
        self.error = error
        self.calls = 0

    async def evaluate(self, policies):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return RateLimitResult(True, 0)

    async def acquire(self, policies):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return AcquireResult(True, uuid4())

    async def check_or_claim_eligibility(self, identity):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return EligibilityResult(True, CircuitState.CLOSED)


@pytest.mark.asyncio
async def test_dependency_falls_back_and_mode_is_sticky():
    normal = Normal(RateLimitDependencyError())
    local = LocalDegradedProtection()
    wrapper = DegradedRateLimiter(normal, local)
    item = policy()
    assert (await wrapper.evaluate([item])).allowed
    normal.error = None
    assert not (await wrapper.evaluate([item])).allowed
    assert normal.calls == 1
    assert local.mode_degraded and local.reason == "redis_unreachable"


@pytest.mark.asyncio
async def test_concurrency_stays_local_after_normal_backend_becomes_healthy():
    from app.redis.concurrency import ConcurrencyDependencyError

    normal = Normal(ConcurrencyDependencyError())
    local = LocalDegradedProtection()
    wrapper = DegradedConcurrencySemaphore(normal, local)
    item = ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4
    )
    first = await wrapper.acquire([item])
    assert first.acquired and local.mode_degraded
    normal.error = None
    assert not (await wrapper.acquire([item])).acquired
    assert normal.calls == 1 and local.concurrency.entry_count == 1
    assert (await wrapper.renew([item], first.lease_id)).renewed
    assert (await wrapper.release([item], first.lease_id)).released
    assert normal.calls == 1 and local.mode_degraded


@pytest.mark.asyncio
async def test_circuit_stays_local_after_normal_backend_becomes_healthy():
    from app.redis.circuit import CircuitDependencyError

    normal = Normal(CircuitDependencyError())
    local = LocalDegradedProtection()
    wrapper = DegradedCircuitStore(
        normal, local, CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    )
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    first = await wrapper.check_or_claim_eligibility(identity)
    assert first.state is CircuitState.DEGRADED_HALF_OPEN and first.probe_id
    normal.error = None
    assert not (await wrapper.check_or_claim_eligibility(identity)).eligible
    assert normal.calls == 1 and local.circuit.entry_count == 1
    assert local.mode_degraded


@pytest.mark.asyncio
async def test_one_domain_failure_makes_other_wrappers_choose_local_first():
    from app.redis.circuit import CircuitDependencyError
    from app.redis.concurrency import ConcurrencyDependencyError

    local = LocalDegradedProtection()
    rate_normal = Normal(RateLimitDependencyError())
    await DegradedRateLimiter(rate_normal, local).evaluate([policy()])
    assert local.mode_degraded

    concurrency_normal = Normal(ConcurrencyDependencyError())
    concurrency = DegradedConcurrencySemaphore(concurrency_normal, local)
    item = ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4
    )
    assert (await concurrency.acquire([item])).acquired
    assert concurrency_normal.calls == 0

    circuit_normal = Normal(CircuitDependencyError())
    circuit = DegradedCircuitStore(
        circuit_normal, local, CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    )
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    assert (await circuit.check_or_claim_eligibility(identity)).probe_id
    assert circuit_normal.calls == 0


@pytest.mark.asyncio
async def test_in_flight_normal_call_may_finish_but_new_call_is_local():
    class InFlightNormal:
        def __init__(self):
            self.calls = 0
            self.first_started = asyncio.Event()
            self.second_started = asyncio.Event()
            self.release_first = asyncio.Event()
            self.release_second = asyncio.Event()

        async def evaluate(self, policies):
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                await self.release_first.wait()
                raise RateLimitDependencyError()
            self.second_started.set()
            await self.release_second.wait()
            return RateLimitResult(True, 0)

    normal = InFlightNormal()
    local = LocalDegradedProtection()
    wrapper = DegradedRateLimiter(normal, local)
    item = policy()
    first = asyncio.create_task(wrapper.evaluate([item]))
    await normal.first_started.wait()
    second = asyncio.create_task(wrapper.evaluate([item]))
    await normal.second_started.wait()
    normal.release_first.set()
    assert (await first).allowed
    assert local.mode_degraded
    normal.release_second.set()
    assert (await second).allowed
    assert not (await wrapper.evaluate([item])).allowed
    assert normal.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [RateLimitProtocolError(), AuthenticationError(), ResponseError(), ValueError()],
)
async def test_non_dependency_never_enters_local_mode(error):
    local = LocalDegradedProtection()
    wrapper = DegradedRateLimiter(Normal(error), local)
    with pytest.raises(type(error)):
        await wrapper.evaluate([policy()])
    assert not local.mode_degraded and local.rate.entry_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ConcurrencyProtocolError(),
        ConcurrencyPolicyError("invalid"),
        AuthenticationError(),
    ],
)
async def test_concurrency_non_dependency_never_falls_back(error):
    local = LocalDegradedProtection()
    wrapper = DegradedConcurrencySemaphore(Normal(error), local)
    item = ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4
    )
    with pytest.raises(type(error)):
        await wrapper.acquire([item])
    assert not local.mode_degraded and local.concurrency.entry_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        CircuitProtocolError(),
        CircuitStateError(),
        CircuitContractError("invalid"),
        AuthenticationError(),
    ],
)
async def test_circuit_non_dependency_never_falls_back(error):
    local = LocalDegradedProtection()
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    wrapper = DegradedCircuitStore(Normal(error), local, config)
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    with pytest.raises(type(error)):
        await wrapper.check_or_claim_eligibility(identity)
    assert not local.mode_degraded and local.circuit.entry_count == 0
