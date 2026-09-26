from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitContractError,
    CircuitIdentity,
    CircuitState,
)
from app.redis.concurrency import ResolvedConcurrencyPolicy
from app.redis.local_degraded import LocalDegradedProtection, _capacity
from app.redis.rate_limit import ResolvedRatePolicy


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def rate(capacity=4, factor=None, *, tenant=None, scope=None):
    return ResolvedRatePolicy(
        uuid4(),
        tenant or uuid4(),
        RateScopeType.API_KEY,
        scope or uuid4(),
        capacity,
        60,
        degraded_factor=factor,
    )


def concurrency(capacity=4, factor=None, *, tenant=None, scope=None):
    return ResolvedConcurrencyPolicy(
        uuid4(),
        tenant or uuid4(),
        RateScopeType.API_KEY,
        scope or uuid4(),
        capacity,
        degraded_factor=factor,
    )


@pytest.mark.parametrize(
    "configured,expected",
    [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (8, 2), (20, 5)],
)
def test_frozen_quarter_factor_examples(configured, expected):
    assert _capacity(configured, None) == expected


@pytest.mark.parametrize("invalid", [0, -1, 1_000_001, True, 1.5])
def test_degraded_store_bound_setting_is_positive_bounded_integer(invalid):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, degraded_local_max_entries_per_store=invalid)


def test_default_independent_store_budgets_are_10000():
    local = LocalDegradedProtection()
    assert (
        local.rate._max,
        local.concurrency._max,
        local.circuit._max,
        local.pre_auth._rate._max,
    ) == (10000, 10000, 10000, 10000)


@pytest.mark.asyncio
async def test_rate_factor_floor_minimum_and_window():
    clock = Clock()
    local = LocalDegradedProtection(clock=clock)
    item = rate(8)
    assert (await local.rate.evaluate([item])).allowed
    assert (await local.rate.evaluate([item])).allowed
    denied = await local.rate.evaluate([item])
    assert not denied.allowed and denied.retry_after_ms == 30000
    clock.now = 30
    assert (await local.rate.evaluate([item])).allowed
    override = rate(8, Decimal("0.5"))
    results = [(await local.rate.evaluate([override])).allowed for _ in range(5)]
    assert sum(results) == 4


@pytest.mark.asyncio
async def test_rate_layers_are_atomic_and_isolated():
    local = LocalDegradedProtection()
    tenant = uuid4()
    first = rate(1, tenant=tenant)
    second = rate(4, Decimal(1), tenant=tenant)
    assert (await local.rate.evaluate([first, second])).allowed
    assert not (await local.rate.evaluate([first, second])).allowed
    assert (await local.rate.evaluate([second])).allowed
    assert (await LocalDegradedProtection().rate.evaluate([first])).allowed


@pytest.mark.asyncio
async def test_rate_bound_fails_closed_until_safe_expiry():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first, second = rate(), rate()
    await local.rate.evaluate([first])
    assert not (await local.rate.evaluate([second])).allowed
    clock.now = 121
    assert (await local.rate.evaluate([second])).allowed
    assert local.rate.entry_count <= 1


@pytest.mark.asyncio
async def test_rate_safe_lru_uses_effective_refill_before_idle_ttl():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first, second = rate(1), rate(1)
    await local.rate.evaluate([first])
    clock.now = 60
    assert (await local.rate.evaluate([second])).allowed
    assert local.rate.entry_count == 1


@pytest.mark.asyncio
async def test_rate_evicts_oldest_safe_entry():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=2, clock=clock)
    first, second, third = rate(1), rate(1), rate(1)
    await local.rate.evaluate([first])
    clock.now = 1
    await local.rate.evaluate([second])
    clock.now = 61
    assert (await local.rate.evaluate([third])).allowed
    assert (
        first.tenant_id,
        first.scope_type,
        first.scope_id,
        None,
    ) not in local.rate._entries
    assert (
        second.tenant_id,
        second.scope_type,
        second.scope_id,
        None,
    ) in local.rate._entries


@pytest.mark.asyncio
async def test_rate_ttl_is_refreshed_on_rejection_and_safe_lru_only():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first, second = rate(1), rate(1)
    await local.rate.evaluate([first])
    clock.now = 119
    assert (await local.rate.evaluate([first])).allowed
    assert not (await local.rate.evaluate([first])).allowed
    clock.now = 121
    assert not (await local.rate.evaluate([second])).allowed
    clock.now = 240
    assert (await local.rate.evaluate([second])).allowed


@pytest.mark.asyncio
async def test_rate_maximum_retry_and_no_partial_consumption():
    local = LocalDegradedProtection()
    tenant = uuid4()
    first = rate(1, tenant=tenant)
    second = rate(2, Decimal(1), tenant=tenant)
    assert (await local.rate.evaluate([first, second])).allowed
    denied = await local.rate.evaluate([first, second])
    assert not denied.allowed and denied.retry_after_ms == 60000
    assert (await local.rate.evaluate([second])).allowed


@pytest.mark.asyncio
async def test_concurrency_layers_lease_and_bound():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=2, clock=clock, lease_duration_ms=5000)
    tenant = uuid4()
    first = concurrency(1, tenant=tenant)
    second = concurrency(2, tenant=tenant)
    acquired = await local.concurrency.acquire([first, second])
    assert acquired.acquired
    assert not (await local.concurrency.acquire([first, second])).acquired
    assert (await local.concurrency.acquire([second])).acquired is False
    assert (
        await local.concurrency.release([first, second], acquired.lease_id)
    ).released
    assert not (
        await local.concurrency.release([first, second], acquired.lease_id)
    ).released
    clock.now = 6
    assert (await local.concurrency.acquire([first, second])).acquired


@pytest.mark.asyncio
async def test_concurrency_layer_rejection_creates_no_partial_owner():
    local = LocalDegradedProtection()
    tenant = uuid4()
    first = concurrency(2, Decimal(1), tenant=tenant)
    second = concurrency(1, tenant=tenant)
    held = await local.concurrency.acquire([second])
    assert held.acquired
    assert not (await local.concurrency.acquire([first, second])).acquired
    assert (await local.concurrency.acquire([first])).acquired
    assert (await local.concurrency.acquire([first])).acquired
    assert not (await local.concurrency.acquire([first])).acquired


@pytest.mark.asyncio
async def test_concurrency_owner_cannot_be_evicted_and_renewal_extends_lease():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock, lease_duration_ms=5000)
    first, second = concurrency(4), concurrency(4)
    lease = (await local.concurrency.acquire([first])).lease_id
    clock.now = 4
    assert (await local.concurrency.renew([first], lease)).renewed
    assert not (await local.concurrency.acquire([second])).acquired
    clock.now = 10
    assert (await local.concurrency.acquire([second])).acquired
    assert local.concurrency.entry_count == 1


@pytest.mark.asyncio
async def test_concurrency_empty_container_ttl_is_twice_lease():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock, lease_duration_ms=5000)
    item = concurrency(4)
    lease = (await local.concurrency.acquire([item])).lease_id
    assert (await local.concurrency.release([item], lease)).released
    clock.now = 9
    local.concurrency._clean(9000)
    assert local.concurrency.entry_count == 1
    clock.now = 10
    local.concurrency._clean(10000)
    assert local.concurrency.entry_count == 0
    other = concurrency(4)
    assert (await local.concurrency.acquire([other])).acquired
    assert local.concurrency.entry_count == 1


@pytest.mark.asyncio
async def test_concurrency_evicts_oldest_empty_scope():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=2, clock=clock, lease_duration_ms=5000)
    first, second, third = concurrency(4), concurrency(4), concurrency(4)
    lease = (await local.concurrency.acquire([first])).lease_id
    await local.concurrency.release([first], lease)
    clock.now = 1
    lease = (await local.concurrency.acquire([second])).lease_id
    await local.concurrency.release([second], lease)
    clock.now = 2
    assert (await local.concurrency.acquire([third])).acquired
    assert (
        first.tenant_id,
        first.scope_type,
        first.scope_id,
    ) not in local.concurrency._entries
    assert (
        second.tenant_id,
        second.scope_type,
        second.scope_id,
    ) in local.concurrency._entries


@pytest.mark.asyncio
async def test_pre_auth_has_independent_budget_and_five_per_window():
    local = LocalDegradedProtection(max_entries=1)
    identity = uuid4()
    results = [(await local.pre_auth.evaluate(identity)).allowed for _ in range(5)]
    assert all(results)
    assert not (await local.pre_auth.evaluate(identity)).allowed
    assert not (await local.pre_auth.evaluate(uuid4())).allowed
    assert local.pre_auth.entry_count == 1
    assert local.rate.entry_count == 0


@pytest.mark.asyncio
async def test_pre_auth_reject_refreshes_ttl_and_expires_at_120_seconds():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first, second = uuid4(), uuid4()
    for _ in range(5):
        await local.pre_auth.evaluate(first)
    clock.now = 119
    assert (await local.pre_auth.evaluate(first)).allowed
    assert not (await local.pre_auth.evaluate(second)).allowed
    clock.now = 240
    assert (await local.pre_auth.evaluate(second)).allowed


@pytest.mark.asyncio
async def test_pre_auth_evicts_oldest_safely_refilled_identity():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=2, clock=clock)
    first, second, third = uuid4(), uuid4(), uuid4()
    await local.pre_auth.evaluate(first)
    clock.now = 1
    await local.pre_auth.evaluate(second)
    clock.now = 61
    assert (await local.pre_auth.evaluate(third)).allowed
    assert local.pre_auth.entry_count == 2
    keys = local.pre_auth._rate._entries
    assert not any(key[2] == first for key in keys)
    assert any(key[2] == second for key in keys)


@pytest.mark.asyncio
async def test_unknown_circuit_is_single_probe_then_closed_or_open():
    local = LocalDegradedProtection()
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    first = await local.circuit.check_or_claim_eligibility(identity, config)
    assert first.state.value == "DEGRADED_HALF_OPEN" and first.probe_id
    assert not (
        await local.circuit.check_or_claim_eligibility(identity, config)
    ).eligible
    assert (
        await local.circuit.record_success(identity, first.probe_id, config)
    ).resulting_state is CircuitState.CLOSED
    assert (await local.circuit.check_or_claim_eligibility(identity, config)).eligible


@pytest.mark.asyncio
async def test_circuit_open_expiry_probe_and_safe_bound():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    probe = (await local.circuit.check_or_claim_eligibility(first, config)).probe_id
    assert not (await local.circuit.check_or_claim_eligibility(second, config)).eligible
    assert (
        await local.circuit.record_failure(first, probe, config)
    ).resulting_state is CircuitState.OPEN
    assert not (await local.circuit.check_or_claim_eligibility(second, config)).eligible
    clock.now = 29
    assert not (await local.circuit.check_or_claim_eligibility(first, config)).eligible
    clock.now = 30
    result = await local.circuit.check_or_claim_eligibility(first, config)
    assert result.state.value == "DEGRADED_HALF_OPEN" and result.probe_id
    assert not (await local.circuit.check_or_claim_eligibility(second, config)).eligible


@pytest.mark.asyncio
async def test_circuit_closed_failure_window_and_lru():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    probe = (await local.circuit.check_or_claim_eligibility(first, config)).probe_id
    await local.circuit.record_success(first, probe, config)
    token = (await local.circuit.check_or_claim_eligibility(first, config)).normal_token
    await local.circuit.record_failure(first, token, config)
    assert not (await local.circuit.check_or_claim_eligibility(second, config)).eligible
    clock.now = 61
    assert (await local.circuit.check_or_claim_eligibility(second, config)).eligible
    assert local.circuit.entry_count == 1


@pytest.mark.asyncio
async def test_closed_circuit_failure_threshold_opens_within_window():
    local = LocalDegradedProtection()
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    probe = (await local.circuit.check_or_claim_eligibility(identity, config)).probe_id
    await local.circuit.record_success(identity, probe, config)
    token = (
        await local.circuit.check_or_claim_eligibility(identity, config)
    ).normal_token
    first = await local.circuit.record_failure(identity, token, config)
    second = await local.circuit.record_failure(identity, token, config)
    assert first.resulting_state is CircuitState.CLOSED
    assert second.resulting_state is CircuitState.OPEN
    assert not (
        await local.circuit.check_or_claim_eligibility(identity, config)
    ).eligible


@pytest.mark.asyncio
async def test_circuit_quiescent_closed_ttl_is_120_seconds_by_default():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    probe = (await local.circuit.check_or_claim_eligibility(first, config)).probe_id
    await local.circuit.record_success(first, probe, config)
    clock.now = 120
    assert (await local.circuit.check_or_claim_eligibility(second, config)).eligible
    assert local.circuit.entry_count == 1


@pytest.mark.asyncio
async def test_circuit_evicts_oldest_quiescent_closed_target():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=2, clock=clock)
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    third = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    for identity in (first, second):
        probe = (
            await local.circuit.check_or_claim_eligibility(identity, config)
        ).probe_id
        await local.circuit.record_success(identity, probe, config)
        clock.now += 1
    assert (await local.circuit.check_or_claim_eligibility(third, config)).eligible
    assert first not in local.circuit._entries
    assert second in local.circuit._entries


@pytest.mark.asyncio
async def test_circuit_eviction_uses_existing_targets_own_failure_window():
    clock = Clock()
    local = LocalDegradedProtection(max_entries=1, clock=clock)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    long_window = CircuitConfig(5, 120000, 30000, 1, 1, 30000)
    short_window = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    probe = (
        await local.circuit.check_or_claim_eligibility(first, long_window)
    ).probe_id
    await local.circuit.record_success(first, probe, long_window)
    token = (
        await local.circuit.check_or_claim_eligibility(first, long_window)
    ).normal_token
    await local.circuit.record_failure(first, token, long_window)
    clock.now = 61
    assert not (
        await local.circuit.check_or_claim_eligibility(second, short_window)
    ).eligible
    with pytest.raises(CircuitContractError):
        await local.circuit.check_or_claim_eligibility(first, short_window)


@pytest.mark.asyncio
async def test_new_process_instance_has_no_local_state_in_any_store():
    first = LocalDegradedProtection()
    second = LocalDegradedProtection()
    rate_policy = rate(1)
    concurrency_policy = concurrency(4)
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    await first.rate.evaluate([rate_policy])
    await first.concurrency.acquire([concurrency_policy])
    await first.pre_auth.evaluate(uuid4())
    await first.circuit.check_or_claim_eligibility(identity, config)
    first.mark_unreachable()
    assert not second.mode_degraded
    assert (
        second.rate.entry_count,
        second.concurrency.entry_count,
        second.pre_auth.entry_count,
        second.circuit.entry_count,
    ) == (0, 0, 0, 0)
