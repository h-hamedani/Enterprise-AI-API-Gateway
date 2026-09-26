import asyncio
import os
from uuid import uuid4

import pytest
import pytest_asyncio

from app.core.config import Settings, get_settings
from app.persistence.models.llm_registry import LlmModel, LlmProviderTarget
from app.persistence.models.normal_api import NormalApiRoute, NormalApiService
from app.redis.circuit import (
    CircuitConfig,
    CircuitContractError,
    CircuitDependencyError,
    CircuitIdentity,
    CircuitState,
    CircuitStateError,
    RedisCircuitStore,
    circuit_key,
)
from app.redis.runtime import RedisRuntime


@pytest_asyncio.fixture
async def redis_runtime():
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    await runtime.start()
    yield runtime
    await runtime.close()


@pytest_asyncio.fixture
async def redis_peer():
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    await runtime.start()
    yield runtime
    await runtime.close()


def identity():
    tenant = uuid4()
    service = NormalApiService(id=uuid4(), tenant_id=tenant)
    route = NormalApiRoute(id=uuid4(), tenant_id=tenant, service_id=service.id)
    return CircuitIdentity.normal(route, service)


def config(**overrides):
    return CircuitConfig(
        failure_threshold=overrides.get("failure_threshold", 2),
        failure_window_ms=overrides.get("failure_window_ms", 1000),
        open_duration_ms=overrides.get("open_duration_ms", 60),
        half_open_probe_limit=overrides.get("half_open_probe_limit", 1),
        successes_to_close=overrides.get("successes_to_close", 1),
        probe_lease_duration_ms=overrides.get("probe_lease_duration_ms", 60),
    )


def test_settings_and_tenant_scoped_key():
    settings = Settings(_env_file=None)
    assert CircuitConfig.from_settings(settings).failure_threshold == 5
    tenant = uuid4()
    route = uuid4()
    service = uuid4()
    item = CircuitIdentity.normal(
        NormalApiRoute(id=route, tenant_id=tenant, service_id=service),
        NormalApiService(id=service, tenant_id=tenant),
    )
    assert circuit_key(item) == f"gw:v1:cb:{{{tenant}}}:route:{route}:{service}"
    other_tenant = uuid4()
    other = CircuitIdentity.normal(
        NormalApiRoute(id=route, tenant_id=other_tenant, service_id=service),
        NormalApiService(id=service, tenant_id=other_tenant),
    )
    assert circuit_key(other) != circuit_key(item)


def test_identity_rejects_mismatched_parents():
    tenant = uuid4()
    service = NormalApiService(id=uuid4(), tenant_id=tenant)
    route = NormalApiRoute(id=uuid4(), tenant_id=tenant, service_id=uuid4())
    with pytest.raises(CircuitContractError):
        CircuitIdentity.normal(route, service)
    target = LlmProviderTarget(id=uuid4(), tenant_id=tenant)
    model = LlmModel(id=uuid4(), tenant_id=tenant, provider_target_id=uuid4())
    with pytest.raises(CircuitContractError):
        CircuitIdentity.llm(target, model)


def test_llm_identity_uses_provider_target_and_model():
    tenant = uuid4()
    target = LlmProviderTarget(id=uuid4(), tenant_id=tenant)
    model = LlmModel(id=uuid4(), tenant_id=tenant, provider_target_id=target.id)
    item = CircuitIdentity.llm(target, model)
    assert circuit_key(item) == (
        f"gw:v1:cb:{{{tenant}}}:provider_target:{target.id}:{model.id}"
    )


def test_circuit_config_rejects_unsupported_probe_counts_and_coercion():
    with pytest.raises(CircuitContractError):
        config(half_open_probe_limit=2)
    with pytest.raises(CircuitContractError):
        CircuitConfig(2, 1000, 1000, 1, 2, 1000)
    with pytest.raises(CircuitContractError):
        CircuitConfig("2", 1000, 1000, 1, 1, 1000)


@pytest.mark.asyncio
async def test_threshold_trip_success_reset_and_single_probe(redis_runtime, redis_peer):
    item = identity()
    first = RedisCircuitStore(redis_runtime, config(probe_lease_duration_ms=5000))
    second = RedisCircuitStore(redis_peer, config(probe_lease_duration_ms=5000))
    admitted = await first.check_or_claim_eligibility(item)
    assert admitted.eligible and admitted.state is CircuitState.CLOSED
    token = admitted.normal_token
    assert token is not None
    assert (await first.record_failure(item, token)).applied
    assert (await second.record_success(item, token)).applied
    assert (
        await first.record_failure(item, token)
    ).resulting_state is CircuitState.CLOSED
    tripped = await second.record_failure(item, token)
    assert tripped.applied and tripped.resulting_state is CircuitState.OPEN
    assert not (await first.record_failure(item, token)).applied
    await asyncio.sleep(0.07)
    results = await asyncio.gather(
        *(
            (first if i % 2 else second).check_or_claim_eligibility(item)
            for i in range(30)
        )
    )
    probes = [result.probe_id for result in results if result.eligible]
    assert len(probes) == 1 and probes[0] is not None
    assert all(not result.eligible for result in results if result.probe_id is None)
    assert not (await first.record_success(item, uuid4())).applied
    assert not (await second.record_failure(item, uuid4())).applied
    closed = await first.record_success(item, probes[0])
    assert closed.applied and closed.resulting_state is CircuitState.CLOSED
    fresh = await second.check_or_claim_eligibility(item)
    assert fresh.normal_token.generation == token.generation + 1
    assert fresh.normal_token.incarnation_id == token.incarnation_id
    assert not (await first.record_success(item, token)).applied
    assert not (await first.record_failure(item, token)).applied


@pytest.mark.asyncio
async def test_probe_expiry_replacement_and_state_loss(redis_runtime):
    item = identity()
    store = RedisCircuitStore(
        redis_runtime, config(open_duration_ms=20, probe_lease_duration_ms=30)
    )
    admitted = await store.check_or_claim_eligibility(item)
    token = admitted.normal_token
    await store.record_failure(item, token)
    await store.record_failure(item, token)
    await asyncio.sleep(0.03)
    first_probe = (await store.check_or_claim_eligibility(item)).probe_id
    assert first_probe is not None
    await asyncio.sleep(0.04)
    assert not (await store.record_success(item, first_probe)).applied
    replacement = (await store.check_or_claim_eligibility(item)).probe_id
    assert replacement is not None and replacement != first_probe
    assert (
        await store.record_failure(item, replacement)
    ).resulting_state is CircuitState.OPEN
    await redis_runtime.client.delete(
        circuit_key(item), f"{circuit_key(item)}:failures"
    )
    new = await store.check_or_claim_eligibility(item)
    assert new.normal_token.incarnation_id != token.incarnation_id
    assert not (await store.record_failure(item, token)).applied


@pytest.mark.asyncio
async def test_noscript_reload(redis_runtime):
    item = identity()
    store = RedisCircuitStore(redis_runtime, config())
    admitted = await store.check_or_claim_eligibility(item)
    await redis_runtime.client.script_flush()
    assert (await store.record_failure(item, admitted.normal_token)).applied


@pytest.mark.asyncio
async def test_metadata_loss_drops_old_failure_history(redis_runtime):
    item = identity()
    store = RedisCircuitStore(redis_runtime, config(failure_threshold=3))
    old = (await store.check_or_claim_eligibility(item)).normal_token
    await store.record_failure(item, old)
    assert await redis_runtime.client.zcard(f"{circuit_key(item)}:failures") == 1
    await redis_runtime.client.delete(circuit_key(item))
    new = (await store.check_or_claim_eligibility(item)).normal_token
    assert new.incarnation_id != old.incarnation_id
    assert await redis_runtime.client.zcard(f"{circuit_key(item)}:failures") == 0
    assert not (await store.record_failure(item, old)).applied


@pytest.mark.asyncio
async def test_generation_overflow_keeps_probe_owner(redis_runtime):
    item = identity()
    store = RedisCircuitStore(
        redis_runtime, config(open_duration_ms=20, probe_lease_duration_ms=5000)
    )
    token = (await store.check_or_claim_eligibility(item)).normal_token
    await redis_runtime.client.hset(circuit_key(item), "generation", 9007199254740991)
    maximal = (await store.check_or_claim_eligibility(item)).normal_token
    await store.record_failure(item, maximal)
    await store.record_failure(item, maximal)
    await asyncio.sleep(0.03)
    probe = (await store.check_or_claim_eligibility(item)).probe_id
    with pytest.raises(CircuitStateError):
        await store.record_success(item, probe)
    assert await redis_runtime.client.hget(circuit_key(item), "state") == "HALF_OPEN"
    assert await redis_runtime.client.hget(circuit_key(item), "probe_owner") == str(
        probe
    )
    assert not (await store.record_failure(item, token)).applied


@pytest.mark.asyncio
async def test_deadline_overflow_does_not_partially_insert_failure(redis_runtime):
    item = identity()
    store = RedisCircuitStore(
        redis_runtime, config(failure_threshold=1, open_duration_ms=9007199254740991)
    )
    token = (await store.check_or_claim_eligibility(item)).normal_token
    with pytest.raises(CircuitStateError):
        await store.record_failure(item, token)
    assert await redis_runtime.client.zcard(f"{circuit_key(item)}:failures") == 0
    assert await redis_runtime.client.hget(circuit_key(item), "state") == "CLOSED"


@pytest.mark.asyncio
async def test_sliding_window_prunes_old_failure_but_keeps_recent(redis_runtime):
    item = identity()
    store = RedisCircuitStore(redis_runtime, config(failure_window_ms=1000))
    token = (await store.check_or_claim_eligibility(item)).normal_token
    seconds, micros = await redis_runtime.client.time()
    now = seconds * 1000 + micros // 1000
    await redis_runtime.client.zadd(
        f"{circuit_key(item)}:failures", {"boundary": now - 1000, "recent": now - 500}
    )
    result = await store.record_failure(item, token)
    assert result.resulting_state is CircuitState.OPEN
    assert await redis_runtime.client.zcard(f"{circuit_key(item)}:failures") == 0


@pytest.mark.asyncio
async def test_sliding_window_excludes_scores_after_redis_now(redis_runtime):
    item = identity()
    store = RedisCircuitStore(redis_runtime, config())
    token = (await store.check_or_claim_eligibility(item)).normal_token
    seconds, micros = await redis_runtime.client.time()
    now = seconds * 1000 + micros // 1000
    await redis_runtime.client.zadd(
        f"{circuit_key(item)}:failures", {"future": now + 10000}
    )
    first = await store.record_failure(item, token)
    assert first.resulting_state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_dependency_error_suppresses_raw_redis_exception_context():
    from redis.exceptions import ConnectionError as RedisConnectionError

    class FailingClient:
        async def script_load(self, script):
            raise RedisConnectionError("redis://secret@host")

    class FailingRuntime:
        client = FailingClient()

    store = RedisCircuitStore(FailingRuntime(), config())
    with pytest.raises(CircuitDependencyError) as caught:
        await store.check_or_claim_eligibility(identity())
    assert caught.value.__suppress_context__
    assert caught.value.__cause__ is None
    assert str(caught.value) == "Circuit dependency is unavailable."


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_OUTAGE_TEST") != "1",
    reason="controlled Compose Redis restart test is opt-in",
)
@pytest.mark.asyncio
async def test_restart_reloads_script_and_reincarnates_after_state_loss(redis_runtime):
    item = identity()
    store = RedisCircuitStore(redis_runtime, config())
    old = (await store.check_or_claim_eligibility(item)).normal_token

    async def compose(action: str) -> None:
        process = await asyncio.create_subprocess_exec(
            "docker", "compose", action, "redis"
        )
        assert await asyncio.wait_for(process.wait(), timeout=30) == 0

    try:
        await compose("stop")
        await compose("start")
        for _ in range(20):
            if (await redis_runtime.check()).available:
                break
            await asyncio.sleep(0.25)
        else:
            pytest.fail("Redis did not recover within the bounded test window")
        # Compose persistence can retain circuit state; remove only this test key.
        await redis_runtime.client.delete(
            circuit_key(item), f"{circuit_key(item)}:failures"
        )
        new = (await store.check_or_claim_eligibility(item)).normal_token
        assert new.incarnation_id != old.incarnation_id
        assert not (await store.record_failure(item, old)).applied
    finally:
        await compose("start")
