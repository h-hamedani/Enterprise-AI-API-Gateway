"""Real Redis socket success and connection-refusal degraded acceptance."""

from uuid import uuid4

import pytest

from app.core.config import get_settings
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitIdentity,
    CircuitState,
    RedisCircuitStore,
)
from app.redis.concurrency import RedisConcurrencySemaphore, ResolvedConcurrencyPolicy
from app.redis.degraded import (
    DegradedCircuitStore,
    DegradedConcurrencySemaphore,
    DegradedRateLimiter,
)
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.rate_limit import RedisTokenBucket, ResolvedRatePolicy
from app.redis.runtime import RedisRuntime


def policy():
    return ResolvedRatePolicy(uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4, 60)


def concurrency_policy():
    return ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 4
    )


def circuit_config():
    return CircuitConfig(5, 60000, 30000, 1, 1, 30000)


@pytest.mark.asyncio
async def test_real_socket_refusal_activates_bounded_local_rate():
    settings = get_settings()
    normal = RedisRuntime(settings.redis_url, settings.dependency_timeout_seconds)
    await normal.start()
    try:
        item = policy()
        assert (await RedisTokenBucket(normal).evaluate([item])).allowed
    finally:
        await normal.close()

    unavailable = RedisRuntime("redis://127.0.0.1:1/0", 0.2)
    await unavailable.start()
    try:
        local = LocalDegradedProtection()
        limiter = DegradedRateLimiter(RedisTokenBucket(unavailable), local)
        item = policy()
        assert (await limiter.evaluate([item])).allowed
        assert not (await limiter.evaluate([item])).allowed
        assert local.mode_degraded and local.reason == "redis_unreachable"
        assert local.rate.entry_count == 1
    finally:
        await unavailable.close()


@pytest.mark.asyncio
async def test_real_socket_refusal_activates_bounded_local_concurrency_and_circuit():
    unavailable = RedisRuntime("redis://127.0.0.1:1/0", 0.2)
    await unavailable.start()
    try:
        local_concurrency = LocalDegradedProtection(lease_duration_ms=5000)
        semaphore = DegradedConcurrencySemaphore(
            RedisConcurrencySemaphore(unavailable, 5000), local_concurrency
        )
        item = concurrency_policy()
        first = await semaphore.acquire([item])
        assert first.acquired and first.lease_id
        assert not (await semaphore.acquire([item])).acquired
        assert local_concurrency.mode_degraded

        local_circuit = LocalDegradedProtection()
        config = circuit_config()
        circuit = DegradedCircuitStore(
            RedisCircuitStore(unavailable, config), local_circuit, config
        )
        identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
        probe = await circuit.check_or_claim_eligibility(identity)
        assert probe.state is CircuitState.DEGRADED_HALF_OPEN and probe.probe_id
        assert not (await circuit.check_or_claim_eligibility(identity)).eligible
        assert (
            await circuit.record_failure(identity, probe.probe_id)
        ).resulting_state is CircuitState.OPEN
        assert not (await circuit.check_or_claim_eligibility(identity)).eligible
        assert local_circuit.mode_degraded
    finally:
        await unavailable.close()
