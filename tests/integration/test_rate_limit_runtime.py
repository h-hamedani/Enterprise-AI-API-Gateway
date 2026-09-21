import asyncio
import os
from uuid import uuid4

import pytest
import pytest_asyncio

from app.core.config import get_settings
from app.persistence.models.enums import RateScopeType
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RedisTokenBucket,
    ResolvedRatePolicy,
    rate_limit_key,
)
from app.redis.runtime import RedisRuntime


async def _docker_compose(action: str) -> None:
    process = await asyncio.create_subprocess_exec("docker", "compose", action, "redis")
    assert await asyncio.wait_for(process.wait(), timeout=30) == 0


def policy(tenant_id, scope_type, *, capacity=3, window=60, scope_id=None):
    return ResolvedRatePolicy(
        policy_id=uuid4(),
        tenant_id=tenant_id,
        scope_type=scope_type,
        scope_id=scope_id or uuid4(),
        requests_per_window=capacity,
        window_seconds=window,
    )


@pytest_asyncio.fixture
async def redis_runtime():
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url, settings.dependency_timeout_seconds)
    await runtime.start()
    await runtime.client.flushdb()
    yield runtime
    await runtime.client.flushdb()
    await runtime.close()


@pytest.mark.asyncio
async def test_real_redis_capacity_ttl_and_tenant_isolation(redis_runtime) -> None:
    shared_scope = uuid4()
    first = policy(uuid4(), RateScopeType.API_KEY, capacity=2, scope_id=shared_scope)
    second = policy(uuid4(), RateScopeType.API_KEY, capacity=2, scope_id=shared_scope)
    limiter = RedisTokenBucket(redis_runtime)

    assert (await limiter.evaluate([first])).allowed
    assert (await limiter.evaluate([first])).allowed
    rejected = await limiter.evaluate([first])
    assert not rejected.allowed
    assert rejected.retry_after_ms > 0
    assert (await limiter.evaluate([second])).allowed
    ttl = await redis_runtime.client.pttl(rate_limit_key(first))
    assert 119_000 <= ttl <= 120_000


@pytest.mark.asyncio
async def test_real_redis_multi_client_concurrency_never_over_admits(redis_runtime):
    item = policy(uuid4(), RateScopeType.ROUTE, capacity=25)
    first = RedisTokenBucket(redis_runtime)
    second = RedisTokenBucket(redis_runtime)
    results = await asyncio.gather(
        *((first if index % 2 else second).evaluate([item]) for index in range(100))
    )
    assert sum(result.allowed for result in results) == 25


@pytest.mark.asyncio
async def test_real_redis_rejection_does_not_partially_consume(redis_runtime):
    tenant_id = uuid4()
    allowing = policy(tenant_id, RateScopeType.API_KEY, capacity=2)
    rejecting = policy(tenant_id, RateScopeType.ROUTE, capacity=1)
    limiter = RedisTokenBucket(redis_runtime)
    assert (await limiter.evaluate([rejecting])).allowed

    layered = await limiter.evaluate([allowing, rejecting])
    assert not layered.allowed

    # The rejected layered request did not consume the otherwise-full bucket.
    assert (await limiter.evaluate([allowing])).allowed
    assert (await limiter.evaluate([allowing])).allowed
    assert not (await limiter.evaluate([allowing])).allowed


@pytest.mark.asyncio
async def test_real_redis_multiple_rejectors_return_maximum_delay(redis_runtime):
    tenant_id = uuid4()
    fast = policy(tenant_id, RateScopeType.API_KEY, capacity=1, window=10)
    slow = policy(tenant_id, RateScopeType.SERVICE, capacity=1, window=30)
    limiter = RedisTokenBucket(redis_runtime)
    assert (await limiter.evaluate([fast])).allowed
    assert (await limiter.evaluate([slow])).allowed

    rejected = await limiter.evaluate([fast, slow])
    assert not rejected.allowed
    assert 29_000 <= rejected.retry_after_ms <= 30_000


@pytest.mark.asyncio
async def test_real_redis_uses_server_time_and_recovers_after_script_flush(
    redis_runtime, monkeypatch
):
    item = policy(uuid4(), RateScopeType.LLM_MODEL, capacity=1, window=1)
    limiter = RedisTokenBucket(redis_runtime)
    monkeypatch.setattr("time.time", lambda: -(10**12))

    assert (await limiter.evaluate([item])).allowed
    assert not (await limiter.evaluate([item])).allowed
    await redis_runtime.client.script_flush()
    await asyncio.sleep(1.01)
    assert (await limiter.evaluate([item])).allowed


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_OUTAGE_TEST") != "1",
    reason="controlled Compose Redis restart test is opt-in",
)
@pytest.mark.asyncio
async def test_real_redis_restart_reloads_script_with_same_limiter(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY, capacity=2)
    limiter = RedisTokenBucket(redis_runtime)
    assert (await limiter.evaluate([item])).allowed
    try:
        await _docker_compose("stop")
        await _docker_compose("start")
        for _ in range(20):
            try:
                result = await limiter.evaluate([item])
                break
            except RateLimitDependencyError:  # Redis is still starting.
                await asyncio.sleep(0.25)
        else:
            pytest.fail("Redis did not recover within the bounded test window")
        assert isinstance(result.allowed, bool)
    finally:
        await _docker_compose("start")
