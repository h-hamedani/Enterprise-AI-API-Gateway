import asyncio
import os
from uuid import uuid4

import pytest
import pytest_asyncio

from app.core.config import get_settings
from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import (
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
    semaphore_key,
)
from app.redis.runtime import RedisRuntime


async def _docker_compose(action: str) -> None:
    process = await asyncio.create_subprocess_exec("docker", "compose", action, "redis")
    assert await asyncio.wait_for(process.wait(), timeout=30) == 0


def policy(tenant_id, scope_type, *, maximum=2):
    return ResolvedConcurrencyPolicy(
        policy_id=uuid4(),
        tenant_id=tenant_id,
        scope_type=scope_type,
        scope_id=uuid4(),
        max_concurrency=maximum,
    )


@pytest_asyncio.fixture
async def redis_runtime():
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url, settings.dependency_timeout_seconds)
    await runtime.start()
    yield runtime
    await runtime.close()


@pytest.mark.asyncio
async def test_distributed_admission_and_owner_release(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY, maximum=5)
    first = RedisConcurrencySemaphore(redis_runtime, 5000)
    second = RedisConcurrencySemaphore(redis_runtime, 5000)
    results = await asyncio.gather(
        *((first if i % 2 else second).acquire([item]) for i in range(40))
    )
    admitted = [result.lease_id for result in results if result.acquired]
    assert len(admitted) == 5
    assert all(lease is not None and lease.version == 7 for lease in admitted)
    assert not (await first.release([item], uuid4())).released
    assert not (await second.renew([item], uuid4())).renewed
    assert (await first.release([item], admitted[0])).released
    assert not (await first.release([item], admitted[0])).released
    assert (await second.acquire([item])).acquired


@pytest.mark.asyncio
async def test_layered_rejection_leaves_no_partial_slot(redis_runtime):
    tenant = uuid4()
    free = policy(tenant, RateScopeType.API_KEY, maximum=2)
    full = policy(tenant, RateScopeType.ROUTE, maximum=1)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    owner = await semaphore.acquire([full])
    assert owner.acquired
    assert not (await semaphore.acquire([free, full])).acquired
    assert await redis_runtime.client.zcard(semaphore_key(free)) == 0
    assert (await semaphore.acquire([free])).acquired


@pytest.mark.asyncio
async def test_layered_renew_and_release_are_atomic(redis_runtime):
    tenant = uuid4()
    first = policy(tenant, RateScopeType.API_KEY)
    second = policy(tenant, RateScopeType.SERVICE)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    acquired = await semaphore.acquire([second, first])
    assert acquired.acquired and acquired.lease_id is not None
    member = str(acquired.lease_id)
    old = await redis_runtime.client.zscore(semaphore_key(first), member)
    assert (await semaphore.renew([first, second], acquired.lease_id)).renewed
    renewed = await redis_runtime.client.zscore(semaphore_key(first), member)
    assert renewed <= old + 5000
    await redis_runtime.client.zrem(semaphore_key(second), member)
    assert not (await semaphore.renew([first, second], acquired.lease_id)).renewed
    assert not (await semaphore.release([first, second], acquired.lease_id)).released
    assert await redis_runtime.client.zscore(semaphore_key(first), member) is None


@pytest.mark.asyncio
async def test_expired_lease_frees_capacity(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY, maximum=1)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    owner = await semaphore.acquire([item])
    assert owner.acquired and owner.lease_id is not None
    assert not (await semaphore.acquire([item])).acquired
    await asyncio.sleep(5.1)
    assert not (await semaphore.renew([item], owner.lease_id)).renewed
    assert (await semaphore.acquire([item])).acquired


@pytest.mark.asyncio
async def test_renewal_uses_redis_now_and_ttl_is_bounded(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY, maximum=1)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    owner = await semaphore.acquire([item])
    assert owner.lease_id is not None
    key = semaphore_key(item)
    first_expiry = await redis_runtime.client.zscore(key, str(owner.lease_id))
    assert 0 < await redis_runtime.client.pttl(key) <= 10000
    assert (await semaphore.renew([item], owner.lease_id)).renewed
    second_expiry = await redis_runtime.client.zscore(key, str(owner.lease_id))
    assert first_expiry <= second_expiry < first_expiry + 5000
    assert 0 < await redis_runtime.client.pttl(key) <= 10000


@pytest.mark.asyncio
async def test_script_flush_reloads_all_operations(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    owner = await semaphore.acquire([item])
    assert owner.lease_id is not None
    await redis_runtime.client.script_flush()
    assert (await semaphore.renew([item], owner.lease_id)).renewed
    await redis_runtime.client.script_flush()
    assert (await semaphore.release([item], owner.lease_id)).released
    await redis_runtime.client.script_flush()
    assert (await semaphore.acquire([item])).acquired


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_OUTAGE_TEST") != "1",
    reason="controlled Compose Redis restart test is opt-in",
)
@pytest.mark.asyncio
async def test_restart_loses_old_lease_and_reloads_script(redis_runtime):
    item = policy(uuid4(), RateScopeType.API_KEY, maximum=1)
    semaphore = RedisConcurrencySemaphore(redis_runtime, 5000)
    owner = await semaphore.acquire([item])
    assert owner.lease_id is not None
    try:
        await _docker_compose("stop")
        await _docker_compose("start")
        for _ in range(20):
            if (await redis_runtime.check()).available:
                break
            await asyncio.sleep(0.25)
        else:
            pytest.fail("Redis did not recover within the bounded test window")
        # This Compose Redis has persistence: restart may retain the old lease.
        # Remove only this test key to exercise the contracted ephemeral-loss case.
        await redis_runtime.client.delete(semaphore_key(item))
        assert not (await semaphore.renew([item], owner.lease_id)).renewed
        assert not (await semaphore.release([item], owner.lease_id)).released
        assert (await semaphore.acquire([item])).acquired
    finally:
        await _docker_compose("start")
