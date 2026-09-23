from uuid import uuid4

import pytest
import pytest_asyncio

from app.control_plane.protection import pre_auth_policy
from app.core.config import get_settings
from app.persistence.models.enums import RateScopeType
from app.redis.rate_limit import RedisTokenBucket, ResolvedRatePolicy
from app.redis.runtime import RedisRuntime


@pytest_asyncio.fixture
async def redis_peers():
    settings = get_settings()
    first = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    second = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    await first.start()
    await second.start()
    yield first, second
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_pre_auth_bucket_is_shared_across_instances(redis_peers):
    first, second = redis_peers
    key = pre_auth_policy(f"203.0.113.{uuid4().int % 250 + 1}", b"m" * 32)
    results = await __import__("asyncio").gather(
        *(
            (RedisTokenBucket(first) if i % 2 else RedisTokenBucket(second)).evaluate(
                [key]
            )
            for i in range(25)
        )
    )
    assert sum(result.allowed for result in results) == 20
    assert all(result.retry_after_ms > 0 for result in results if not result.allowed)


@pytest.mark.asyncio
async def test_admin_token_buckets_are_shared_and_token_scoped(redis_peers):
    first, second = redis_peers
    tenant = uuid4()
    token_a, token_b = uuid4(), uuid4()

    def policy(token):
        return ResolvedRatePolicy(
            uuid4(), tenant, RateScopeType.ADMIN_TOKEN, token, 2, 60
        )

    policies = [policy(token_a)]
    results = await __import__("asyncio").gather(
        *(
            (RedisTokenBucket(first) if i % 2 else RedisTokenBucket(second)).evaluate(
                policies
            )
            for i in range(3)
        )
    )
    assert sum(result.allowed for result in results) == 2
    other = await RedisTokenBucket(second).evaluate([policy(token_b)])
    assert other.allowed
