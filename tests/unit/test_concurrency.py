from uuid import uuid4

import pytest

from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import (
    ConcurrencyPolicyError,
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
    semaphore_key,
)


def policy(**overrides):
    values = {
        "policy_id": uuid4(),
        "tenant_id": uuid4(),
        "scope_type": RateScopeType.API_KEY,
        "scope_id": uuid4(),
        "max_concurrency": 2,
    }
    values.update(overrides)
    return ResolvedConcurrencyPolicy(**values)


@pytest.mark.parametrize("value", [0, -1, True, 1.0, "1", 2**31])
def test_invalid_limit(value):
    with pytest.raises(ConcurrencyPolicyError):
        policy(max_concurrency=value)


def test_canonical_key():
    item = policy()
    assert semaphore_key(item) == (
        f"gw:v1:sem:{{{item.tenant_id}}}:{item.scope_type.value}:{item.scope_id}"
    )


class NoRedis:
    @property
    def client(self):
        raise AssertionError("Redis must not be used")


@pytest.mark.asyncio
async def test_empty_policy_set_needs_no_redis():
    result = await RedisConcurrencySemaphore(NoRedis(), 30000).acquire([])
    assert result.acquired is True
    assert result.lease_id is None


@pytest.mark.asyncio
async def test_mixed_tenant_and_duplicate_scope_fail_before_redis():
    semaphore = RedisConcurrencySemaphore(NoRedis(), 30000)
    with pytest.raises(ConcurrencyPolicyError):
        await semaphore.acquire([policy(), policy()])
    first = policy()
    duplicate = policy(
        tenant_id=first.tenant_id,
        scope_type=first.scope_type,
        scope_id=first.scope_id,
    )
    with pytest.raises(ConcurrencyPolicyError):
        await semaphore.acquire([first, duplicate])
