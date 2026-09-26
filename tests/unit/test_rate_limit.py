from uuid import uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.persistence.models.enums import RateScopeType
from app.redis.rate_limit import (
    MAX_EXACT_LUA_INTEGER,
    MAX_POSTGRES_INTEGER,
    RateLimitDependencyError,
    RateLimitPolicyError,
    RedisTokenBucket,
    ResolvedRatePolicy,
    rate_limit_key,
)


def policy(**overrides) -> ResolvedRatePolicy:
    values = {
        "policy_id": uuid4(),
        "tenant_id": uuid4(),
        "scope_type": RateScopeType.API_KEY,
        "scope_id": uuid4(),
        "requests_per_window": 10,
        "window_seconds": 60,
    }
    values.update(overrides)
    return ResolvedRatePolicy(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("requests_per_window", 0),
        ("requests_per_window", -1),
        ("requests_per_window", 1.0),
        ("requests_per_window", "1e3"),
        ("window_seconds", 0),
        ("window_seconds", -1),
        ("window_seconds", 1.5),
    ],
)
def test_policy_rejects_non_positive_or_non_integral_values(field, value) -> None:
    with pytest.raises(RateLimitPolicyError):
        policy(**{field: value})


def test_policy_accepts_exact_numeric_bound_and_rejects_overflow() -> None:
    largest_safe_window = MAX_EXACT_LUA_INTEGER // (MAX_POSTGRES_INTEGER * 1000)
    exact = policy(
        requests_per_window=MAX_POSTGRES_INTEGER,
        window_seconds=largest_safe_window,
    )
    assert exact.capacity_units <= MAX_EXACT_LUA_INTEGER

    with pytest.raises(RateLimitPolicyError):
        policy(
            requests_per_window=MAX_POSTGRES_INTEGER,
            window_seconds=largest_safe_window + 1,
        )


def test_key_is_typed_tenant_scoped_and_canonical() -> None:
    item = policy()
    assert rate_limit_key(item) == (
        f"gw:v1:rl:{{{item.tenant_id}}}:{item.scope_type.value}:{item.scope_id}"
    )


class RecordingRedis:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.result = result or [1, 0]
        self.error = error
        self.calls = []

    async def script_load(self, script):
        self.calls.append(("script_load",))
        return "sha"

    async def evalsha(self, sha, count, *args):
        self.calls.append(("evalsha", sha, count, *args))
        if self.error:
            raise self.error
        return self.result


class Runtime:
    def __init__(self, client) -> None:
        self.client = client


@pytest.mark.asyncio
async def test_empty_policy_set_allows_without_redis_call() -> None:
    redis = RecordingRedis()
    result = await RedisTokenBucket(Runtime(redis)).evaluate([])
    assert result.allowed is True
    assert result.retry_after_ms == 0
    assert redis.calls == []


@pytest.mark.asyncio
async def test_mixed_tenant_set_fails_before_redis_call() -> None:
    redis = RecordingRedis()
    with pytest.raises(RateLimitPolicyError):
        await RedisTokenBucket(Runtime(redis)).evaluate([policy(), policy()])
    assert redis.calls == []


@pytest.mark.asyncio
async def test_script_input_is_deterministically_layer_ordered() -> None:
    tenant_id = uuid4()
    service = policy(tenant_id=tenant_id, scope_type=RateScopeType.SERVICE)
    api_key = policy(tenant_id=tenant_id, scope_type=RateScopeType.API_KEY)
    route = policy(tenant_id=tenant_id, scope_type=RateScopeType.ROUTE)
    redis = RecordingRedis()

    await RedisTokenBucket(Runtime(redis)).evaluate([service, route, api_key])

    eval_call = redis.calls[-1]
    assert list(eval_call[3:6]) == [
        rate_limit_key(api_key),
        rate_limit_key(route),
        rate_limit_key(service),
    ]


@pytest.mark.asyncio
async def test_redis_failure_is_typed_and_secret_safe() -> None:
    secret = "redis://user:password@host"
    redis = RecordingRedis(error=RedisConnectionError(secret))
    item = policy()

    with pytest.raises(RateLimitDependencyError) as caught:
        await RedisTokenBucket(Runtime(redis)).evaluate([item])

    assert secret not in str(caught.value)
    assert rate_limit_key(item) not in str(caught.value)
