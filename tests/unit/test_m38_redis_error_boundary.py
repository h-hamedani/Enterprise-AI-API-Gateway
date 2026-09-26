import asyncio
from uuid import uuid4

import pytest
from redis import exceptions as redis_errors

from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitDependencyError,
    CircuitIdentity,
    CircuitProtocolError,
    CircuitStateError,
    RedisCircuitStore,
)
from app.redis.concurrency import (
    ConcurrencyDependencyError,
    ConcurrencyProtocolError,
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
)
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RateLimitProtocolError,
    RedisTokenBucket,
    ResolvedRatePolicy,
)
from app.redis.redis_failure import is_redis_availability_failure


class Client:
    def __init__(self, *, error=None, result=None):
        self.error = error
        self.result = result
        self.loads = 0
        self.evaluations = 0

    async def script_load(self, script):
        self.loads += 1
        return "sha"

    async def evalsha(self, *args):
        self.evaluations += 1
        if self.error is not None:
            raise self.error
        return self.result


class Runtime:
    def __init__(self, client):
        self.client = client


@pytest.mark.parametrize(
    "error,eligible",
    [
        (redis_errors.ConnectionError(), True),
        (redis_errors.TimeoutError(), True),
        (TimeoutError(), True),
        (redis_errors.BusyLoadingError(), True),
        (redis_errors.MaxConnectionsError(), True),
        (redis_errors.AuthenticationError(), False),
        (redis_errors.AuthorizationError(), False),
        (redis_errors.ExternalAuthProviderError(), False),
        (redis_errors.NoPermissionError(), False),
        (redis_errors.ResponseError(), False),
        (redis_errors.ReadOnlyError(), False),
        (redis_errors.NoScriptError(), False),
        (redis_errors.DataError(), False),
        (asyncio.CancelledError(), False),
    ],
)
def test_exact_redis_availability_whitelist(error, eligible):
    assert is_redis_availability_failure(error) is eligible


def _rate_policy():
    return ResolvedRatePolicy(uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 10, 60)


def _concurrency_policy():
    return ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 2
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected",
    [
        (redis_errors.ConnectionError(), RateLimitDependencyError),
        (redis_errors.TimeoutError(), RateLimitDependencyError),
        (redis_errors.AuthenticationError(), redis_errors.AuthenticationError),
        (redis_errors.ResponseError(), redis_errors.ResponseError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_rate_failure_classification(error, expected):
    client = Client(error=error)
    with pytest.raises(expected):
        await RedisTokenBucket(Runtime(client)).evaluate([_rate_policy()])


@pytest.mark.asyncio
async def test_rate_malformed_result_is_not_dependency():
    with pytest.raises(RateLimitProtocolError):
        await RedisTokenBucket(Runtime(Client(result=["not-a-number", 0]))).evaluate(
            [_rate_policy()]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected",
    [
        (redis_errors.ConnectionError(), ConcurrencyDependencyError),
        (redis_errors.TimeoutError(), ConcurrencyDependencyError),
        (redis_errors.AuthenticationError(), redis_errors.AuthenticationError),
        (redis_errors.NoPermissionError(), redis_errors.NoPermissionError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_concurrency_failure_classification(error, expected):
    with pytest.raises(expected):
        await RedisConcurrencySemaphore(Runtime(Client(error=error)), 30000).acquire(
            [_concurrency_policy()]
        )


@pytest.mark.asyncio
async def test_concurrency_malformed_result_is_not_dependency():
    with pytest.raises(ConcurrencyProtocolError):
        await RedisConcurrencySemaphore(Runtime(Client(result=[1])), 30000).acquire(
            [_concurrency_policy()]
        )


def _circuit_identity():
    return CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())


def _circuit_config():
    return CircuitConfig(5, 60000, 30000, 1, 1, 30000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected",
    [
        (redis_errors.ConnectionError(), CircuitDependencyError),
        (redis_errors.TimeoutError(), CircuitDependencyError),
        (redis_errors.AuthenticationError(), redis_errors.AuthenticationError),
        (redis_errors.ResponseError(), redis_errors.ResponseError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_circuit_failure_classification(error, expected):
    with pytest.raises(expected):
        await RedisCircuitStore(
            Runtime(Client(error=error)), _circuit_config()
        ).check_or_claim_eligibility(_circuit_identity())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row,expected",
    [
        (["bad", "CLOSED", "denied"], CircuitProtocolError),
        (["overflow", "CLOSED", "error"], CircuitStateError),
        (["ok", "BROKEN", "denied"], CircuitProtocolError),
        (["ok", "CLOSED", "normal", "not-a-uuid", "1"], CircuitProtocolError),
        (["ok", "CLOSED", "normal", str(uuid4()).upper(), "1"], CircuitProtocolError),
        (["ok", "CLOSED", "normal", str(uuid4()), "nan"], CircuitProtocolError),
        (["ok", "CLOSED", "probe", "not-a-uuid"], CircuitProtocolError),
        (["ok", "HALF_OPEN", "probe", str(uuid4()).upper()], CircuitProtocolError),
    ],
)
async def test_circuit_non_dependency_rows(row, expected):
    with pytest.raises(expected):
        await RedisCircuitStore(
            Runtime(Client(result=row)), _circuit_config()
        ).check_or_claim_eligibility(_circuit_identity())


class NoScriptThen:
    def __init__(self, second):
        self.second = second
        self.loads = 0
        self.calls = 0

    async def script_load(self, script):
        self.loads += 1
        return "sha"

    async def evalsha(self, *args):
        self.calls += 1
        if self.calls == 1:
            raise redis_errors.NoScriptError()
        if isinstance(self.second, BaseException):
            raise self.second
        return self.second


@pytest.mark.asyncio
async def test_rate_noscript_reloads_once_then_succeeds():
    client = NoScriptThen([1, 0])
    assert (await RedisTokenBucket(Runtime(client)).evaluate([_rate_policy()])).allowed
    assert (client.loads, client.calls) == (2, 2)


@pytest.mark.asyncio
async def test_rate_noscript_then_transport_failure_is_dependency():
    client = NoScriptThen(redis_errors.ConnectionError())
    with pytest.raises(RateLimitDependencyError):
        await RedisTokenBucket(Runtime(client)).evaluate([_rate_policy()])
    assert (client.loads, client.calls) == (2, 2)


@pytest.mark.asyncio
async def test_repeated_noscript_is_not_redis_availability():
    client = NoScriptThen(redis_errors.NoScriptError())
    with pytest.raises(redis_errors.NoScriptError):
        await RedisTokenBucket(Runtime(client)).evaluate([_rate_policy()])
    assert (client.loads, client.calls) == (2, 2)


@pytest.mark.asyncio
async def test_concurrency_noscript_reloads_once():
    client = NoScriptThen(1)
    result = await RedisConcurrencySemaphore(Runtime(client), 30000).acquire(
        [_concurrency_policy()]
    )
    assert result.acquired
    assert (client.loads, client.calls) == (2, 2)


@pytest.mark.asyncio
async def test_circuit_noscript_then_transport_failure_is_dependency():
    client = NoScriptThen(redis_errors.TimeoutError())
    with pytest.raises(CircuitDependencyError):
        await RedisCircuitStore(
            Runtime(client), _circuit_config()
        ).check_or_claim_eligibility(_circuit_identity())
    assert (client.loads, client.calls) == (2, 2)
