import base64
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis import exceptions as redis_errors

from app.control_plane.protection import ControlPlaneProtection
from app.core.config import Settings
from app.core.errors import GatewayHttpError
from app.persistence.models.enums import RateScopeType
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.rate_limit import ResolvedRatePolicy
from app.redis.runtime import RedisAvailability, RedisRuntime


class Client:
    def __init__(self, error=None, result=None):
        self.error = error
        self.result = result
        self.calls = 0
        self.pings = 0

    async def script_load(self, script):
        self.calls += 1
        if self.error:
            raise self.error
        return "sha"

    async def evalsha(self, *args):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result

    async def ping(self):
        self.pings += 1
        return True

    async def aclose(self):
        return None


class Runtime:
    def __init__(self, client):
        self.client = client


def protection(error=None, result=None):
    secret = base64.b64encode(b"k" * 32).decode()
    settings = Settings(_env_file=None, credential_hmac_secret=secret)
    return ControlPlaneProtection(Runtime(Client(error, result)), settings)


def request(peer="203.0.113.7", **headers):
    return SimpleNamespace(
        client=SimpleNamespace(host=peer),
        headers={key.lower(): value for key, value in headers.items()},
    )


@pytest.mark.asyncio
async def test_real_control_plane_pre_auth_falls_back_to_five_of_sixty():
    guard = protection(redis_errors.ConnectionError())
    incoming = request()
    for _ in range(5):
        await guard.pre_auth(incoming)
    with pytest.raises(GatewayHttpError) as caught:
        await guard.pre_auth(incoming)
    assert caught.value.status_code == 429
    assert caught.value.headers["Retry-After"] == "12"
    assert guard.local.mode_degraded
    assert guard.local.pre_auth.entry_count == 1


@pytest.mark.asyncio
async def test_pre_auth_stays_local_after_redis_client_recovers():
    client = Client(redis_errors.ConnectionError(), [1, 0])
    secret = base64.b64encode(b"k" * 32).decode()
    guard = ControlPlaneProtection(
        Runtime(client), Settings(_env_file=None, credential_hmac_secret=secret)
    )
    incoming = request()
    await guard.pre_auth(incoming)
    assert guard.local.mode_degraded and guard.local.pre_auth.entry_count == 1
    calls_before_recovery = client.calls
    client.error = None
    for _ in range(4):
        await guard.pre_auth(incoming)
    with pytest.raises(GatewayHttpError) as caught:
        await guard.pre_auth(incoming)
    assert caught.value.status_code == 429
    assert client.calls == calls_before_recovery
    assert guard.local.mode_degraded and guard.local.pre_auth.entry_count == 1


@pytest.mark.asyncio
async def test_admin_token_stays_local_after_redis_client_recovers():
    client = Client(redis_errors.ConnectionError(), [1, 0])
    secret = base64.b64encode(b"k" * 32).decode()
    guard = ControlPlaneProtection(
        Runtime(client), Settings(_env_file=None, credential_hmac_secret=secret)
    )
    item = ResolvedRatePolicy(
        uuid4(), uuid4(), RateScopeType.ADMIN_TOKEN, uuid4(), 8, 60
    )
    await guard._evaluate([item])
    assert guard.local.mode_degraded and guard.local.rate.entry_count == 1
    calls_before_recovery = client.calls
    client.error = None
    await guard._evaluate([item])
    with pytest.raises(GatewayHttpError) as caught:
        await guard._evaluate([item])
    assert caught.value.status_code == 429
    assert client.calls == calls_before_recovery
    assert guard.local.mode_degraded


@pytest.mark.asyncio
async def test_available_redis_health_probe_does_not_exit_degraded_mode():
    client = Client(redis_errors.ConnectionError(), [1, 0])
    runtime = RedisRuntime(
        "redis://test.invalid/0", 0.2, factory=lambda *a, **kw: client
    )
    await runtime.start()
    secret = base64.b64encode(b"k" * 32).decode()
    guard = ControlPlaneProtection(
        runtime, Settings(_env_file=None, credential_hmac_secret=secret)
    )
    try:
        incoming = request()
        await guard.pre_auth(incoming)
        assert guard.local.mode_degraded and guard.local.pre_auth.entry_count == 1
        calls_before_health = client.calls
        client.error = None
        health = await runtime.check()
        assert health.state is RedisAvailability.AVAILABLE and client.pings == 1
        await guard.pre_auth(incoming)
        assert client.calls == calls_before_health
        assert guard.local.mode_degraded and guard.local.pre_auth.entry_count == 1
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_process_degraded_elsewhere_bypasses_redis_for_both_admin_guards():
    local = LocalDegradedProtection()
    local.mark_unreachable()
    client = Client(result=[1, 0])
    secret = base64.b64encode(b"k" * 32).decode()
    guard = ControlPlaneProtection(
        Runtime(client),
        Settings(_env_file=None, credential_hmac_secret=secret),
        local=local,
    )
    await guard.pre_auth(request())
    item = ResolvedRatePolicy(
        uuid4(), uuid4(), RateScopeType.ADMIN_TOKEN, uuid4(), 4, 60
    )
    await guard._evaluate([item])
    assert client.calls == 0
    assert local.pre_auth.entry_count == 1 and local.rate.entry_count == 1
    assert local.mode_degraded


@pytest.mark.asyncio
async def test_trusted_proxy_identity_is_preserved_when_degraded():
    guard = protection(redis_errors.ConnectionError())
    guard._settings.trusted_proxy_cidrs = ("10.0.0.0/8",)
    for _ in range(5):
        await guard.pre_auth(request("10.1.2.3", **{"X-Forwarded-For": "203.0.113.9"}))
    with pytest.raises(GatewayHttpError) as caught:
        await guard.pre_auth(request("10.1.2.3", **{"X-Forwarded-For": "203.0.113.9"}))
    assert caught.value.status_code == 429
    await guard.pre_auth(request("10.1.2.3", **{"X-Forwarded-For": "203.0.113.10"}))


@pytest.mark.asyncio
async def test_post_auth_policy_uses_its_degraded_factor():
    guard = protection(redis_errors.ConnectionError())
    item = ResolvedRatePolicy(
        uuid4(),
        uuid4(),
        RateScopeType.ADMIN_TOKEN,
        uuid4(),
        8,
        60,
        degraded_factor=0.5,
    )
    for _ in range(4):
        await guard._evaluate([item])
    with pytest.raises(GatewayHttpError) as caught:
        await guard._evaluate([item])
    assert caught.value.status_code == 429
    assert guard.local.rate.entry_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [redis_errors.AuthenticationError(), redis_errors.ResponseError()]
)
async def test_non_dependency_control_plane_error_does_not_fallback(error):
    guard = protection(error)
    with pytest.raises(type(error)):
        await guard.pre_auth(request())
    assert not guard.local.mode_degraded
    assert guard.local.pre_auth.entry_count == 0
