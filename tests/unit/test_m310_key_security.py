from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from app.control_plane.protection import pre_auth_policy
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import (
    CircuitConfig,
    CircuitIdentity,
    RedisCircuitStore,
    circuit_key,
)
from app.redis.concurrency import (
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
    semaphore_key,
)
from app.redis.rate_limit import (
    RateLimitPolicyError,
    RedisTokenBucket,
    ResolvedRatePolicy,
    rate_limit_key,
)


def test_raw_rate_key_override_is_rejected() -> None:
    with pytest.raises(RateLimitPolicyError):
        ResolvedRatePolicy(
            policy_id=uuid4(),
            tenant_id=uuid4(),
            scope_type=RateScopeType.API_KEY,
            scope_id=uuid4(),
            requests_per_window=1,
            window_seconds=60,
            key_override="gw:v1:injected:adm_secret:192.0.2.10",
        )


def test_internal_pre_auth_key_preserves_namespace_and_hides_raw_ip() -> None:
    policy = pre_auth_policy("192.0.2.10", b"internal-test-secret")
    key = rate_limit_key(policy)
    assert key.startswith("gw:v1:adminpre:ADMIN_AUTH_PROTECTED:{")
    assert key.endswith("}")
    assert len(key.split("{")[1][:-1]) == 64
    assert "192.0.2.10" not in key
    assert "internal-test-secret" not in key
    assert policy.tenant_id == UUID(int=0)


@pytest.mark.asyncio
async def test_pre_auth_redis_arguments_contain_only_hmac_key_and_numeric_policy() -> (
    None
):
    class Client:
        def __init__(self):
            self.script = None
            self.args = None

        async def script_load(self, script):
            self.script = script
            return "sha"

        async def evalsha(self, *args):
            self.args = args
            return [1, 0]

    client = Client()
    policy = pre_auth_policy("192.0.2.10", b"internal-test-secret")
    assert (
        await RedisTokenBucket(SimpleNamespace(client=client)).evaluate([policy])
    ).allowed
    assert client.args[0] == "sha"
    assert client.args[1] == 1
    assert client.args[2] == rate_limit_key(policy)
    assert client.args[3:] == (20, 60000)
    assert "192.0.2.10" not in str(client.args)
    assert "internal-test-secret" not in str(client.args)
    assert "Authorization" not in str(client.args)
    assert "prompt" not in str(client.args)


def test_lua_sources_are_static_key_argument_scripts_without_keyspace_scan() -> None:
    scripts = Path(__file__).parents[2] / "app" / "redis" / "scripts"
    names = {
        "token_bucket_v1.lua",
        "semaphore_acquire_v1.lua",
        "semaphore_renew_v1.lua",
        "semaphore_release_v1.lua",
        "circuit_v1.lua",
    }
    assert {path.name for path in scripts.glob("*.lua")} == names
    for name in names:
        source = (scripts / name).read_text(encoding="utf-8")
        assert "KEYS" in source and "ARGV" in source
        assert "SCAN" not in source and "KEYS(" not in source
        assert "AUTH" not in source and "credential" not in source.lower()
        assert "192.0.2.10" not in source


@pytest.mark.asyncio
async def test_semaphore_redis_values_are_only_lease_and_numeric_coordination() -> None:
    class Client:
        async def script_load(self, script):
            assert script == (
                Path(__file__).parents[2] / "app/redis/scripts/semaphore_acquire_v1.lua"
            ).read_text(encoding="utf-8")
            return "sha"

        async def evalsha(self, *args):
            self.args = args
            return 1

    client = Client()
    policy = ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.API_KEY, uuid4(), 2
    )
    result = await RedisConcurrencySemaphore(
        SimpleNamespace(client=client), 30000
    ).acquire([policy])
    assert result.acquired
    assert client.args[:3] == ("sha", 1, semaphore_key(policy))
    assert client.args[3] == 30000
    assert UUID(client.args[4]) == result.lease_id
    assert client.args[5] == 2
    assert len(client.args) == 6


@pytest.mark.asyncio
async def test_circuit_redis_values_are_only_uuid_and_numeric_coordination() -> None:
    class Client:
        async def script_load(self, script):
            assert script == (
                Path(__file__).parents[2] / "app/redis/scripts/circuit_v1.lua"
            ).read_text(encoding="utf-8")
            return "sha"

        async def evalsha(self, *args):
            self.args = args
            return ["ok", "OPEN", "denied"]

    client = Client()
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    config = CircuitConfig(5, 60000, 30000, 1, 1, 30000)
    result = await RedisCircuitStore(
        SimpleNamespace(client=client), config
    ).check_or_claim_eligibility(identity)
    assert not result.eligible
    assert client.args[:4] == (
        "sha",
        2,
        circuit_key(identity),
        f"{circuit_key(identity)}:failures",
    )
    assert client.args[4:8] == ("eligibility", "", "", 0)
    assert all(UUID(value).version == 7 for value in client.args[8:10])
    assert client.args[10:] == (5, 60000, 30000, 30000)
