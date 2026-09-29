from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings
from app.persistence.models import Base
from app.redis.circuit import (
    CircuitConfig,
    CircuitIdentity,
    RedisCircuitStore,
    circuit_key,
)
from app.redis.circuit_cleanup import (
    CircuitAuthority,
    CircuitAuthorityRepository,
    CircuitOrphanSweeper,
    circuit_failure_key,
)
from app.redis.runtime import RedisRuntime


@pytest.fixture(scope="module")
def event_loop_policy():
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest.mark.asyncio
async def test_real_redis_sweep_retains_active_and_reclaims_orphan_pair():
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    await runtime.start()
    active = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    retired = CircuitIdentity._create(uuid4(), "provider_target", uuid4(), uuid4())
    active_key, retired_key = circuit_key(active), circuit_key(retired)
    client = runtime.client
    try:
        circuit = RedisCircuitStore(runtime, CircuitConfig.from_settings(settings))
        admitted = await circuit.check_or_claim_eligibility(active)
        await circuit.record_failure(active, admitted.normal_token)
        await client.hset(retired_key, mapping={"state": "CLOSED"})
        await client.zadd(circuit_failure_key(retired), {"old": 1})
        await client.set("gw:v1:unrelated-test:" + str(active.tenant_id), "keep")

        async def authority(identity: CircuitIdentity) -> CircuitAuthority:
            return (
                CircuitAuthority.RETIRED
                if identity == retired
                else CircuitAuthority.ACTIVE
            )

        sweep = CircuitOrphanSweeper(client, authority)
        for _ in range(8):
            await sweep.reconcile_once()
            if not await client.exists(retired_key, circuit_failure_key(retired)):
                break
        assert await client.exists(active_key, circuit_failure_key(active)) == 2
        assert await client.pttl(active_key) == -1
        assert await client.pttl(circuit_failure_key(active)) == -1
        assert await client.exists(retired_key, circuit_failure_key(retired)) == 0
        assert (
            await client.get("gw:v1:unrelated-test:" + str(active.tenant_id)) == "keep"
        )
        await sweep.reconcile_once()
        assert await client.exists(retired_key, circuit_failure_key(retired)) == 0
    finally:
        await client.delete(
            active_key,
            circuit_failure_key(active),
            retired_key,
            circuit_failure_key(retired),
            "gw:v1:unrelated-test:" + str(active.tenant_id),
        )
        await runtime.close()


@pytest.mark.asyncio
async def test_active_open_and_half_open_state_survive_cleanup_without_ttl():
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url, max(5.0, settings.dependency_timeout_seconds)
    )
    await runtime.start()
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    primary = circuit_key(identity)
    client = runtime.client
    try:
        config = CircuitConfig(
            failure_threshold=1,
            failure_window_ms=60000,
            open_duration_ms=30000,
            half_open_probe_limit=1,
            successes_to_close=1,
            probe_lease_duration_ms=30000,
        )
        circuit = RedisCircuitStore(runtime, config)
        admitted = await circuit.check_or_claim_eligibility(identity)
        await circuit.record_failure(identity, admitted.normal_token)

        async def active(_identity: CircuitIdentity) -> CircuitAuthority:
            return CircuitAuthority.ACTIVE

        sweep = CircuitOrphanSweeper(client, active)
        await sweep.reconcile_once()
        assert await client.hget(primary, "state") == "OPEN"
        assert await client.pttl(primary) == -1
        seconds, micros = await client.time()
        await client.hset(primary, "open_until_ms", seconds * 1000 + micros // 1000 - 1)
        probe = await circuit.check_or_claim_eligibility(identity)
        assert probe.probe_id is not None
        await sweep.reconcile_once()
        assert await client.hget(primary, "state") == "HALF_OPEN"
        assert await client.pttl(primary) == -1
    finally:
        await client.delete(primary, circuit_failure_key(identity))
        await runtime.close()


@pytest.mark.asyncio
async def test_postgresql_authority_uses_relationship_not_status():
    settings = get_settings()
    engine = create_async_engine(settings.postgres_dsn)
    tables = Base.metadata.tables
    tenant, service, route, provider, target, model = (uuid4() for _ in range(6))
    normal = CircuitIdentity._create(tenant, "route", route, service)
    llm = CircuitIdentity._create(tenant, "provider_target", target, model)
    now = datetime.now(UTC)
    repository = CircuitAuthorityRepository(engine)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                insert(tables["tenants"]).values(
                    id=tenant,
                    name=f"circuit-orphan-{tenant}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["normal_api_services"]).values(
                    id=service,
                    tenant_id=tenant,
                    slug=f"s-{service.hex[:20]}",
                    display_name="orphan test",
                    upstream_base_url="https://example.invalid",
                    status="DISABLED",
                    request_body_limit_bytes=1024,
                    connect_timeout_seconds=5,
                    pool_timeout_seconds=5,
                    write_timeout_seconds=5,
                    read_idle_timeout_seconds=5,
                    pre_response_timeout_seconds=5,
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["normal_api_routes"]).values(
                    id=route,
                    tenant_id=tenant,
                    service_id=service,
                    path_pattern="/test",
                    method="GET",
                    upstream_path_template="/test",
                    priority=1,
                    status="DISABLED",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["llm_providers"]).values(
                    id=provider,
                    tenant_id=tenant,
                    name=f"p-{provider.hex}",
                    provider_type="OPENAI",
                    status="DISABLED",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["llm_provider_targets"]).values(
                    id=target,
                    tenant_id=tenant,
                    provider_id=provider,
                    name="target",
                    base_url="https://example.invalid",
                    timeout_ms=30000,
                    pre_output_idle_timeout_ms=20000,
                    pre_output_budget_ms=30000,
                    post_output_idle_timeout_ms=60000,
                    status="DISABLED",
                    certification_status="UNVERIFIED",
                    allow_uncertified_runtime=False,
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["llm_models"]).values(
                    id=model,
                    tenant_id=tenant,
                    provider_target_id=target,
                    provider_model_name="orphan-test",
                    status="DISABLED",
                    created_at=now,
                    updated_at=now,
                )
            )
        assert await repository.lookup(normal) is CircuitAuthority.ACTIVE
        assert await repository.lookup(llm) is CircuitAuthority.ACTIVE
        wrong_service = CircuitIdentity._create(tenant, "route", route, uuid4())
        wrong_target = CircuitIdentity._create(
            tenant, "provider_target", uuid4(), model
        )
        assert await repository.lookup(wrong_service) is CircuitAuthority.RETIRED
        assert await repository.lookup(wrong_target) is CircuitAuthority.RETIRED
        async with engine.begin() as connection:
            await connection.execute(
                delete(tables["normal_api_routes"]).where(
                    tables["normal_api_routes"].c.id == route
                )
            )
            await connection.execute(
                delete(tables["llm_models"]).where(tables["llm_models"].c.id == model)
            )
        assert await repository.lookup(normal) is CircuitAuthority.RETIRED
        assert await repository.lookup(llm) is CircuitAuthority.RETIRED
    finally:
        async with engine.begin() as connection:
            for name, resource_id in (
                ("normal_api_routes", route),
                ("llm_models", model),
                ("normal_api_services", service),
                ("llm_provider_targets", target),
                ("llm_providers", provider),
                ("tenants", tenant),
            ):
                table = tables[name]
                await connection.execute(delete(table).where(table.c.id == resource_id))
        await engine.dispose()
