import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete, event, insert, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from app.control_plane.config_reconciler import ConfigVersionRepository
from app.control_plane.config_subscriber import (
    InvalidationRegistry,
    RedisInvalidationSubscriber,
)
from app.core.config import get_settings
from app.core.errors import GatewayHttpError
from app.main import create_app
from app.normal_api.current_state import CurrentStateRepository, NormalApiCurrentState
from app.persistence.models import Base
from app.redis.namespace import M3_INVALIDATION_CHANNEL
from tests.integration.test_m311_multiprocess_acceptance import (
    _command,
    _start_worker,
    _stop_worker,
)


@asynccontextmanager
async def persisted_state():
    engine = create_async_engine(get_settings().postgres_dsn)
    tenant, service, route, credential = uuid4(), uuid4(), uuid4(), uuid4()
    tables = Base.metadata.tables
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                insert(tables["tenants"]).values(
                    id=tenant,
                    name=f"p1a-{tenant}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["normal_api_services"]).values(
                    id=service,
                    tenant_id=tenant,
                    slug="p1a",
                    display_name="P1A",
                    upstream_base_url="http://example.invalid",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["normal_api_routes"]).values(
                    id=route,
                    tenant_id=tenant,
                    service_id=service,
                    method="GET",
                    path_pattern="/old",
                    upstream_path_template="/inactive",
                    priority=0,
                    status="ACTIVE",
                    header_policy={"request_allowlist": ["accept"]},
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(tables["service_credentials"]).values(
                    id=credential,
                    tenant_id=tenant,
                    service_id=service,
                    auth_type="NONE",
                    status="ACTIVE",
                    created_at=now,
                )
            )
            await connection.execute(
                insert(tables["config_versions"]).values(
                    tenant_id=tenant, version=1, updated_at=now
                )
            )
        yield engine, tenant, service, route
    finally:
        async with engine.begin() as connection:
            for name in (
                "service_credentials",
                "normal_api_routes",
                "normal_api_services",
                "config_versions",
            ):
                await connection.execute(
                    delete(tables[name]).where(tables[name].c.tenant_id == tenant)
                )
            await connection.execute(
                delete(tables["tenants"]).where(tables["tenants"].c.id == tenant)
            )
        await engine.dispose()


async def change(engine, tenant, route, *, disabled=False):
    tables = Base.metadata.tables
    async with engine.begin() as connection:
        await connection.execute(
            update(tables["normal_api_routes"])
            .where(tables["normal_api_routes"].c.id == route)
            .values(path_pattern="/new", status="DISABLED" if disabled else "ACTIVE")
        )
        await connection.execute(
            update(tables["config_versions"])
            .where(tables["config_versions"].c.tenant_id == tenant)
            .values(version=tables["config_versions"].c.version + 1)
        )


@pytest.mark.asyncio
async def test_real_postgres_redis_projection_repair_and_lifespan():
    async with persisted_state() as (engine, tenant, _service, route):
        app = create_app()
        async with app.router.lifespan_context(app):
            consumer = app.state.normal_api_current_state
            registry = app.state.invalidation_registry
            await consumer.register(tenant)
            await app.state.config_reconciler.reconcile_once()
            old = await consumer.capture(tenant)
            assert old.projection.routes[0]["path_pattern"] == "/old"
            assert old.projection.credentials[0]["auth_type"] == "NONE"
            assert "secret_ciphertext" not in old.projection.credentials[0]
            ready, received = asyncio.Event(), asyncio.Event()
            subscriber = app.state.invalidation_subscriber
            original_record, original_observe = subscriber._record, registry.observe

            def record(outcome, resource_type=None):
                original_record(outcome, resource_type)
                if outcome == "connected":
                    ready.set()

            async def observe(event):
                result = await original_observe(event)
                if event.tenant_id == tenant:
                    received.set()
                return result

            subscriber._record = record
            registry.observe = observe
            if subscriber.ready:
                ready.set()
            await asyncio.wait_for(ready.wait(), 5)
            await change(engine, tenant, route)
            await app.state.redis.publish(
                M3_INVALIDATION_CHANNEL,
                json.dumps(
                    {
                        "tenant_id": str(tenant),
                        "resource_type": "ROUTE",
                        "resource_id": str(route),
                        "version": 2,
                    }
                ),
            )
            await asyncio.wait_for(received.wait(), 5)
            with pytest.raises(GatewayHttpError):
                await consumer.capture(tenant)
            await app.state.config_reconciler.reconcile_once()
            assert (await consumer.capture(tenant)).projection.routes[0][
                "path_pattern"
            ] == "/new"
            assert old.projection.routes[0]["path_pattern"] == "/old"
            # Missed event: authoritative version fencing is the repair path.
            await change(engine, tenant, route, disabled=True)
            await app.state.config_reconciler.reconcile_once()
            assert not (await consumer.capture(tenant)).projection.routes
        with pytest.raises(GatewayHttpError):
            await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_two_real_processes_initialize_and_repair_independently():
    workers = []
    async with persisted_state() as (engine, tenant, _service, route):
        try:
            for _ in range(2):
                workers.append(await _start_worker())
            assert workers[0][0].pid != workers[1][0].pid
            for _, port, _ in workers:
                await _command(port, cmd="normal_register", tenant=str(tenant))
                await _command(port, cmd="reconcile")
                before = await _command(port, cmd="normal_capture", tenant=str(tenant))
                assert before["version"] == 1 and before["paths"] == ["/old"]
            await change(engine, tenant, route)
            for _, port, _ in workers:
                await _command(port, cmd="reconcile")
                after = await _command(port, cmd="normal_capture", tenant=str(tenant))
                assert after["version"] == 2 and after["paths"] == ["/new"]
        finally:
            for process, _, _ in workers:
                await _stop_worker(process)


@pytest.mark.asyncio
async def test_f2_real_rotation_and_independent_tenant_projections():
    async with (
        persisted_state() as (engine, tenant_a, service_a, route_a),
        persisted_state() as (_other_engine, tenant_b, service_b, route_b),
    ):
        tables = Base.metadata.tables
        credentials = tables["service_credentials"]
        versions = tables["config_versions"]
        synthetic_ciphertext = b"F2 synthetic opaque ciphertext; never decrypted"
        async with engine.begin() as connection:
            for tenant in (tenant_a, tenant_b):
                await connection.execute(
                    update(credentials)
                    .where(credentials.c.tenant_id == tenant)
                    .values(
                        auth_type="STATIC_BEARER",
                        secret_ciphertext=synthetic_ciphertext,
                        key_version=1,
                    )
                )
            await connection.execute(
                update(tables["normal_api_services"])
                .where(tables["normal_api_services"].c.id == service_b)
                .values(slug="p1a-b", upstream_base_url="http://b.invalid")
            )
            await connection.execute(
                update(tables["normal_api_routes"])
                .where(tables["normal_api_routes"].c.id == route_b)
                .values(path_pattern="/tenant-b")
            )

        registry = InvalidationRegistry()
        consumer = NormalApiCurrentState(registry, CurrentStateRepository(engine).load)
        lookup = ConfigVersionRepository(engine).lookup
        redis = Redis.from_url(get_settings().redis_url)
        subscriber = RedisInvalidationSubscriber(redis, registry)
        ready, received = asyncio.Event(), asyncio.Event()
        original_record, original_observe = subscriber._record, registry.observe

        def record(outcome, resource_type=None):
            original_record(outcome, resource_type)
            if outcome == "connected":
                ready.set()

        async def observe(invalidation):
            result = await original_observe(invalidation)
            if invalidation.tenant_id == tenant_a:
                received.set()
            return result

        subscriber._record = record
        registry.observe = observe
        try:
            await subscriber.start()
            await asyncio.wait_for(ready.wait(), 5)
            for tenant in (tenant_a, tenant_b):
                await consumer.register(tenant)
                await registry.reconcile_fenced(tenant, lookup)
            old_a, old_b = (
                await consumer.capture(tenant_a),
                await consumer.capture(tenant_b),
            )
            async with engine.connect() as connection:
                active_ids = dict(
                    (
                        await connection.execute(
                            select(credentials.c.tenant_id, credentials.c.id).where(
                                credentials.c.tenant_id.in_((tenant_a, tenant_b)),
                                credentials.c.status == "ACTIVE",
                            )
                        )
                    ).all()
                )
            old_id = old_a.projection.credentials[0]["id"]
            assert old_id != old_b.projection.credentials[0]["id"]
            for snapshot, tenant, service, route in (
                (old_a, tenant_a, service_a, route_a),
                (old_b, tenant_b, service_b, route_b),
            ):
                assert snapshot.tenant_id == tenant
                assert [row["id"] for row in snapshot.projection.services] == [service]
                assert [row["id"] for row in snapshot.projection.routes] == [route]
                metadata = snapshot.projection.credentials[0]
                assert metadata["id"] == active_ids[tenant]
                assert (
                    metadata["tenant_id"] == tenant
                    and metadata["service_id"] == service
                )
                assert set(metadata) == {
                    "id",
                    "tenant_id",
                    "service_id",
                    "auth_type",
                    "header_name",
                    "key_version",
                }
                assert synthetic_ciphertext.decode() not in str(metadata)
                assert not any(isinstance(value, bytes) for value in metadata.values())
            replacement = uuid4()

            async def notify(version):
                received.clear()
                await redis.publish(
                    M3_INVALIDATION_CHANNEL,
                    json.dumps(
                        {
                            "tenant_id": str(tenant_a),
                            "resource_type": "SERVICE",
                            "resource_id": str(service_a),
                            "version": version,
                        }
                    ),
                )
                await asyncio.wait_for(received.wait(), 5)
                with pytest.raises(GatewayHttpError):
                    await consumer.capture(tenant_a)
                assert registry.is_tenant_reconciled(tenant_b)
                assert (await consumer.capture(tenant_b)).projection is old_b.projection
                await registry.reconcile_fenced(tenant_a, lookup)
                assert registry.is_tenant_reconciled(tenant_a)

            async with engine.begin() as connection:
                await connection.execute(
                    update(credentials)
                    .where(credentials.c.id == old_id)
                    .values(status="DISABLED")
                )
                await connection.execute(
                    insert(credentials).values(
                        id=replacement,
                        tenant_id=tenant_a,
                        service_id=service_a,
                        auth_type="STATIC_HEADER",
                        header_name="X-Synthetic-Upstream",
                        key_version=2,
                        secret_ciphertext=synthetic_ciphertext,
                        status="ACTIVE",
                        created_at=datetime.now(UTC),
                    )
                )
                await connection.execute(
                    update(versions)
                    .where(versions.c.tenant_id == tenant_a)
                    .values(version=2)
                )
            await notify(2)
            rotated = await consumer.capture(tenant_a)
            assert rotated.projection.credentials[0]["id"] == replacement
            assert (
                rotated.projection.credentials[0]["header_name"]
                == "X-Synthetic-Upstream"
            )
            assert rotated.projection.credentials[0]["key_version"] == 2
            assert old_a.projection.credentials[0]["id"] == old_id
            assert old_a.projection.credentials[0]["key_version"] == 1
            with pytest.raises(TypeError):
                old_a.projection.credentials[0]["id"] = replacement
            for version, action in ((3, "disable"), (4, "enable"), (5, "delete")):
                async with engine.begin() as connection:
                    if action == "delete":
                        await connection.execute(
                            delete(credentials).where(credentials.c.id == replacement)
                        )
                    else:
                        await connection.execute(
                            update(credentials)
                            .where(credentials.c.id == replacement)
                            .values(
                                status="ACTIVE" if action == "enable" else "DISABLED"
                            )
                        )
                    await connection.execute(
                        update(versions)
                        .where(versions.c.tenant_id == tenant_a)
                        .values(version=version)
                    )
                await notify(version)
                latest = await consumer.capture(tenant_a)
                if action == "enable":
                    assert latest.projection.credentials[0]["id"] == replacement
                else:
                    assert not latest.projection.credentials
            assert rotated.projection.credentials[0]["id"] == replacement
        finally:
            await subscriber.stop()
            await consumer.close()
            await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_table", ["normal_api_services", "normal_api_routes", "service_credentials"]
)
async def test_f2_real_repository_query_failure_preserves_snapshot_and_recovers(
    failed_table,
):
    async with persisted_state() as (engine, tenant, _service, route):
        registry = InvalidationRegistry()
        consumer = NormalApiCurrentState(registry, CurrentStateRepository(engine).load)
        lookup = ConfigVersionRepository(engine).lookup
        await consumer.register(tenant)
        await registry.reconcile_fenced(tenant, lookup)
        old = await consumer.capture(tenant)
        await change(engine, tenant, route)
        statements = []

        def fail_query(connection, cursor, statement, parameters, context, many):
            if statement.lstrip().startswith("SELECT"):
                statements.append(statement)
                if f"FROM {failed_table}" in statement:
                    raise RuntimeError("synthetic repository read failure")

        event.listen(engine.sync_engine, "before_cursor_execute", fail_query)
        try:
            with pytest.raises(RuntimeError, match="synthetic repository read failure"):
                await registry.reconcile_fenced(tenant, lookup)
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", fail_query)
        assert statements and not registry.is_tenant_reconciled(tenant)
        assert registry.observed_version(tenant) == 1
        assert consumer._published[tenant] is old.projection
        assert old.projection.routes[0]["path_pattern"] == "/old"
        with pytest.raises(GatewayHttpError):
            await consumer.capture(tenant)
        await registry.reconcile_fenced(tenant, lookup)
        current = await consumer.capture(tenant)
        assert current.observed_version == 2
        assert current.projection.routes[0]["path_pattern"] == "/new"
        # A connection remains usable after failure; loader context was released.
        async with engine.connect() as connection:
            assert await connection.scalar(select(1)) == 1
        await consumer.close()
