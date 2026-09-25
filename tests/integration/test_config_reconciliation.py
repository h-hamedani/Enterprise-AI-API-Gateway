from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, event, insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.control_plane.config_reconciler import ConfigVersionRepository
from app.core.config import get_settings
from app.main import create_app
from app.persistence.models import Base


@pytest.mark.asyncio
async def test_real_postgres_set_lookup_and_missed_event_repair():
    engine = create_async_engine(get_settings().postgres_dsn)
    tenants = Base.metadata.tables["tenants"]
    versions = Base.metadata.tables["config_versions"]
    tenant_a, tenant_missing, unrelated = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            for tenant in (tenant_a, tenant_missing, unrelated):
                await connection.execute(
                    insert(tenants).values(
                        id=tenant,
                        name=f"m37-{tenant}",
                        status="ACTIVE",
                        created_at=now,
                        updated_at=now,
                    )
                )
            for tenant, version in ((tenant_a, 14), (unrelated, 99)):
                await connection.execute(
                    insert(versions).values(
                        tenant_id=tenant, version=version, updated_at=now
                    )
                )

        repository = ConfigVersionRepository(engine)
        selects = []

        def count_version_select(
            connection, cursor, statement, parameters, context, many
        ):
            if "config_versions" in statement and statement.lstrip().startswith(
                "SELECT"
            ):
                selects.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", count_version_select)
        result = await repository.lookup(frozenset({tenant_a, tenant_missing}))
        event.remove(engine.sync_engine, "before_cursor_execute", count_version_select)
        assert result == {
            tenant_a: 14,
            tenant_missing: 0,
        }
        assert len(selects) == 1

        app = create_app()
        async with app.router.lifespan_context(app):
            registry = app.state.invalidation_registry
            reconciler = app.state.config_reconciler
            assert registry.tenant_ids() == frozenset()
            assert reconciler.running
            assert await reconciler.reconcile_once() == "empty_membership"
            observed = []
            await registry.register_tenant_callback(tenant_a, observed.append)
            assert await reconciler.reconcile_once() == "pass_success"
            assert registry.version(tenant_a) == 14
            observed.clear()
            async with engine.begin() as connection:
                await connection.execute(
                    versions.update()
                    .where(versions.c.tenant_id == tenant_a)
                    .values(version=18, updated_at=datetime.now(UTC))
                )
            assert await reconciler.reconcile_once() == "pass_success"
            assert observed == [18]
            assert registry.version(tenant_a) == 18
            assert await reconciler.reconcile_once() == "pass_success"
            assert observed == [18]
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(versions.c.version).where(
                            versions.c.tenant_id == tenant_a
                        )
                    )
                    == 18
                )
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                delete(versions).where(
                    versions.c.tenant_id.in_((tenant_a, tenant_missing, unrelated))
                )
            )
            await connection.execute(
                delete(tenants).where(
                    tenants.c.id.in_((tenant_a, tenant_missing, unrelated))
                )
            )
        await engine.dispose()
