from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert

from app.control_plane.admin_reads import create_admin_read_service
from app.core.config import get_settings
from app.persistence.models import Base


@pytest.fixture
def read_fixture():
    engine = create_engine(get_settings().postgres_migration_dsn)
    tenant_a, tenant_b = uuid4(), uuid4()
    timestamp = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]),
            [
                {
                    "id": tenant_a,
                    "name": f"m211-a-{tenant_a}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": tenant_b,
                    "name": f"m211-b-{tenant_b}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            ],
        )
    yield engine, tenant_a, tenant_b
    with engine.begin() as connection:
        for table_name in ("config_versions", "audit_logs"):
            table = Base.metadata.tables[table_name]
            connection.execute(
                table.delete().where(table.c.tenant_id.in_([tenant_a, tenant_b]))
            )
        connection.execute(
            Base.metadata.tables["tenants"]
            .delete()
            .where(Base.metadata.tables["tenants"].c.id.in_([tenant_a, tenant_b]))
        )
    engine.dispose()


def _audit(tenant_id, created_at, resource_id):
    return {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "action": "SERVICE_UPDATE",
        "resource_type": "SERVICE",
        "resource_id": resource_id,
        "result": "SUCCESS",
        "created_at": created_at,
    }


def test_audit_pagination_is_stable_tenant_bound_and_tamper_safe(read_fixture):
    engine, tenant_a, tenant_b = read_fixture
    service = create_admin_read_service(b"p" * 32)
    tied = datetime.now(UTC) - timedelta(minutes=1)
    newest = tied + timedelta(seconds=1)
    resources = [uuid4() for _ in range(5)]
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["audit_logs"]),
            [
                _audit(tenant_a, newest, resources[0]),
                _audit(tenant_a, tied, resources[1]),
                _audit(tenant_a, tied, resources[2]),
                _audit(tenant_a, tied - timedelta(seconds=1), resources[3]),
                _audit(tenant_b, newest, resources[4]),
            ],
        )

    with engine.connect() as connection:
        first = service.list_audit_logs(
            connection, tenant_id=tenant_a, limit=2, cursor=None
        )
        second = service.list_audit_logs(
            connection, tenant_id=tenant_a, limit=2, cursor=first.next_cursor
        )

        with pytest.raises(ValueError, match="Invalid pagination cursor"):
            service.list_audit_logs(
                connection,
                tenant_id=tenant_b,
                limit=2,
                cursor=first.next_cursor,
            )
        with pytest.raises(ValueError, match="Invalid pagination cursor"):
            service.list_audit_logs(
                connection,
                tenant_id=tenant_a,
                limit=2,
                cursor=f"{first.next_cursor}x",
            )

    ids = [item.id for item in first.data + second.data]
    assert len(ids) == 4
    assert len(set(ids)) == 4
    assert first.data[0].resource_id == resources[0]
    tied_ids = sorted(
        [
            item.id
            for item in first.data + second.data
            if item.resource_id in resources[1:3]
        ],
        reverse=True,
    )
    traversed_tied_ids = [
        item.id
        for item in first.data + second.data
        if item.resource_id in resources[1:3]
    ]
    assert traversed_tied_ids == tied_ids


def test_config_version_is_tenant_scoped_and_defaults_to_zero(read_fixture):
    engine, tenant_a, tenant_b = read_fixture
    service = create_admin_read_service(b"v" * 32)
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["config_versions"]).values(
                tenant_id=tenant_a,
                version=17,
                updated_at=datetime.now(UTC),
            )
        )
    with engine.connect() as connection:
        assert (
            service.get_config_version(connection, tenant_id=tenant_a).config_version
            == 17
        )
        assert (
            service.get_config_version(connection, tenant_id=tenant_b).config_version
            == 0
        )
