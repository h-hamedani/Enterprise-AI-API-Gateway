from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.exc import IntegrityError

from app.control_plane.application_api_keys import (
    ResourceNotFoundError,
    create_control_plane_admin_services,
)
from app.core.config import get_settings
from app.core.idempotency import IdempotencyConflictError, IdempotencyInProgressError
from app.core.security.credentials import CredentialAuthenticator, CredentialHasher
from app.persistence.models import Base
from app.schemas.control_plane import ApiKeyCreate, ApplicationCreate, ApplicationPatch


@pytest.fixture
def admin_fixture():
    engine = create_engine(get_settings().postgres_migration_dsn)
    timestamp = datetime.now(UTC)
    tenant_a, tenant_b = uuid4(), uuid4()
    admin_a, admin_b = uuid4(), uuid4()
    foreign_application_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]),
            [
                {
                    "id": tenant_a,
                    "name": f"m24-tenant-{tenant_a}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": tenant_b,
                    "name": f"m24-tenant-{tenant_b}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["admin_users"]),
            [
                {
                    "id": admin_a,
                    "tenant_id": tenant_a,
                    "name": "m24-admin-a",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": admin_b,
                    "tenant_id": tenant_b,
                    "name": "m24-admin-b",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["applications"]).values(
                id=foreign_application_id,
                tenant_id=tenant_b,
                name="foreign-application",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    services = create_control_plane_admin_services(
        credential_hmac_key=b"c" * 32,
        idempotency_hmac_key=b"i" * 32,
        encryption_keys={1: b"e" * 32},
        current_encryption_key_version=1,
    )
    yield {
        "engine": engine,
        "services": services,
        "tenant_a": tenant_a,
        "tenant_b": tenant_b,
        "admin_a": admin_a,
        "admin_b": admin_b,
        "foreign_application_id": foreign_application_id,
    }
    with engine.begin() as connection:
        for table_name in (
            "idempotency_records",
            "api_keys",
            "applications",
            "admin_users",
        ):
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


def _create_application(fixture, name: str = "primary") -> UUID:
    with fixture["engine"].begin() as connection:
        result = fixture["services"].applications.create(
            connection,
            tenant_id=fixture["tenant_a"],
            admin_user_id=fixture["admin_a"],
            request=ApplicationCreate(name=name),
            raw_idempotency_key=None,
        )
    return UUID(json.loads(result.response.body)["id"])


def test_application_crud_is_tenant_scoped_and_duplicate_is_constrained(admin_fixture):
    application_id = _create_application(admin_fixture)
    with admin_fixture["engine"].begin() as connection:
        application = admin_fixture["services"].applications.get(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            application_id=application_id,
        )
        updated = admin_fixture["services"].applications.update(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            application_id=application_id,
            patch=ApplicationPatch(name="renamed", status="DISABLED"),
        )
        with pytest.raises(ResourceNotFoundError):
            admin_fixture["services"].applications.get(
                connection,
                tenant_id=admin_fixture["tenant_b"],
                application_id=application_id,
            )
        with pytest.raises(ResourceNotFoundError):
            admin_fixture["services"].applications.get(
                connection,
                tenant_id=admin_fixture["tenant_a"],
                application_id=uuid4(),
            )
    assert application.status == "ACTIVE"
    assert updated.name == "renamed"
    assert updated.status == "DISABLED"

    with pytest.raises(IntegrityError), admin_fixture["engine"].begin() as connection:
        admin_fixture["services"].applications.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=ApplicationCreate(name="renamed"),
            raw_idempotency_key=None,
        )


def test_application_list_paginates_without_cross_tenant_leak(admin_fixture):
    for name in ("page-a", "page-b", "page-c"):
        _create_application(admin_fixture, name)
    with admin_fixture["engine"].connect() as connection:
        first = admin_fixture["services"].applications.list(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            limit=2,
            cursor=None,
        )
        second = admin_fixture["services"].applications.list(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            limit=2,
            cursor=first.next_cursor,
        )
        with pytest.raises(ValueError, match="Invalid pagination cursor"):
            admin_fixture["services"].applications.list(
                connection,
                tenant_id=admin_fixture["tenant_b"],
                limit=2,
                cursor=first.next_cursor,
            )
    assert len(first.data) == 2
    assert len(second.data) == 1
    assert {item.id for item in first.data}.isdisjoint(item.id for item in second.data)
    assert all(item.name != "foreign-application" for item in first.data + second.data)


def test_api_key_creation_persists_only_verifier_and_replays_exact_secret(
    admin_fixture,
):
    application_id = _create_application(admin_fixture)
    request = ApiKeyCreate(application_id=application_id, name="production")
    with admin_fixture["engine"].begin() as connection:
        created = admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=request,
            raw_idempotency_key="m24-create-key-value",
        )
    with admin_fixture["engine"].connect() as connection:
        replay = admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=request,
            raw_idempotency_key="m24-create-key-value",
        )
        rows = (
            connection.execute(
                select(Base.metadata.tables["api_keys"]).where(
                    Base.metadata.tables["api_keys"].c.tenant_id
                    == admin_fixture["tenant_a"]
                )
            )
            .mappings()
            .all()
        )
        safe_page = admin_fixture["services"].api_keys.list(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            limit=50,
            cursor=None,
        )

    body = json.loads(created.response.body)
    assert created.response == replay.response
    assert replay.replayed
    assert len(rows) == 1
    assert body["key"].startswith("gw_")
    assert rows[0]["key_prefix"] == body["key_prefix"] == body["key"][:12]
    assert body["key"] not in str(rows[0])
    assert CredentialHasher(b"c" * 32).verify(body["key"], rows[0]["key_hash"])
    assert "key" not in safe_page.data[0].model_dump()
    assert "key_hash" not in safe_page.data[0].model_dump()


def test_api_key_foreign_application_and_fingerprint_conflict(admin_fixture):
    foreign = ApiKeyCreate(
        application_id=admin_fixture["foreign_application_id"], name="denied"
    )
    with (
        admin_fixture["engine"].begin() as connection,
        pytest.raises(ResourceNotFoundError),
    ):
        admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=foreign,
            raw_idempotency_key="foreign-app-key-value",
        )

    application_id = _create_application(admin_fixture)
    with admin_fixture["engine"].begin() as connection:
        admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=ApiKeyCreate(application_id=application_id, name="first"),
            raw_idempotency_key="fingerprint-key-value",
        )
    with (
        admin_fixture["engine"].begin() as connection,
        pytest.raises(IdempotencyConflictError),
    ):
        admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=ApiKeyCreate(application_id=application_id, name="different"),
            raw_idempotency_key="fingerprint-key-value",
        )


def test_api_key_list_is_safe_paginated_and_tenant_scoped(admin_fixture):
    application_id = _create_application(admin_fixture)
    with admin_fixture["engine"].begin() as connection:
        for number in range(3):
            admin_fixture["services"].api_keys.create(
                connection,
                tenant_id=admin_fixture["tenant_a"],
                admin_user_id=admin_fixture["admin_a"],
                request=ApiKeyCreate(
                    application_id=application_id, name=f"tenant-a-{number}"
                ),
                raw_idempotency_key=f"tenant-a-list-key-{number}",
            )
        admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_b"],
            admin_user_id=admin_fixture["admin_b"],
            request=ApiKeyCreate(
                application_id=admin_fixture["foreign_application_id"],
                name="tenant-b-hidden",
            ),
            raw_idempotency_key="tenant-b-list-key-value",
        )
    with admin_fixture["engine"].connect() as connection:
        first = admin_fixture["services"].api_keys.list(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            limit=2,
            cursor=None,
        )
        second = admin_fixture["services"].api_keys.list(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            limit=2,
            cursor=first.next_cursor,
        )
    keys = first.data + second.data
    assert len(keys) == 3
    assert all(item.name.startswith("tenant-a-") for item in keys)
    assert all(
        set(item.model_dump()) == {"id", "name", "key_prefix", "status", "expires_at"}
        for item in keys
    )


def test_api_key_concurrent_duplicate_does_not_create_second_row(admin_fixture):
    application_id = _create_application(admin_fixture)
    request = ApiKeyCreate(application_id=application_id, name="concurrent")
    first_connection = admin_fixture["engine"].connect()
    first_transaction = first_connection.begin()
    try:
        admin_fixture["services"].api_keys.create(
            first_connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=request,
            raw_idempotency_key="concurrent-api-key-value",
        )
        with (
            admin_fixture["engine"].begin() as second_connection,
            pytest.raises(IdempotencyInProgressError),
        ):
            admin_fixture["services"].api_keys.create(
                second_connection,
                tenant_id=admin_fixture["tenant_a"],
                admin_user_id=admin_fixture["admin_a"],
                request=request,
                raw_idempotency_key="concurrent-api-key-value",
            )
        first_transaction.commit()
    finally:
        if first_transaction.is_active:
            first_transaction.rollback()
        first_connection.close()
    with admin_fixture["engine"].connect() as connection:
        count = connection.scalar(
            select(func.count()).select_from(Base.metadata.tables["api_keys"])
        )
    assert count == 1


def test_api_key_creation_rollback_removes_key_and_claim(admin_fixture):
    application_id = _create_application(admin_fixture)
    connection = admin_fixture["engine"].connect()
    transaction = connection.begin()
    try:
        admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=ApiKeyCreate(application_id=application_id, name="rollback"),
            raw_idempotency_key="rollback-api-key-value",
        )
        transaction.rollback()
    finally:
        if transaction.is_active:
            transaction.rollback()
        connection.close()
    with admin_fixture["engine"].connect() as check:
        assert (
            check.scalar(
                select(func.count()).select_from(Base.metadata.tables["api_keys"])
            )
            == 0
        )
        assert (
            check.scalar(
                select(func.count()).select_from(
                    Base.metadata.tables["idempotency_records"]
                )
            )
            == 0
        )


def test_revoke_is_irreversible_consistent_and_disables_authentication(admin_fixture):
    application_id = _create_application(admin_fixture)
    with admin_fixture["engine"].begin() as connection:
        created = admin_fixture["services"].api_keys.create(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            admin_user_id=admin_fixture["admin_a"],
            request=ApiKeyCreate(application_id=application_id, name="revoke"),
            raw_idempotency_key="revoke-api-key-value",
        )
    body = json.loads(created.response.body)
    api_key_id = UUID(body["id"])
    with admin_fixture["engine"].begin() as connection:
        revoked = admin_fixture["services"].api_keys.revoke(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            api_key_id=api_key_id,
        )
        repeated = admin_fixture["services"].api_keys.revoke(
            connection,
            tenant_id=admin_fixture["tenant_a"],
            api_key_id=api_key_id,
        )
        row = (
            connection.execute(
                select(Base.metadata.tables["api_keys"]).where(
                    Base.metadata.tables["api_keys"].c.id == api_key_id
                )
            )
            .mappings()
            .one()
        )
        with pytest.raises(ResourceNotFoundError):
            admin_fixture["services"].api_keys.revoke(
                connection,
                tenant_id=admin_fixture["tenant_b"],
                api_key_id=api_key_id,
            )
    assert revoked.status == repeated.status == "REVOKED"
    assert row["revoked_at"] is not None
    assert not CredentialAuthenticator(CredentialHasher(b"c" * 32)).authenticate(
        body["key"],
        verifier=row["key_hash"],
        status=row["status"],
        expires_at=row["expires_at"],
    )
