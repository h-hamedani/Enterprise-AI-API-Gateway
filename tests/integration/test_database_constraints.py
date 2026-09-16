from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.persistence.models import Base


@pytest.fixture
def db_connection():
    settings = get_settings()

    if settings.environment == "production":
        pytest.fail("Integration database tests must never run in production.")

    engine = create_engine(settings.postgres_migration_dsn)

    with engine.connect() as connection:
        transaction = connection.begin()

        try:
            yield connection
        finally:
            transaction.rollback()

    engine.dispose()


def now() -> datetime:
    return datetime.now(UTC)


def tenant_values(*, tenant_id, name: str) -> dict:
    timestamp = now()

    return {
        "id": tenant_id,
        "name": name,
        "status": "ACTIVE",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def admin_user_values(
    *,
    admin_user_id,
    tenant_id,
    name: str,
) -> dict:
    timestamp = now()

    return {
        "id": admin_user_id,
        "tenant_id": tenant_id,
        "name": name,
        "status": "ACTIVE",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def application_values(
    *,
    application_id,
    tenant_id,
    name: str,
) -> dict:
    timestamp = now()

    return {
        "id": application_id,
        "tenant_id": tenant_id,
        "name": name,
        "status": "ACTIVE",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def test_duplicate_tenant_name_is_rejected(db_connection):
    tenants = Base.metadata.tables["tenants"]

    tenant_1 = uuid4()
    tenant_2 = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_1,
                name="tenant-duplicate-test",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(tenants).values(
                **tenant_values(
                    tenant_id=tenant_2,
                    name="tenant-duplicate-test",
                )
            )
        )


def test_duplicate_admin_user_name_in_same_tenant_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    admin_users = Base.metadata.tables["admin_users"]

    tenant_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="tenant-admin-name-test",
            )
        )
    )

    db_connection.execute(
        insert(admin_users).values(
            **admin_user_values(
                admin_user_id=uuid4(),
                tenant_id=tenant_id,
                name="admin-user",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(admin_users).values(
                **admin_user_values(
                    admin_user_id=uuid4(),
                    tenant_id=tenant_id,
                    name="admin-user",
                )
            )
        )


def test_admin_token_cross_tenant_reference_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    admin_users = Base.metadata.tables["admin_users"]
    admin_tokens = Base.metadata.tables["admin_tokens"]

    tenant_a = uuid4()
    tenant_b = uuid4()
    admin_user_id = uuid4()

    db_connection.execute(
        insert(tenants),
        [
            tenant_values(
                tenant_id=tenant_a,
                name="tenant-admin-token-a",
            ),
            tenant_values(
                tenant_id=tenant_b,
                name="tenant-admin-token-b",
            ),
        ],
    )

    db_connection.execute(
        insert(admin_users).values(
            **admin_user_values(
                admin_user_id=admin_user_id,
                tenant_id=tenant_a,
                name="admin-a",
            )
        )
    )

    timestamp = now()

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(admin_tokens).values(
                id=uuid4(),
                tenant_id=tenant_b,
                admin_user_id=admin_user_id,
                token_prefix="adm_cross",
                token_hash=f"test-admin-token-{uuid4()}",
                status="ACTIVE",
                revoked_at=None,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )


def test_api_key_cross_tenant_reference_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    applications = Base.metadata.tables["applications"]
    api_keys = Base.metadata.tables["api_keys"]

    tenant_a = uuid4()
    tenant_b = uuid4()
    application_id = uuid4()

    db_connection.execute(
        insert(tenants),
        [
            tenant_values(
                tenant_id=tenant_a,
                name="tenant-api-key-a",
            ),
            tenant_values(
                tenant_id=tenant_b,
                name="tenant-api-key-b",
            ),
        ],
    )

    db_connection.execute(
        insert(applications).values(
            **application_values(
                application_id=application_id,
                tenant_id=tenant_a,
                name="application-a",
            )
        )
    )

    timestamp = now()

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(api_keys).values(
                id=uuid4(),
                tenant_id=tenant_b,
                application_id=application_id,
                name="cross-tenant-key",
                key_prefix="gw_cross",
                key_hash=f"test-cross-tenant-{uuid4()}",
                status="ACTIVE",
                revoked_at=None,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )


def test_duplicate_api_key_hash_is_rejected(db_connection):
    tenants = Base.metadata.tables["tenants"]
    applications = Base.metadata.tables["applications"]
    api_keys = Base.metadata.tables["api_keys"]

    tenant_id = uuid4()
    application_id = uuid4()
    key_hash = f"duplicate-key-hash-{uuid4()}"

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="tenant-key-hash-test",
            )
        )
    )

    db_connection.execute(
        insert(applications).values(
            **application_values(
                application_id=application_id,
                tenant_id=tenant_id,
                name="application-key-hash-test",
            )
        )
    )

    timestamp = now()

    db_connection.execute(
        insert(api_keys).values(
            id=uuid4(),
            tenant_id=tenant_id,
            application_id=application_id,
            name="duplicate-key-a",
            key_prefix="gw_duplicate_a",
            key_hash=key_hash,
            status="ACTIVE",
            revoked_at=None,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(api_keys).values(
                id=uuid4(),
                tenant_id=tenant_id,
                application_id=application_id,
                name="duplicate-key-b",
                key_prefix="gw_duplicate_b",
                key_hash=key_hash,
                status="ACTIVE",
                revoked_at=None,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )


def test_duplicate_idempotency_scope_key_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    admin_users = Base.metadata.tables["admin_users"]
    idempotency_records = Base.metadata.tables["idempotency_records"]

    tenant_id = uuid4()
    admin_user_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="tenant-idempotency-duplicate",
            )
        )
    )

    db_connection.execute(
        insert(admin_users).values(
            **admin_user_values(
                admin_user_id=admin_user_id,
                tenant_id=tenant_id,
                name="idempotency-admin",
            )
        )
    )

    timestamp = now()

    record = {
        "tenant_id": tenant_id,
        "admin_user_id": admin_user_id,
        "endpoint_key": "admin.api_keys.create",
        "idempotency_key": "idem-duplicate-test",
        "request_fingerprint": "a" * 64,
        "state": "IN_PROGRESS",
        "response_status": None,
        "response_body_ciphertext": None,
        "response_metadata": None,
        "expires_at": timestamp + timedelta(hours=24),
        "completed_at": None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    db_connection.execute(
        insert(idempotency_records).values(
            id=uuid4(),
            **record,
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(idempotency_records).values(
                id=uuid4(),
                **record,
            )
        )


def test_same_idempotency_key_for_different_admin_users_is_allowed(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    admin_users = Base.metadata.tables["admin_users"]
    idempotency_records = Base.metadata.tables["idempotency_records"]

    tenant_id = uuid4()
    admin_user_a = uuid4()
    admin_user_b = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="tenant-idempotency-multi-user",
            )
        )
    )

    db_connection.execute(
        insert(admin_users),
        [
            admin_user_values(
                admin_user_id=admin_user_a,
                tenant_id=tenant_id,
                name="admin-a",
            ),
            admin_user_values(
                admin_user_id=admin_user_b,
                tenant_id=tenant_id,
                name="admin-b",
            ),
        ],
    )

    timestamp = now()

    common_values = {
        "tenant_id": tenant_id,
        "endpoint_key": "admin.api_keys.create",
        "idempotency_key": "same-key-different-admin",
        "state": "IN_PROGRESS",
        "response_status": None,
        "response_body_ciphertext": None,
        "response_metadata": None,
        "expires_at": timestamp + timedelta(hours=24),
        "completed_at": None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    db_connection.execute(
        insert(idempotency_records).values(
            id=uuid4(),
            admin_user_id=admin_user_a,
            request_fingerprint="a" * 64,
            **common_values,
        )
    )

    db_connection.execute(
        insert(idempotency_records).values(
            id=uuid4(),
            admin_user_id=admin_user_b,
            request_fingerprint="b" * 64,
            **common_values,
        )
    )
