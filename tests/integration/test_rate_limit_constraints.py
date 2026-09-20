from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert, inspect
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.control_plane.permissions import PermissionValidationError, RateScopeValidator
from app.core.config import get_settings
from app.persistence.models import Base
from app.persistence.models.operations import RateLimitPolicy


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


def add_tenant(connection, name):
    tenant_id, timestamp = uuid4(), datetime.now(UTC)
    connection.execute(
        insert(Base.metadata.tables["tenants"]).values(
            id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return tenant_id


def policy_values(tenant_id, **overrides):
    timestamp = datetime.now(UTC)
    values = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "scope_type": "API_KEY",
        "scope_id": uuid4(),
        "requests_per_window": 100,
        "window_seconds": 60,
        "max_concurrency": None,
        "degraded_factor": Decimal("0.25"),
        "enabled": True,
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize(
    "scope_type",
    [
        "API_KEY",
        "ROUTE",
        "SERVICE",
        "LLM_ALIAS",
        "LLM_MODEL",
        "PROVIDER_TARGET",
        "ADMIN_TOKEN",
    ],
)
def test_all_rate_scope_types_are_accepted(db_connection, scope_type):
    tenant_id = add_tenant(db_connection, f"rate-{scope_type.lower()}")
    db_connection.execute(
        insert(Base.metadata.tables["rate_limit_policies"]).values(
            **policy_values(tenant_id, scope_type=scope_type)
        )
    )


def test_invalid_rate_scope_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "rate-invalid-enum")
    with pytest.raises(DBAPIError):
        db_connection.execute(
            insert(Base.metadata.tables["rate_limit_policies"]).values(
                **policy_values(tenant_id, scope_type="UNKNOWN")
            )
        )


def test_only_one_enabled_policy_per_scope(db_connection):
    tenant_id = add_tenant(db_connection, "rate-enabled-unique")
    table = Base.metadata.tables["rate_limit_policies"]
    scope_id = uuid4()
    db_connection.execute(
        insert(table).values(**policy_values(tenant_id, scope_id=scope_id))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(table).values(**policy_values(tenant_id, scope_id=scope_id))
        )


def test_disabled_policy_history_is_allowed(db_connection):
    tenant_id = add_tenant(db_connection, "rate-disabled-history")
    table = Base.metadata.tables["rate_limit_policies"]
    scope_id = uuid4()
    for _ in range(3):
        db_connection.execute(
            insert(table).values(
                **policy_values(tenant_id, scope_id=scope_id, enabled=False)
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"requests_per_window": 0},
        {"window_seconds": 0},
        {"max_concurrency": 0},
        {"requests_per_window": 1, "window_seconds": None},
        {"requests_per_window": None, "window_seconds": 1},
        {
            "requests_per_window": None,
            "window_seconds": None,
            "max_concurrency": None,
        },
        {"degraded_factor": Decimal(0)},
        {"degraded_factor": Decimal("1.0001")},
    ],
)
def test_invalid_policy_dimensions_are_rejected(db_connection, overrides):
    tenant_id = add_tenant(db_connection, f"rate-invalid-{uuid4()}")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["rate_limit_policies"]).values(
                **policy_values(tenant_id, **overrides)
            )
        )


def test_degraded_factor_application_default_and_polymorphic_scope():
    default = RateLimitPolicy.__table__.c.degraded_factor.default
    assert default is not None
    assert default.arg == Decimal("0.25")
    assert RateLimitPolicy.__table__.c.degraded_factor.server_default is None

    scope_fks = [
        fk
        for fk in RateLimitPolicy.__table__.foreign_key_constraints
        if "scope_id" in fk.column_keys
    ]
    assert scope_fks == []


def test_rate_policy_indexes_exist(db_connection):
    indexes = {
        item["name"]: item
        for item in inspect(db_connection).get_indexes("rate_limit_policies")
    }
    assert "ix_rate_limit_policies_scope" in indexes
    assert indexes["uq_rate_limit_policy_one_enabled"]["unique"] is True


def test_admin_token_rate_scope_requires_same_tenant_token(db_connection):
    timestamp = datetime.now(UTC)
    tenant_a = add_tenant(db_connection, "rate-admin-token-a")
    tenant_b = add_tenant(db_connection, "rate-admin-token-b")
    user_id, token_id = uuid4(), uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=user_id,
            tenant_id=tenant_a,
            name="rate-admin",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    db_connection.execute(
        insert(Base.metadata.tables["admin_tokens"]).values(
            id=token_id,
            tenant_id=tenant_a,
            admin_user_id=user_id,
            token_prefix="adm_scope12",
            token_hash=f"scope-{token_id}",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )

    validator = RateScopeValidator()
    validator.validate_admin_token(
        db_connection, tenant_id=tenant_a, admin_token_id=token_id
    )
    with pytest.raises(PermissionValidationError):
        validator.validate_admin_token(
            db_connection, tenant_id=tenant_b, admin_token_id=token_id
        )
