from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert, inspect
from sqlalchemy.exc import DBAPIError, IntegrityError

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


def now():
    return datetime.now(UTC)


def add_tenant(connection, name):
    tenant_id, ts = uuid4(), now()
    connection.execute(
        insert(Base.metadata.tables["tenants"]).values(
            id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    return tenant_id


def add_admin(connection, tenant_id, suffix):
    user_id, token_id, ts = uuid4(), uuid4(), now()
    connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=user_id,
            tenant_id=tenant_id,
            name=f"admin-{suffix}",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["admin_tokens"]).values(
            id=token_id,
            tenant_id=tenant_id,
            admin_user_id=user_id,
            token_hash=f"hash-{suffix}",
            token_prefix=f"adm_{suffix}"[:32],
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    return user_id, token_id


def audit_values(tenant_id, **overrides):
    values = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "action": "target.update",
        "resource_type": "LLM_TARGET",
        "resource_id": uuid4(),
        "result": "SUCCESS",
        "metadata": {"changed_fields": ["status"]},
        "created_at": now(),
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize("result", ["SUCCESS", "FAILED"])
def test_audit_actor_and_results_are_supported(db_connection, result):
    tenant_id = add_tenant(db_connection, f"audit-{result.lower()}")
    user_id, token_id = add_admin(db_connection, tenant_id, result.lower())
    db_connection.execute(
        insert(Base.metadata.tables["audit_logs"]).values(
            **audit_values(
                tenant_id,
                actor_admin_user_id=user_id,
                actor_admin_token_id=token_id,
                request_id=uuid4(),
                result=result,
            )
        )
    )


def test_system_audit_actor_may_be_null(db_connection):
    tenant_id = add_tenant(db_connection, "audit-system")
    db_connection.execute(
        insert(Base.metadata.tables["audit_logs"]).values(**audit_values(tenant_id))
    )


@pytest.mark.parametrize("actor", ["actor_admin_user_id", "actor_admin_token_id"])
def test_cross_tenant_audit_actor_is_rejected(db_connection, actor):
    tenant_a = add_tenant(db_connection, f"audit-cross-a-{actor}")
    tenant_b = add_tenant(db_connection, f"audit-cross-b-{actor}")
    user_id, token_id = add_admin(db_connection, tenant_a, actor)
    actor_id = user_id if actor == "actor_admin_user_id" else token_id
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["audit_logs"]).values(
                **audit_values(tenant_b, **{actor: actor_id})
            )
        )


def test_invalid_audit_result_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "audit-invalid-result")
    with pytest.raises(DBAPIError):
        db_connection.execute(
            insert(Base.metadata.tables["audit_logs"]).values(
                **audit_values(tenant_id, result="UNKNOWN")
            )
        )


def test_audit_request_id_is_correlation_only(db_connection):
    tenant_id = add_tenant(db_connection, "audit-correlation")
    db_connection.execute(
        insert(Base.metadata.tables["audit_logs"]).values(
            **audit_values(tenant_id, request_id=uuid4())
        )
    )
    request_fks = [
        fk
        for fk in Base.metadata.tables["audit_logs"].foreign_key_constraints
        if "request_id" in fk.column_keys
    ]
    assert request_fks == []


def test_audit_schema_and_indexes_are_safe(db_connection):
    inspector = inspect(db_connection)
    names = {column["name"] for column in inspector.get_columns("audit_logs")}
    assert names.isdisjoint(
        {
            "secret",
            "token_hash",
            "key_hash",
            "ciphertext",
            "authorization",
            "cookie",
            "provider_payload",
            "exception_trace",
        }
    )
    indexes = {item["name"] for item in inspector.get_indexes("audit_logs")}
    assert {
        "ix_audit_logs_tenant_created_id",
        "ix_audit_logs_tenant_resource",
        "ix_audit_logs_request_id",
    } <= indexes


def test_config_version_is_one_row_per_tenant(db_connection):
    tenant_id = add_tenant(db_connection, "config-version")
    table = Base.metadata.tables["config_versions"]
    values = {"tenant_id": tenant_id, "version": 0, "updated_at": now()}
    db_connection.execute(insert(table).values(**values))
    with pytest.raises(IntegrityError):
        db_connection.execute(insert(table).values(**values))


def test_negative_config_version_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "config-negative")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["config_versions"]).values(
                tenant_id=tenant_id, version=-1, updated_at=now()
            )
        )


def test_cross_tenant_config_updater_is_rejected(db_connection):
    tenant_a = add_tenant(db_connection, "config-updater-a")
    tenant_b = add_tenant(db_connection, "config-updater-b")
    user_id, _ = add_admin(db_connection, tenant_a, "config")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["config_versions"]).values(
                tenant_id=tenant_b,
                version=1,
                updated_at=now(),
                updated_by_admin_user_id=user_id,
            )
        )


def checkpoint_values(tenant_id, **overrides):
    ts = now()
    values = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "job_type": "API_RETENTION",
        "scope_date": now().date(),
        "cutoff_at": ts - timedelta(days=180),
        "state": "STARTED",
        "started_at": ts,
        "updated_at": ts,
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize(
    "state", ["STARTED", "ROLLED_UP", "VERIFIED", "PURGING", "COMPLETED", "FAILED"]
)
def test_all_retention_states_are_accepted(db_connection, state):
    tenant_id = add_tenant(db_connection, f"retention-{state.lower()}")
    overrides = {"state": state}
    if state == "COMPLETED":
        overrides["completed_at"] = now()
    db_connection.execute(
        insert(Base.metadata.tables["retention_checkpoints"]).values(
            **checkpoint_values(tenant_id, **overrides)
        )
    )


@pytest.mark.parametrize("job_type", ["API_RETENTION", "LLM_RETENTION"])
def test_retention_job_types_are_accepted(db_connection, job_type):
    tenant_id = add_tenant(db_connection, f"retention-job-{job_type.lower()}")
    db_connection.execute(
        insert(Base.metadata.tables["retention_checkpoints"]).values(
            **checkpoint_values(tenant_id, job_type=job_type)
        )
    )


@pytest.mark.parametrize("field", ["state", "job_type"])
def test_invalid_retention_enum_is_rejected(db_connection, field):
    tenant_id = add_tenant(db_connection, f"retention-invalid-{field}")
    with pytest.raises(DBAPIError):
        db_connection.execute(
            insert(Base.metadata.tables["retention_checkpoints"]).values(
                **checkpoint_values(tenant_id, **{field: "UNKNOWN"})
            )
        )


@pytest.mark.parametrize("scope_date", [datetime.now(UTC).date(), None])
def test_retention_run_identity_is_deterministic(db_connection, scope_date):
    tenant_id = add_tenant(db_connection, f"retention-unique-{scope_date}")
    table = Base.metadata.tables["retention_checkpoints"]
    cutoff = now() - timedelta(days=180)
    db_connection.execute(
        insert(table).values(
            **checkpoint_values(tenant_id, scope_date=scope_date, cutoff_at=cutoff)
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(table).values(
                **checkpoint_values(tenant_id, scope_date=scope_date, cutoff_at=cutoff)
            )
        )


@pytest.mark.parametrize("field", ["source_row_count", "aggregate_row_count"])
def test_negative_retention_counter_is_rejected(db_connection, field):
    tenant_id = add_tenant(db_connection, f"retention-negative-{field}")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["retention_checkpoints"]).values(
                **checkpoint_values(tenant_id, **{field: -1})
            )
        )


def test_completed_at_requires_completed_state(db_connection):
    tenant_id = add_tenant(db_connection, "retention-completed-state")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["retention_checkpoints"]).values(
                **checkpoint_values(tenant_id, state="FAILED", completed_at=now())
            )
        )


def test_failed_checkpoint_persists_resume_fields_and_indexes(db_connection):
    tenant_id = add_tenant(db_connection, "retention-failed-resume")
    db_connection.execute(
        insert(Base.metadata.tables["retention_checkpoints"]).values(
            **checkpoint_values(
                tenant_id,
                state="FAILED",
                source_row_count=100,
                aggregate_row_count=99,
                last_purged_id=uuid4(),
                error_code="VERIFY_MISMATCH",
            )
        )
    )
    indexes = {
        item["name"]
        for item in inspect(db_connection).get_indexes("retention_checkpoints")
    }
    assert {
        "ix_retention_checkpoints_resume",
        "ix_retention_checkpoints_cutoff",
    } <= indexes


def test_usage_aggregate_tables_are_deferred(db_connection):
    tables = set(inspect(db_connection).get_table_names())
    assert "api_usage_daily" not in tables
    assert "llm_usage_daily" not in tables
