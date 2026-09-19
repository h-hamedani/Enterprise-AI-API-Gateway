from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, insert, select, text, update

from app.control_plane.bootstrap import (
    BootstrapUnavailableError,
    FirstAdminBootstrapService,
)
from app.control_plane.break_glass import (
    BreakGlassDeniedError,
    BreakGlassRecoveryService,
)
from app.control_plane.permissions import (
    PermissionValidationError,
    PolymorphicPermissionValidator,
)
from app.core.config import get_settings
from app.core.idempotency import (
    ClaimState,
    IdempotencyConflictError,
    IdempotencyExpiredError,
    IdempotencyRepository,
)
from app.core.security.credentials import CredentialHasher
from app.core.security.idempotency import IdempotencyDigester
from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository


@pytest.fixture
def db_connection():
    engine = create_engine(get_settings().postgres_migration_dsn)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()
    engine.dispose()


def add_tenant(connection, name: str):
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


def test_tenant_repository_cannot_load_or_update_another_tenant(db_connection):
    tenant_a = add_tenant(db_connection, "repository-a")
    tenant_b = add_tenant(db_connection, "repository-b")
    service_id = uuid4()
    timestamp = datetime.now(UTC)
    services = Base.metadata.tables["normal_api_services"]
    db_connection.execute(
        insert(services).values(
            id=service_id,
            tenant_id=tenant_a,
            slug="repository-service",
            display_name="Service",
            upstream_base_url="https://example.test",
            status="ACTIVE",
            request_body_limit_bytes=1024,
            connect_timeout_seconds=5,
            pool_timeout_seconds=5,
            write_timeout_seconds=30,
            read_idle_timeout_seconds=60,
            pre_response_timeout_seconds=120,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    repository = TenantScopedRepository(services)
    assert (
        repository.get(db_connection, tenant_id=tenant_b, resource_id=service_id)
        is None
    )
    assert not repository.update(
        db_connection,
        tenant_id=tenant_b,
        resource_id=service_id,
        values={"display_name": "Changed"},
    )


def test_permission_validator_rejects_cross_tenant_resource(db_connection):
    tenant_a = add_tenant(db_connection, "permission-validator-a")
    tenant_b = add_tenant(db_connection, "permission-validator-b")
    service_id = uuid4()
    timestamp = datetime.now(UTC)
    db_connection.execute(
        insert(Base.metadata.tables["normal_api_services"]).values(
            id=service_id,
            tenant_id=tenant_a,
            slug="permission-service",
            display_name="Service",
            upstream_base_url="https://example.test",
            status="ACTIVE",
            request_body_limit_bytes=1024,
            connect_timeout_seconds=5,
            pool_timeout_seconds=5,
            write_timeout_seconds=30,
            read_idle_timeout_seconds=60,
            pre_response_timeout_seconds=120,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    validator = PolymorphicPermissionValidator()
    validator.validate(
        db_connection,
        tenant_id=tenant_a,
        resource_type="SERVICE",
        resource_id=service_id,
        action="INVOKE",
    )
    with pytest.raises(PermissionValidationError):
        validator.validate(
            db_connection,
            tenant_id=tenant_b,
            resource_type="SERVICE",
            resource_id=service_id,
            action="INVOKE",
        )


def test_bootstrap_is_show_once_hash_only_and_second_attempt_fails(db_connection):
    service = FirstAdminBootstrapService(CredentialHasher(b"c" * 32))
    result = service.bootstrap(db_connection, admin_name="initial-admin")
    token = (
        db_connection.execute(
            select(Base.metadata.tables["admin_tokens"]).where(
                Base.metadata.tables["admin_tokens"].c.admin_user_id
                == result.admin_user_id
            )
        )
        .mappings()
        .one()
    )
    assert result.raw_token.startswith("adm_")
    assert result.raw_token not in token["token_hash"]
    assert token["token_prefix"] == result.raw_token[:12]
    with pytest.raises(BootstrapUnavailableError):
        service.bootstrap(db_connection, admin_name="second-admin")


def test_bootstrap_audit_failure_rolls_back_everything(db_connection):
    def reject_audit(_connection, clause, *_args, **_kwargs):
        if getattr(getattr(clause, "table", None), "name", None) == "audit_logs":
            raise RuntimeError("simulated audit failure")

    event.listen(db_connection, "before_execute", reject_audit)
    try:
        with (
            pytest.raises(RuntimeError, match="simulated audit failure"),
            db_connection.begin_nested(),
        ):
            FirstAdminBootstrapService(CredentialHasher(b"c" * 32)).bootstrap(
                db_connection, admin_name="must-rollback"
            )
    finally:
        event.remove(db_connection, "before_execute", reject_audit)

    assert (
        db_connection.scalar(
            select(text("count(*)")).select_from(Base.metadata.tables["admin_users"])
        )
        == 0
    )
    assert (
        db_connection.scalar(
            select(text("count(*)")).select_from(Base.metadata.tables["admin_tokens"])
        )
        == 0
    )


def test_break_glass_rotates_token_and_writes_non_secret_audit(db_connection):
    hasher = CredentialHasher(b"c" * 32)
    bootstrap = FirstAdminBootstrapService(hasher).bootstrap(
        db_connection, admin_name="recover-admin"
    )
    recovery_secret = "operator-recovery-secret"
    result = BreakGlassRecoveryService(
        hasher,
        recovery_secret_hash=hashlib.sha256(recovery_secret.encode()).hexdigest(),
    ).recover(
        db_connection,
        tenant_id=bootstrap.tenant_id,
        admin_user_id=bootstrap.admin_user_id,
        recovery_secret=recovery_secret,
        recovery_mechanism="hidden_prompt",
    )
    audits = (
        db_connection.execute(
            select(Base.metadata.tables["audit_logs"]).where(
                Base.metadata.tables["audit_logs"].c.action
                == "ADMIN_BREAK_GLASS_RECOVERY"
            )
        )
        .mappings()
        .all()
    )
    assert result.raw_replacement_token.startswith("adm_")
    assert len(audits) == 1
    assert recovery_secret not in str(audits[0])
    assert result.raw_replacement_token not in str(audits[0])


def test_break_glass_wrong_secret_changes_nothing(db_connection):
    hasher = CredentialHasher(b"c" * 32)
    bootstrap = FirstAdminBootstrapService(hasher).bootstrap(
        db_connection, admin_name="denied-admin"
    )
    before = db_connection.scalar(
        select(text("count(*)")).select_from(Base.metadata.tables["admin_tokens"])
    )
    with pytest.raises(BreakGlassDeniedError):
        BreakGlassRecoveryService(
            hasher,
            recovery_secret_hash=hashlib.sha256(b"correct").hexdigest(),
        ).recover(
            db_connection,
            tenant_id=bootstrap.tenant_id,
            admin_user_id=bootstrap.admin_user_id,
            recovery_secret="wrong",
            recovery_mechanism="hidden_prompt",
        )
    after = db_connection.scalar(
        select(text("count(*)")).select_from(Base.metadata.tables["admin_tokens"])
    )
    assert after == before


def test_idempotency_digest_persistence_replay_and_conflict(db_connection):
    bootstrap = FirstAdminBootstrapService(CredentialHasher(b"c" * 32)).bootstrap(
        db_connection, admin_name="idempotency-admin"
    )
    repository = IdempotencyRepository(IdempotencyDigester(b"i" * 32))
    raw_key = "raw-idempotency-value"
    claim = repository.claim(
        db_connection,
        tenant_id=bootstrap.tenant_id,
        admin_user_id=bootstrap.admin_user_id,
        endpoint_key="api-key.create",
        raw_key=raw_key,
        request_fingerprint="a" * 64,
    )
    assert claim.state is ClaimState.CREATED
    row = (
        db_connection.execute(
            select(Base.metadata.tables["idempotency_records"]).where(
                Base.metadata.tables["idempotency_records"].c.id == claim.record_id
            )
        )
        .mappings()
        .one()
    )
    assert raw_key not in str(row)
    assert row["expires_at"] == row["created_at"] + timedelta(hours=24)
    repository.complete(
        db_connection,
        tenant_id=bootstrap.tenant_id,
        record_id=claim.record_id,
        response_status=201,
        response_body_ciphertext=b"encrypted-replay",
    )
    replay = repository.claim(
        db_connection,
        tenant_id=bootstrap.tenant_id,
        admin_user_id=bootstrap.admin_user_id,
        endpoint_key="api-key.create",
        raw_key=raw_key,
        request_fingerprint="a" * 64,
    )
    assert replay.state is ClaimState.REPLAY
    with pytest.raises(IdempotencyConflictError):
        repository.claim(
            db_connection,
            tenant_id=bootstrap.tenant_id,
            admin_user_id=bootstrap.admin_user_id,
            endpoint_key="api-key.create",
            raw_key=raw_key,
            request_fingerprint="b" * 64,
        )


def test_expired_idempotency_record_is_never_replayed(db_connection):
    bootstrap = FirstAdminBootstrapService(CredentialHasher(b"c" * 32)).bootstrap(
        db_connection, admin_name="expired-idempotency-admin"
    )
    repository = IdempotencyRepository(IdempotencyDigester(b"i" * 32))
    raw_key = "expired-idempotency-value"
    claim = repository.claim(
        db_connection,
        tenant_id=bootstrap.tenant_id,
        admin_user_id=bootstrap.admin_user_id,
        endpoint_key="api-key.create",
        raw_key=raw_key,
        request_fingerprint="a" * 64,
    )
    table = Base.metadata.tables["idempotency_records"]
    db_connection.execute(
        update(table)
        .where(table.c.id == claim.record_id)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    with pytest.raises(IdempotencyExpiredError):
        repository.claim(
            db_connection,
            tenant_id=bootstrap.tenant_id,
            admin_user_id=bootstrap.admin_user_id,
            endpoint_key="api-key.create",
            raw_key=raw_key,
            request_fingerprint="a" * 64,
        )


def test_database_role_privilege_boundaries(db_connection):
    assert (
        db_connection.scalar(
            text("SELECT has_schema_privilege('migration_owner', 'public', 'CREATE')")
        )
        is True
    )
    assert (
        db_connection.scalar(
            text("SELECT has_schema_privilege('gateway_runtime', 'public', 'CREATE')")
        )
        is False
    )
    assert (
        db_connection.scalar(
            text("SELECT has_table_privilege('gateway_runtime', 'requests', 'DELETE')")
        )
        is False
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('gateway_runtime', 'audit_logs', 'UPDATE')"
            )
        )
        is False
    )
    assert (
        db_connection.scalar(
            text("SELECT has_table_privilege('retention_worker', 'requests', 'SELECT')")
        )
        is True
    )
    assert (
        db_connection.scalar(
            text("SELECT has_table_privilege('retention_worker', 'requests', 'DELETE')")
        )
        is True
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('retention_worker', 'admin_tokens', 'SELECT')"
            )
        )
        is False
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('security_operations', 'admin_tokens', 'UPDATE')"
            )
        )
        is True
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('security_operations', 'audit_logs', 'INSERT')"
            )
        )
        is True
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('security_operations', 'audit_logs', 'UPDATE')"
            )
        )
        is False
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('security_operations', 'audit_logs', 'DELETE')"
            )
        )
        is False
    )
    assert (
        db_connection.scalar(
            text(
                "SELECT has_table_privilege('gateway_runtime', 'audit_logs', 'DELETE')"
            )
        )
        is False
    )
