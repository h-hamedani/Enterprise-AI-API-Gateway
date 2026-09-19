from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, insert, select, text
from sqlalchemy.exc import DBAPIError

from app import cli
from app.control_plane.bootstrap import FirstAdminBootstrapService
from app.core.config import get_settings
from app.core.security.credentials import CredentialHasher
from app.persistence.models import Base


def test_audit_table_is_append_only_for_operational_roles():
    engine = create_engine(get_settings().postgres_migration_dsn)
    tenants = Base.metadata.tables["tenants"]
    audit = Base.metadata.tables["audit_logs"]
    tenant_id = uuid4()
    audit_id = uuid4()
    timestamp = datetime.now(UTC)
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(tenants).values(
                    id=tenant_id,
                    name=f"m17-role-{tenant_id}",
                    status="ACTIVE",
                    created_at=timestamp,
                    updated_at=timestamp,
                )
            )
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL ROLE security_operations"))
            connection.execute(
                insert(audit).values(
                    id=audit_id,
                    tenant_id=tenant_id,
                    action="M17_ROLE_TEST",
                    resource_type="TENANT",
                    resource_id=tenant_id,
                    result="SUCCESS",
                    created_at=timestamp,
                )
            )

        for role, statement in (
            (
                "security_operations",
                audit.update().where(audit.c.id == audit_id).values(result="FAILED"),
            ),
            ("security_operations", audit.delete().where(audit.c.id == audit_id)),
            (
                "gateway_runtime",
                audit.update().where(audit.c.id == audit_id).values(result="FAILED"),
            ),
            ("gateway_runtime", audit.delete().where(audit.c.id == audit_id)),
        ):
            with pytest.raises(DBAPIError), engine.begin() as connection:
                connection.execute(text(f"SET LOCAL ROLE {role}"))
                connection.execute(statement)
    finally:
        with engine.begin() as connection:
            connection.execute(audit.delete().where(audit.c.tenant_id == tenant_id))
            connection.execute(tenants.delete().where(tenants.c.id == tenant_id))
        engine.dispose()


@pytest.fixture
def recovery_context(monkeypatch):
    engine = create_engine(get_settings().postgres_migration_dsn)
    hasher = CredentialHasher(b"c" * 32)
    recovery_secret = "correct-recovery-secret"
    with engine.begin() as connection:
        bootstrap = FirstAdminBootstrapService(hasher).bootstrap(
            connection, admin_name="m17-recovery-admin"
        )

    monkeypatch.setattr(cli, "_require_interactive_output", lambda: None)
    monkeypatch.setattr(cli, "_security_engine", lambda: engine)
    monkeypatch.setattr(cli, "_credential_hasher", lambda: hasher)
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(
            break_glass_secret_hash=hashlib.sha256(recovery_secret.encode()).hexdigest()
        ),
    )

    yield engine, bootstrap, recovery_secret

    cleanup_engine = create_engine(get_settings().postgres_migration_dsn)
    with cleanup_engine.begin() as connection:
        audit = Base.metadata.tables["audit_logs"]
        tokens = Base.metadata.tables["admin_tokens"]
        users = Base.metadata.tables["admin_users"]
        tenants = Base.metadata.tables["tenants"]
        connection.execute(
            audit.delete().where(audit.c.tenant_id == bootstrap.tenant_id)
        )
        connection.execute(
            tokens.delete().where(tokens.c.tenant_id == bootstrap.tenant_id)
        )
        connection.execute(
            users.delete().where(users.c.tenant_id == bootstrap.tenant_id)
        )
        connection.execute(tenants.delete().where(tenants.c.id == bootstrap.tenant_id))
    cleanup_engine.dispose()


def run_wrong_secret(monkeypatch, bootstrap) -> int:
    monkeypatch.setattr(
        cli,
        "_read_recovery_secret",
        lambda _secret_file: ("wrong-recovery-secret", "hidden_prompt"),
    )
    return cli.main(
        [
            "break-glass",
            "--tenant-id",
            str(bootstrap.tenant_id),
            "--admin-user-id",
            str(bootstrap.admin_user_id),
        ]
    )


def failed_audits(engine, tenant_id):
    audit = Base.metadata.tables["audit_logs"]
    with engine.connect() as connection:
        return (
            connection.execute(
                select(audit).where(
                    audit.c.tenant_id == tenant_id,
                    audit.c.action == "ADMIN_BREAK_GLASS_RECOVERY",
                    audit.c.result == "FAILED",
                )
            )
            .mappings()
            .all()
        )


def test_wrong_secret_persists_one_safe_failed_audit_without_mutation(
    recovery_context, monkeypatch, caplog
):
    engine, bootstrap, _recovery_secret = recovery_context
    tokens = Base.metadata.tables["admin_tokens"]
    users = Base.metadata.tables["admin_users"]
    with engine.connect() as connection:
        before_tokens = (
            connection.execute(
                select(tokens).where(tokens.c.admin_user_id == bootstrap.admin_user_id)
            )
            .mappings()
            .all()
        )
        before_user = (
            connection.execute(
                select(users).where(users.c.id == bootstrap.admin_user_id)
            )
            .mappings()
            .one()
        )

    assert run_wrong_secret(monkeypatch, bootstrap) == 1

    audits = failed_audits(engine, bootstrap.tenant_id)
    with engine.connect() as connection:
        after_tokens = (
            connection.execute(
                select(tokens).where(tokens.c.admin_user_id == bootstrap.admin_user_id)
            )
            .mappings()
            .all()
        )
        after_user = (
            connection.execute(
                select(users).where(users.c.id == bootstrap.admin_user_id)
            )
            .mappings()
            .one()
        )
    assert len(audits) == 1
    assert audits[0]["metadata"]["failure_category"] == "INVALID_RECOVERY_SECRET"
    assert "wrong-recovery-secret" not in str(audits[0])
    assert "wrong-recovery-secret" not in caplog.text
    assert after_tokens == before_tokens
    assert after_user == before_user


def test_repeated_wrong_secret_attempts_create_separate_failed_audits(
    recovery_context, monkeypatch
):
    engine, bootstrap, _recovery_secret = recovery_context

    assert run_wrong_secret(monkeypatch, bootstrap) == 1
    assert run_wrong_secret(monkeypatch, bootstrap) == 1

    assert len(failed_audits(engine, bootstrap.tenant_id)) == 2


def test_success_audit_failure_rolls_back_state_and_persists_failed_audit(
    recovery_context, monkeypatch
):
    engine, bootstrap, recovery_secret = recovery_context
    tokens = Base.metadata.tables["admin_tokens"]
    users = Base.metadata.tables["admin_users"]
    rejected = False

    def reject_first_audit(_connection, clause, *_args, **_kwargs):
        nonlocal rejected
        if (
            not rejected
            and getattr(getattr(clause, "table", None), "name", None) == "audit_logs"
        ):
            rejected = True
            raise RuntimeError("simulated success audit failure")

    monkeypatch.setattr(
        cli,
        "_read_recovery_secret",
        lambda _secret_file: (recovery_secret, "hidden_prompt"),
    )
    with engine.begin() as connection:
        connection.execute(
            users.update()
            .where(users.c.id == bootstrap.admin_user_id)
            .values(status="DISABLED")
        )
    with engine.connect() as connection:
        before_tokens = (
            connection.execute(
                select(tokens).where(tokens.c.admin_user_id == bootstrap.admin_user_id)
            )
            .mappings()
            .all()
        )
    event.listen(engine, "before_execute", reject_first_audit)
    try:
        assert (
            cli.main(
                [
                    "break-glass",
                    "--tenant-id",
                    str(bootstrap.tenant_id),
                    "--admin-user-id",
                    str(bootstrap.admin_user_id),
                ]
            )
            == 1
        )
    finally:
        event.remove(engine, "before_execute", reject_first_audit)

    with engine.connect() as connection:
        after_tokens = (
            connection.execute(
                select(tokens).where(tokens.c.admin_user_id == bootstrap.admin_user_id)
            )
            .mappings()
            .all()
        )
        admin_status = connection.scalar(
            select(users.c.status).where(users.c.id == bootstrap.admin_user_id)
        )
    audits = failed_audits(engine, bootstrap.tenant_id)
    assert after_tokens == before_tokens
    assert admin_status == "DISABLED"
    assert len(audits) == 1
    assert audits[0]["metadata"]["failure_category"] == "RECOVERY_TRANSACTION_FAILED"


def test_failed_audit_insertion_is_surfaced_fail_closed(recovery_context, monkeypatch):
    engine, bootstrap, _recovery_secret = recovery_context

    def reject_audit(_connection, clause, *_args, **_kwargs):
        if getattr(getattr(clause, "table", None), "name", None) == "audit_logs":
            raise RuntimeError("simulated failed audit failure")

    monkeypatch.setattr(
        cli,
        "_read_recovery_secret",
        lambda _secret_file: ("wrong-recovery-secret", "hidden_prompt"),
    )
    event.listen(engine, "before_execute", reject_audit)
    try:
        with pytest.raises(cli.BreakGlassAuditError):
            run_wrong_secret(monkeypatch, bootstrap)
    finally:
        event.remove(engine, "before_execute", reject_audit)
