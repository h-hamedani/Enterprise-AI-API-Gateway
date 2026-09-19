from __future__ import annotations

import argparse
import base64
import getpass
import os
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import Engine

from app.control_plane.bootstrap import FirstAdminBootstrapService
from app.control_plane.break_glass import (
    BreakGlassDeniedError,
    BreakGlassFailureCategory,
    BreakGlassRecoveryService,
    BreakGlassResult,
)
from app.core.config import get_settings
from app.core.security.credentials import CredentialHasher
from app.persistence.models import Base


class UnsafeSecretOutputError(RuntimeError):
    pass


class BreakGlassAuditError(RuntimeError):
    pass


def _require_interactive_output() -> None:
    if not sys.stdout.isatty():
        raise UnsafeSecretOutputError(
            "Raw administrator token output requires an interactive TTY."
        )


def _read_recovery_secret(secret_file: Path | None) -> tuple[str, str]:
    if secret_file is None:
        return getpass.getpass("Recovery secret: "), "hidden_prompt"
    if os.name == "nt":
        raise PermissionError(
            "Recovery secret files are disabled on Windows until ACL validation "
            "is available; use the hidden prompt."
        )
    file_stat = secret_file.stat()
    if stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise PermissionError(
            "Recovery secret file must not be group/world accessible."
        )
    return secret_file.read_text(encoding="utf-8").strip(), "restricted_secret_file"


def _credential_hasher() -> CredentialHasher:
    encoded = get_settings().credential_hmac_secret
    if encoded is None:
        raise RuntimeError("CREDENTIAL_HMAC_SECRET is required.")
    return CredentialHasher(base64.b64decode(encoded, validate=True))


def _security_engine():
    dsn = get_settings().postgres_security_operations_dsn
    if dsn is None:
        raise RuntimeError("POSTGRES_SECURITY_OPERATIONS_DSN is required.")
    engine = create_engine(dsn)
    with engine.connect() as connection:
        role = connection.scalar(text("SELECT current_user"))
    if role != "security_operations":
        engine.dispose()
        raise RuntimeError("Security CLI requires the security_operations DB role.")
    return engine


def _record_failed_break_glass_audit(
    engine: Engine,
    *,
    tenant_id: UUID,
    admin_user_id: UUID,
    recovery_mechanism: str,
    failure_category: BreakGlassFailureCategory,
) -> None:
    audit = Base.metadata.tables["audit_logs"]
    timestamp = datetime.now(UTC)
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(audit).values(
                    id=uuid4(),
                    tenant_id=tenant_id,
                    actor_admin_user_id=None,
                    actor_admin_token_id=None,
                    action="ADMIN_BREAK_GLASS_RECOVERY",
                    resource_type="ADMIN_USER",
                    resource_id=admin_user_id,
                    result="FAILED",
                    metadata={
                        "tenant_id": str(tenant_id),
                        "admin_user_id": str(admin_user_id),
                        "recovery_mechanism": recovery_mechanism,
                        "failure_category": failure_category.value,
                        "operator_identity": "security_operations",
                    },
                    created_at=timestamp,
                )
            )
    except Exception:  # noqa: BLE001 - this boundary must fail closed on DB errors
        raise BreakGlassAuditError(
            "Break-glass recovery failed and its audit event could not be recorded."
        ) from None


def _execute_break_glass_recovery(
    engine: Engine,
    service: BreakGlassRecoveryService,
    *,
    tenant_id: UUID,
    admin_user_id: UUID,
    recovery_secret: str,
    recovery_mechanism: str,
) -> BreakGlassResult | None:
    try:
        with engine.begin() as connection:
            return service.recover(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                recovery_secret=recovery_secret,
                recovery_mechanism=recovery_mechanism,
            )
    except BreakGlassDeniedError as exc:
        failure_category = exc.failure_category
    except Exception:  # noqa: BLE001 - classify any transactional recovery failure
        failure_category = BreakGlassFailureCategory.RECOVERY_TRANSACTION_FAILED

    _record_failed_break_glass_audit(
        engine,
        tenant_id=tenant_id,
        admin_user_id=admin_user_id,
        recovery_mechanism=recovery_mechanism,
        failure_category=failure_category,
    )
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gateway-security")
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap-admin")
    bootstrap.add_argument("--name", required=True)
    recovery = commands.add_parser("break-glass")
    recovery.add_argument("--tenant-id", type=UUID, required=True)
    recovery.add_argument("--admin-user-id", type=UUID, required=True)
    recovery.add_argument("--recovery-secret-file", type=Path)
    args = parser.parse_args(argv)

    _require_interactive_output()
    settings = get_settings()
    engine = _security_engine()
    try:
        if args.command == "bootstrap-admin":
            with engine.begin() as connection:
                result = FirstAdminBootstrapService(_credential_hasher()).bootstrap(
                    connection, admin_name=args.name
                )
            print(result.raw_token)
            return 0

        if settings.break_glass_secret_hash is None:
            raise RuntimeError("BREAK_GLASS_SECRET_HASH is required.")
        recovery_secret, mechanism = _read_recovery_secret(args.recovery_secret_file)
        result = _execute_break_glass_recovery(
            engine,
            BreakGlassRecoveryService(
                _credential_hasher(),
                recovery_secret_hash=settings.break_glass_secret_hash,
            ),
            tenant_id=args.tenant_id,
            admin_user_id=args.admin_user_id,
            recovery_secret=recovery_secret,
            recovery_mechanism=mechanism,
        )
        if result is None:
            return 1
        print(result.raw_replacement_token)
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
