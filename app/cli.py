from __future__ import annotations

import argparse
import base64
import getpass
import os
import stat
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import create_engine, text

from app.control_plane.bootstrap import FirstAdminBootstrapService
from app.control_plane.break_glass import BreakGlassRecoveryService
from app.core.config import get_settings
from app.core.security.credentials import CredentialHasher


class UnsafeSecretOutputError(RuntimeError):
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
        with engine.begin() as connection:
            result = BreakGlassRecoveryService(
                _credential_hasher(),
                recovery_secret_hash=settings.break_glass_secret_hash,
            ).recover(
                connection,
                tenant_id=args.tenant_id,
                admin_user_id=args.admin_user_id,
                recovery_secret=recovery_secret,
                recovery_mechanism=mechanism,
            )
        print(result.raw_replacement_token)
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
