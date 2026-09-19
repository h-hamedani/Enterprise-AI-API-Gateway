from __future__ import annotations

import base64

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def valid_key() -> str:
    return base64.b64encode(b"k" * 32).decode()


def test_production_requires_security_secrets_and_separate_role_dsns():
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            environment="production",
            postgres_dsn="postgresql+psycopg://runtime@db/gateway",
            postgres_migration_dsn="postgresql+psycopg://owner@db/gateway",
        )


def test_production_accepts_valid_security_configuration():
    settings = Settings(
        _env_file=None,
        environment="production",
        postgres_dsn="postgresql+psycopg://runtime@db/gateway",
        postgres_migration_dsn="postgresql+psycopg://owner@db/gateway",
        postgres_retention_dsn="postgresql+psycopg://retention@db/gateway",
        postgres_security_operations_dsn=(
            "postgresql+psycopg://security_operations@db/gateway"
        ),
        credential_hmac_secret=valid_key(),
        idempotency_hmac_secret=valid_key(),
        encryption_keys={1: valid_key()},
        encryption_current_key_version=1,
        break_glass_secret_hash="a" * 64,
    )
    assert settings.encryption_current_key_version == 1


def test_production_rejects_reused_database_role_dsn():
    shared_dsn = "postgresql+psycopg://shared@db/gateway"
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            environment="production",
            postgres_dsn=shared_dsn,
            postgres_migration_dsn="postgresql+psycopg://owner@db/gateway",
            postgres_retention_dsn="postgresql+psycopg://retention@db/gateway",
            postgres_security_operations_dsn=shared_dsn,
            credential_hmac_secret=valid_key(),
            idempotency_hmac_secret=valid_key(),
            encryption_keys={1: valid_key()},
            encryption_current_key_version=1,
            break_glass_secret_hash="a" * 64,
        )
