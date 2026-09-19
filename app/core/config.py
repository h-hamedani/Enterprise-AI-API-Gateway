import base64
import binascii
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Enterprise AI & API Gateway"
    app_version: str = "0.1.0"

    environment: Literal[
        "development",
        "test",
        "staging",
        "production",
    ] = "development"

    postgres_dsn: str = (
        "postgresql+psycopg_async://gateway:gateway@localhost:55432/gateway"
    )

    postgres_migration_dsn: str = (
        "postgresql+psycopg://gateway:gateway@localhost:55432/gateway"
    )

    postgres_retention_dsn: str | None = None
    postgres_security_operations_dsn: str | None = None

    credential_hmac_secret: str | None = None
    idempotency_hmac_secret: str | None = None
    encryption_keys: dict[int, str] = Field(default_factory=dict)
    encryption_current_key_version: int | None = None
    break_glass_secret_hash: str | None = None

    redis_url: str = "redis://localhost:6379/0"

    dependency_timeout_seconds: float = Field(
        default=1.0,
        gt=0,
        le=10,
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_runtime_configuration(self) -> "Settings":
        if self.environment == "production" and "gateway:gateway" in self.postgres_dsn:
            raise ValueError(
                "Default development PostgreSQL credentials "
                "must not be used in production."
            )

        if self.environment == "production":
            required = {
                "postgres_retention_dsn": self.postgres_retention_dsn,
                "postgres_security_operations_dsn": (
                    self.postgres_security_operations_dsn
                ),
                "credential_hmac_secret": self.credential_hmac_secret,
                "idempotency_hmac_secret": self.idempotency_hmac_secret,
                "encryption_current_key_version": self.encryption_current_key_version,
                "break_glass_secret_hash": self.break_glass_secret_hash,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(
                    "Missing production security configuration: "
                    + ", ".join(sorted(missing))
                )
            role_dsns = {
                self.postgres_dsn,
                self.postgres_migration_dsn,
                self.postgres_retention_dsn,
                self.postgres_security_operations_dsn,
            }
            if len(role_dsns) != 4:
                raise ValueError("Production database role DSNs must all differ.")

        for name, encoded in (
            ("credential_hmac_secret", self.credential_hmac_secret),
            ("idempotency_hmac_secret", self.idempotency_hmac_secret),
        ):
            if encoded is not None:
                self._decode_32_byte_key(name, encoded)
        for version, encoded in self.encryption_keys.items():
            if version <= 0:
                raise ValueError("Encryption key versions must be positive.")
            self._decode_32_byte_key(f"encryption_keys[{version}]", encoded)
        if self.encryption_current_key_version is not None and (
            self.encryption_current_key_version not in self.encryption_keys
        ):
            raise ValueError("Current encryption key version is unavailable.")
        return self

    @staticmethod
    def _decode_32_byte_key(name: str, encoded: str) -> bytes:
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"{name} must be valid base64.") from exc
        if len(decoded) != 32:
            raise ValueError(f"{name} must decode to exactly 32 bytes.")
        return decoded


@lru_cache
def get_settings() -> Settings:
    return Settings()
