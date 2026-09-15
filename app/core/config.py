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

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
