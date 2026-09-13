from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Enterprise AI & API Gateway"
    app_version: str = "0.1.0"
    environment: str = "development"

    postgres_dsn: str = "postgresql+psycopg://gateway:gateway@localhost:5432/gateway"
    redis_url: str = "redis://localhost:6379/0"

    dependency_timeout_seconds: float = 1.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
