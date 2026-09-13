import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_default_dependency_timeout_is_one_second() -> None:
    settings = Settings(_env_file=None)

    assert settings.dependency_timeout_seconds == 1.0


def test_invalid_dependency_timeout_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            dependency_timeout_seconds=0,
        )


def test_production_rejects_default_database_credentials() -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            environment="production",
            postgres_dsn=(
                "postgresql+psycopg://gateway:gateway@localhost:55432/gateway"
            ),
        )
