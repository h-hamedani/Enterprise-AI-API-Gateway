from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from app.core.config import get_settings

EXPECTED_TABLES = {
    "admin_tokens",
    "admin_users",
    "alembic_version",
    "api_keys",
    "applications",
    "idempotency_records",
    "tenants",
}

EXPECTED_ENUMS = {
    "idempotency_state",
    "resource_status",
}


def alembic_config() -> Config:
    return Config("alembic.ini")


def database_enum_names() -> set[str]:
    settings = get_settings()
    engine = create_engine(settings.postgres_migration_dsn)

    try:
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT typname
                    FROM pg_type
                    WHERE typname IN (
                        'resource_status',
                        'idempotency_state'
                    )
                    """
                )
            )

            return {row[0] for row in rows}
    finally:
        engine.dispose()


def database_table_names() -> set[str]:
    settings = get_settings()
    engine = create_engine(settings.postgres_migration_dsn)

    try:
        inspector = inspect(engine)
        return set(inspector.get_table_names())
    finally:
        engine.dispose()


def test_migration_upgrade_downgrade_upgrade_round_trip():
    settings = get_settings()

    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")

    config = alembic_config()

    try:
        command.upgrade(config, "head")

        assert EXPECTED_TABLES <= database_table_names()
        assert EXPECTED_ENUMS <= database_enum_names()

        command.downgrade(config, "base")

        assert database_enum_names().isdisjoint(EXPECTED_ENUMS)

        remaining_tables = database_table_names()

        assert "tenants" not in remaining_tables
        assert "admin_users" not in remaining_tables
        assert "admin_tokens" not in remaining_tables
        assert "applications" not in remaining_tables
        assert "api_keys" not in remaining_tables
        assert "idempotency_records" not in remaining_tables

        command.upgrade(config, "head")

        assert EXPECTED_TABLES <= database_table_names()
        assert EXPECTED_ENUMS <= database_enum_names()

    finally:
        # The developer database must be left at head even if
        # an assertion in this test fails.
        command.upgrade(config, "head")
