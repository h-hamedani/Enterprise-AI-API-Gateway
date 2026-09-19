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
    "api_key_permissions",
    "applications",
    "idempotency_records",
    "llm_alias_targets",
    "llm_aliases",
    "llm_model_capabilities",
    "llm_models",
    "llm_provider_credentials",
    "llm_provider_targets",
    "llm_providers",
    "model_prices",
    "normal_api_routes",
    "normal_api_services",
    "service_credentials",
    "tenants",
    "rate_limit_policies",
    "requests",
    "llm_requests",
    "llm_attempts",
    "audit_logs",
    "config_versions",
    "retention_checkpoints",
}

EXPECTED_ENUMS = {
    "certification_status",
    "idempotency_state",
    "model_capability",
    "provider_type",
    "resource_status",
    "rate_scope_type",
    "workload_type",
    "llm_final_status",
    "attempt_status",
    "audit_result",
    "retention_state",
    "retention_job_type",
    "principal_status",
    "permission_resource_type",
    "permission_action",
    "service_auth_type",
}

M13_TABLES = {
    "llm_alias_targets",
    "llm_aliases",
    "llm_model_capabilities",
    "llm_models",
    "llm_provider_credentials",
    "llm_provider_targets",
    "llm_providers",
    "model_prices",
}

M13_ENUMS = {"certification_status", "model_capability", "provider_type"}

M14_TABLES = {
    "rate_limit_policies",
    "requests",
    "llm_requests",
    "llm_attempts",
    "audit_logs",
    "config_versions",
    "retention_checkpoints",
}

M14_ENUMS = {
    "rate_scope_type",
    "workload_type",
    "llm_final_status",
    "attempt_status",
    "audit_result",
    "retention_state",
    "retention_job_type",
}

M15_TABLES = {"api_key_permissions", "service_credentials"}
M15_ENUMS = {
    "principal_status",
    "permission_resource_type",
    "permission_action",
    "service_auth_type",
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
                        'resource_status', 'idempotency_state',
                        'provider_type', 'certification_status',
                        'model_capability', 'rate_scope_type',
                        'workload_type', 'llm_final_status',
                        'attempt_status', 'audit_result',
                        'retention_state', 'retention_job_type'
                        , 'principal_status', 'permission_resource_type',
                        'permission_action', 'service_auth_type'
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


def test_m13_targeted_downgrade_preserves_baseline_and_reupgrades():
    settings = get_settings()

    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")

    config = alembic_config()

    try:
        command.upgrade(config, "head")
        command.downgrade(config, "d1e0310898aa")

        tables = database_table_names()
        enums = database_enum_names()
        assert tables.isdisjoint(M13_TABLES)
        assert enums.isdisjoint(M13_ENUMS)
        assert {
            "tenants",
            "normal_api_services",
            "normal_api_routes",
        } <= tables
        assert "resource_status" in enums

        command.upgrade(config, "head")
        assert M13_TABLES <= database_table_names()
        assert M13_ENUMS <= database_enum_names()
    finally:
        command.upgrade(config, "head")


def test_m14_targeted_downgrade_preserves_m13_and_reupgrades():
    settings = get_settings()
    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")

    config = alembic_config()
    try:
        command.upgrade(config, "head")
        command.downgrade(config, "a9c4e7f12b36")

        tables = database_table_names()
        enums = database_enum_names()
        assert tables.isdisjoint(M14_TABLES)
        assert enums.isdisjoint(M14_ENUMS)
        assert M13_TABLES <= tables
        assert M13_ENUMS <= enums
        assert "resource_status" in enums

        engine = create_engine(settings.postgres_migration_dsn)
        try:
            inspector = inspect(engine)
            for table_name, constraint_name in (
                ("api_keys", "uq_api_keys_tenant_id"),
                ("admin_tokens", "uq_admin_tokens_tenant_id"),
                ("normal_api_routes", "uq_normal_api_routes_tenant_id"),
            ):
                names = {
                    item["name"]
                    for item in inspector.get_unique_constraints(table_name)
                }
                assert constraint_name not in names
        finally:
            engine.dispose()

        command.upgrade(config, "head")
        assert M14_TABLES <= database_table_names()
        assert M14_ENUMS <= database_enum_names()
    finally:
        command.upgrade(config, "head")


def test_m15_targeted_downgrade_preserves_m14_and_reupgrades():
    settings = get_settings()
    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")

    config = alembic_config()
    try:
        command.upgrade(config, "head")
        command.downgrade(config, "c7d8e9f0a1b2")

        tables = database_table_names()
        enums = database_enum_names()
        assert tables.isdisjoint(M15_TABLES)
        assert enums.isdisjoint(M15_ENUMS)
        assert M14_TABLES <= tables
        assert M14_ENUMS <= enums

        inspector = inspect(create_engine(settings.postgres_migration_dsn))
        route_columns = {
            column["name"] for column in inspector.get_columns("normal_api_routes")
        }
        assert {
            "upstream_path_template",
            "priority",
            "timeout_ms",
            "header_policy",
        }.isdisjoint(route_columns)

        command.upgrade(config, "head")
        assert M15_TABLES <= database_table_names()
        assert M15_ENUMS <= database_enum_names()
    finally:
        command.upgrade(config, "head")


def test_m16_targeted_downgrade_restores_m15_idempotency_schema():
    settings = get_settings()
    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")
    config = alembic_config()
    try:
        command.upgrade(config, "head")
        command.downgrade(config, "f3b6a1c9d2e4")
        engine = create_engine(settings.postgres_migration_dsn)
        try:
            columns = {
                column["name"]
                for column in inspect(engine).get_columns("idempotency_records")
            }
            assert "idempotency_key" in columns
            assert "idempotency_key_hash" not in columns
        finally:
            engine.dispose()
        command.upgrade(config, "head")
        engine = create_engine(settings.postgres_migration_dsn)
        try:
            columns = {
                column["name"]
                for column in inspect(engine).get_columns("idempotency_records")
            }
            assert "idempotency_key_hash" in columns
            assert "idempotency_key" not in columns
        finally:
            engine.dispose()
    finally:
        command.upgrade(config, "head")


def test_m17_targeted_downgrade_restores_m16_audit_grant():
    settings = get_settings()
    if settings.environment == "production":
        raise RuntimeError("Migration integration tests must never run in production.")
    config = alembic_config()
    engine = create_engine(settings.postgres_migration_dsn)
    try:
        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert (
                connection.scalar(
                    text(
                        "SELECT has_table_privilege("
                        "'security_operations', 'audit_logs', 'UPDATE')"
                    )
                )
                is False
            )

        command.downgrade(config, "b7e2c4d6f8a0")
        with engine.connect() as connection:
            assert (
                connection.scalar(
                    text(
                        "SELECT has_table_privilege("
                        "'security_operations', 'audit_logs', 'UPDATE')"
                    )
                )
                is True
            )

        command.upgrade(config, "head")
    finally:
        engine.dispose()
        command.upgrade(config, "head")
