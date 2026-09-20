from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert, select

from app.control_plane.permissions import (
    PermissionAdminService,
    PermissionApiKeyNotFoundError,
    PermissionConflictError,
    PermissionTargetNotFoundError,
)
from app.core.config import get_settings
from app.persistence.models import Base
from app.schemas.control_plane import ApiKeyPermissionWrite


@pytest.fixture
def permission_fixture():
    engine = create_engine(get_settings().postgres_migration_dsn)
    now = datetime.now(UTC)
    tenant_a, tenant_b = uuid4(), uuid4()
    admin_a, admin_b = uuid4(), uuid4()
    app_a, app_b = uuid4(), uuid4()
    key_a, key_b = uuid4(), uuid4()
    ids = {
        "SERVICE": (uuid4(), uuid4()),
        "ROUTE": (uuid4(), uuid4()),
        "LLM_ALIAS": (uuid4(), uuid4()),
        "LLM_MODEL": (uuid4(), uuid4()),
    }
    provider_a, provider_b = uuid4(), uuid4()
    target_a, target_b = uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]),
            [
                {
                    "id": tenant_a,
                    "name": f"m25-{tenant_a}",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "id": tenant_b,
                    "name": f"m25-{tenant_b}",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["admin_users"]),
            [
                {
                    "id": admin_a,
                    "tenant_id": tenant_a,
                    "name": "m25-admin-a",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "id": admin_b,
                    "tenant_id": tenant_b,
                    "name": "m25-admin-b",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["applications"]),
            [
                {
                    "id": app_a,
                    "tenant_id": tenant_a,
                    "name": "m25-app-a",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "id": app_b,
                    "tenant_id": tenant_b,
                    "name": "m25-app-b",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["api_keys"]),
            [
                {
                    "id": key_a,
                    "tenant_id": tenant_a,
                    "application_id": app_a,
                    "name": "m25-key-a",
                    "key_prefix": "gw_m25aaaaaa",
                    "key_hash": f"m25-{key_a}",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "id": key_b,
                    "tenant_id": tenant_b,
                    "application_id": app_b,
                    "name": "m25-key-b",
                    "key_prefix": "gw_m25bbbbbb",
                    "key_hash": f"m25-{key_b}",
                    "status": "ACTIVE",
                    "created_at": now,
                    "updated_at": now,
                },
            ],
        )
        services = Base.metadata.tables["normal_api_services"]
        for tenant, service_id, suffix in (
            (tenant_a, ids["SERVICE"][0], "a"),
            (tenant_b, ids["SERVICE"][1], "b"),
        ):
            connection.execute(
                insert(services).values(
                    id=service_id,
                    tenant_id=tenant,
                    slug=f"m25-{suffix}",
                    display_name="M25",
                    upstream_base_url="https://example.test",
                    status="ACTIVE",
                    request_body_limit_bytes=1024,
                    connect_timeout_seconds=5,
                    pool_timeout_seconds=5,
                    write_timeout_seconds=30,
                    read_idle_timeout_seconds=60,
                    pre_response_timeout_seconds=120,
                    created_at=now,
                    updated_at=now,
                )
            )
        routes = Base.metadata.tables["normal_api_routes"]
        for tenant, route_id, service_id in (
            (tenant_a, ids["ROUTE"][0], ids["SERVICE"][0]),
            (tenant_b, ids["ROUTE"][1], ids["SERVICE"][1]),
        ):
            connection.execute(
                insert(routes).values(
                    id=route_id,
                    tenant_id=tenant,
                    service_id=service_id,
                    path_pattern="/m25",
                    method="GET",
                    upstream_path_template="/m25",
                    priority=1,
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
        providers = Base.metadata.tables["llm_providers"]
        targets = Base.metadata.tables["llm_provider_targets"]
        models = Base.metadata.tables["llm_models"]
        aliases = Base.metadata.tables["llm_aliases"]
        for tenant, provider_id, target_id, model_id, alias_id, suffix in (
            (
                tenant_a,
                provider_a,
                target_a,
                ids["LLM_MODEL"][0],
                ids["LLM_ALIAS"][0],
                "a",
            ),
            (
                tenant_b,
                provider_b,
                target_b,
                ids["LLM_MODEL"][1],
                ids["LLM_ALIAS"][1],
                "b",
            ),
        ):
            connection.execute(
                insert(providers).values(
                    id=provider_id,
                    tenant_id=tenant,
                    name=f"m25-provider-{suffix}",
                    provider_type="OPENAI",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(targets).values(
                    id=target_id,
                    tenant_id=tenant,
                    provider_id=provider_id,
                    name=f"m25-target-{suffix}",
                    base_url="https://llm.example.test",
                    timeout_ms=30000,
                    pre_output_idle_timeout_ms=20000,
                    pre_output_budget_ms=30000,
                    post_output_idle_timeout_ms=60000,
                    status="ACTIVE",
                    certification_status="UNVERIFIED",
                    allow_uncertified_runtime=False,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(models).values(
                    id=model_id,
                    tenant_id=tenant,
                    provider_target_id=target_id,
                    provider_model_name=f"m25-model-{suffix}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(aliases).values(
                    id=alias_id,
                    tenant_id=tenant,
                    name=f"m25-alias-{suffix}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
    yield {
        "engine": engine,
        "tenant_a": tenant_a,
        "tenant_b": tenant_b,
        "key_a": key_a,
        "key_b": key_b,
        "ids": ids,
    }
    with engine.begin() as connection:
        for table_name in (
            "api_key_permissions",
            "llm_aliases",
            "llm_models",
            "llm_provider_targets",
            "llm_providers",
            "normal_api_routes",
            "normal_api_services",
            "api_keys",
            "applications",
            "admin_users",
        ):
            table = Base.metadata.tables[table_name]
            connection.execute(
                table.delete().where(table.c.tenant_id.in_([tenant_a, tenant_b]))
            )
        connection.execute(
            Base.metadata.tables["tenants"]
            .delete()
            .where(Base.metadata.tables["tenants"].c.id.in_([tenant_a, tenant_b]))
        )
    engine.dispose()


def _permission(resource_type, resource_id):
    return ApiKeyPermissionWrite(
        resource_type=resource_type, resource_id=resource_id, action="INVOKE"
    )


def test_all_frozen_resource_types_validate_and_replace_atomically(permission_fixture):
    service = PermissionAdminService()
    desired = [
        _permission(resource_type, pair[0])
        for resource_type, pair in permission_fixture["ids"].items()
    ]
    with permission_fixture["engine"].begin() as connection:
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=desired,
        )
        result = service.list(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
        )
    assert {
        (item.resource_type, item.resource_id, item.action) for item in result.data
    } == {(item.resource_type, item.resource_id, item.action) for item in desired}


def test_foreign_missing_and_mismatched_targets_are_hidden(permission_fixture):
    service = PermissionAdminService()
    invalid = [
        _permission("SERVICE", permission_fixture["ids"]["SERVICE"][1]),
        _permission("SERVICE", uuid4()),
        _permission("SERVICE", permission_fixture["ids"]["ROUTE"][0]),
        _permission("LLM_ALIAS", permission_fixture["ids"]["LLM_MODEL"][0]),
    ]
    for permission in invalid:
        with (
            permission_fixture["engine"].begin() as connection,
            pytest.raises(PermissionTargetNotFoundError),
        ):
            service.replace(
                connection,
                tenant_id=permission_fixture["tenant_a"],
                api_key_id=permission_fixture["key_a"],
                permissions=[permission],
            )


def test_foreign_and_missing_api_keys_are_hidden(permission_fixture):
    service = PermissionAdminService()
    for key_id in (permission_fixture["key_b"], uuid4()):
        with (
            permission_fixture["engine"].begin() as connection,
            pytest.raises(PermissionApiKeyNotFoundError),
        ):
            service.list(
                connection, tenant_id=permission_fixture["tenant_a"], api_key_id=key_id
            )


def test_duplicate_assignment_conflicts_without_duplicate_row(permission_fixture):
    service = PermissionAdminService()
    permission = _permission("SERVICE", permission_fixture["ids"]["SERVICE"][0])
    with (
        permission_fixture["engine"].begin() as connection,
        pytest.raises(PermissionConflictError),
    ):
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[permission, permission],
        )
    with permission_fixture["engine"].connect() as connection:
        assert (
            connection.scalar(select(Base.metadata.tables["api_key_permissions"].c.id))
            is None
        )


def test_replace_removes_only_target_key_permissions_and_empty_clears(
    permission_fixture,
):
    service = PermissionAdminService()
    service_permission = _permission("SERVICE", permission_fixture["ids"]["SERVICE"][0])
    route_permission = _permission("ROUTE", permission_fixture["ids"]["ROUTE"][0])
    with permission_fixture["engine"].begin() as connection:
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[service_permission, route_permission],
        )
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[route_permission],
        )
        result = service.list(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
        )
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[],
        )
        cleared = service.list(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
        )
    assert [(item.resource_type, item.resource_id) for item in result.data] == [
        ("ROUTE", route_permission.resource_id)
    ]
    assert cleared.data == []


def test_invalid_replacement_preserves_existing_set(permission_fixture):
    service = PermissionAdminService()
    valid = _permission("SERVICE", permission_fixture["ids"]["SERVICE"][0])
    with permission_fixture["engine"].begin() as connection:
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[valid],
        )
    with (
        permission_fixture["engine"].begin() as connection,
        pytest.raises(PermissionTargetNotFoundError),
    ):
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[_permission("ROUTE", uuid4())],
        )
    with permission_fixture["engine"].connect() as connection:
        result = service.list(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
        )
    assert len(result.data) == 1
    assert result.data[0].resource_id == valid.resource_id


def test_outer_transaction_rollback_restores_previous_permission_set(
    permission_fixture,
):
    service = PermissionAdminService()
    original = _permission("SERVICE", permission_fixture["ids"]["SERVICE"][0])
    replacement = _permission("ROUTE", permission_fixture["ids"]["ROUTE"][0])
    with permission_fixture["engine"].begin() as connection:
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[original],
        )

    connection = permission_fixture["engine"].connect()
    transaction = connection.begin()
    try:
        service.replace(
            connection,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
            permissions=[replacement],
        )
        transaction.rollback()
    finally:
        if transaction.is_active:
            transaction.rollback()
        connection.close()

    with permission_fixture["engine"].connect() as check:
        result = service.list(
            check,
            tenant_id=permission_fixture["tenant_a"],
            api_key_id=permission_fixture["key_a"],
        )
    assert len(result.data) == 1
    assert result.data[0].resource_id == original.resource_id
