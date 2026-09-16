from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import uuid_utils.compat as uuid_utils
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.persistence.models import Base


@pytest.fixture
def db_connection():
    engine = create_engine(get_settings().postgres_migration_dsn)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()
    engine.dispose()


def now():
    return datetime.now(UTC)


def add_tenant(connection, name):
    tenant_id, timestamp = uuid4(), now()
    connection.execute(
        insert(Base.metadata.tables["tenants"]).values(
            id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return tenant_id


def add_application_and_key(connection, tenant_id, suffix):
    application_id, key_id, timestamp = uuid4(), uuid4(), now()
    connection.execute(
        insert(Base.metadata.tables["applications"]).values(
            id=application_id,
            tenant_id=tenant_id,
            name=f"app-{suffix}",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["api_keys"]).values(
            id=key_id,
            tenant_id=tenant_id,
            application_id=application_id,
            name=f"key-{suffix}",
            key_prefix=f"gw_{suffix}"[:32],
            key_hash=f"hash-{suffix}-{uuid4()}",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return application_id, key_id


def add_service(connection, tenant_id, suffix):
    service_id, timestamp = uuid4(), now()
    slug_suffix = suffix.replace("_", "-")
    connection.execute(
        insert(Base.metadata.tables["normal_api_services"]).values(
            id=service_id,
            tenant_id=tenant_id,
            slug=f"service-{slug_suffix}",
            display_name="Service",
            upstream_base_url="https://service.example.test",
            status="ACTIVE",
            request_body_limit_bytes=1024,
            connect_timeout_seconds=5,
            pool_timeout_seconds=5,
            write_timeout_seconds=30,
            read_idle_timeout_seconds=60,
            pre_response_timeout_seconds=120,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return service_id


def test_m15_contract_objects_are_exposed_by_model_metadata():
    permissions = Base.metadata.tables["api_key_permissions"]
    credentials = Base.metadata.tables["service_credentials"]
    routes = Base.metadata.tables["normal_api_routes"]

    assert {
        "tenant_id",
        "api_key_id",
        "resource_type",
        "resource_id",
        "action",
    } <= set(permissions.c.keys())
    assert {
        "tenant_id",
        "service_id",
        "auth_type",
        "secret_ciphertext",
        "header_name",
        "key_version",
        "status",
        "rotated_at",
    } <= set(credentials.c.keys())
    assert {
        "upstream_path_template",
        "priority",
        "timeout_ms",
        "header_policy",
    } <= set(routes.c.keys())


def test_m15_credentials_expose_only_hashed_or_encrypted_secret_material():
    admin_tokens = Base.metadata.tables["admin_tokens"]
    api_keys = Base.metadata.tables["api_keys"]
    service_credentials = Base.metadata.tables["service_credentials"]

    assert {"token_hash", "token_prefix"} <= set(admin_tokens.c.keys())
    assert {"key_hash", "key_prefix", "name"} <= set(api_keys.c.keys())
    assert "secret_ciphertext" in service_credentials.c

    forbidden = {
        "token",
        "api_key",
        "secret",
        "bearer_token",
        "password",
        "authorization_header",
    }
    assert forbidden.isdisjoint(admin_tokens.c.keys())
    assert forbidden.isdisjoint(api_keys.c.keys())
    assert forbidden.isdisjoint(service_credentials.c.keys())


def test_permission_duplicate_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "permission-duplicate")
    _, key_id = add_application_and_key(db_connection, tenant_id, "permission")
    permission = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "api_key_id": key_id,
        "resource_type": "SERVICE",
        "resource_id": uuid4(),
        "action": "INVOKE",
        "created_at": now(),
    }
    table = Base.metadata.tables["api_key_permissions"]
    db_connection.execute(insert(table).values(**permission))
    permission["id"] = uuid4()
    with pytest.raises(IntegrityError):
        db_connection.execute(insert(table).values(**permission))


def test_permission_cross_tenant_api_key_is_rejected(db_connection):
    tenant_a = add_tenant(db_connection, "permission-tenant-a")
    tenant_b = add_tenant(db_connection, "permission-tenant-b")
    _, key_id = add_application_and_key(db_connection, tenant_a, "cross")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["api_key_permissions"]).values(
                id=uuid4(),
                tenant_id=tenant_b,
                api_key_id=key_id,
                resource_type="ROUTE",
                resource_id=uuid4(),
                action="INVOKE",
                created_at=now(),
            )
        )


def test_only_one_active_service_credential_is_allowed(db_connection):
    tenant_id = add_tenant(db_connection, "credential-active")
    service_id = add_service(db_connection, tenant_id, "credential-active")
    table = Base.metadata.tables["service_credentials"]
    values = {
        "tenant_id": tenant_id,
        "service_id": service_id,
        "auth_type": "STATIC_BEARER",
        "secret_ciphertext": b"encrypted-value",
        "key_version": 1,
        "status": "ACTIVE",
        "created_at": now(),
    }
    db_connection.execute(insert(table).values(id=uuid4(), **values))
    with pytest.raises(IntegrityError):
        db_connection.execute(insert(table).values(id=uuid4(), **values))


def test_service_credential_cross_tenant_service_is_rejected(db_connection):
    tenant_a = add_tenant(db_connection, "credential-tenant-a")
    tenant_b = add_tenant(db_connection, "credential-tenant-b")
    service_id = add_service(db_connection, tenant_a, "credential-cross")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["service_credentials"]).values(
                id=uuid4(),
                tenant_id=tenant_b,
                service_id=service_id,
                auth_type="NONE",
                status="ACTIVE",
                created_at=now(),
            )
        )


def test_disabled_service_credential_history_is_allowed(db_connection):
    tenant_id = add_tenant(db_connection, "credential-history")
    service_id = add_service(db_connection, tenant_id, "credential-history")
    table = Base.metadata.tables["service_credentials"]
    values = {
        "tenant_id": tenant_id,
        "service_id": service_id,
        "auth_type": "STATIC_HEADER",
        "header_name": "X-Upstream-Key",
        "secret_ciphertext": b"encrypted-value",
        "key_version": 2,
        "status": "DISABLED",
        "created_at": now(),
    }
    db_connection.execute(
        insert(table), [{"id": uuid4(), **values}, {"id": uuid4(), **values}]
    )


@pytest.mark.parametrize(
    "auth_type,header_name,secret_ciphertext,key_version",
    [
        ("NONE", None, b"unexpected", None),
        ("STATIC_BEARER", "Authorization", b"encrypted", 1),
        ("STATIC_HEADER", None, b"encrypted", 1),
    ],
)
def test_invalid_service_credential_shapes_are_rejected(
    db_connection, auth_type, header_name, secret_ciphertext, key_version
):
    tenant_id = add_tenant(db_connection, f"credential-shape-{auth_type.lower()}")
    service_id = add_service(db_connection, tenant_id, f"shape-{auth_type.lower()}")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["service_credentials"]).values(
                id=uuid4(),
                tenant_id=tenant_id,
                service_id=service_id,
                auth_type=auth_type,
                header_name=header_name,
                secret_ciphertext=secret_ciphertext,
                key_version=key_version,
                status="ACTIVE",
                created_at=now(),
            )
        )


def test_service_delete_is_restricted_by_route(db_connection):
    tenant_id = add_tenant(db_connection, "service-restrict")
    service_id = add_service(db_connection, tenant_id, "restrict")
    timestamp = now()
    db_connection.execute(
        insert(Base.metadata.tables["normal_api_routes"]).values(
            id=uuid4(),
            tenant_id=tenant_id,
            service_id=service_id,
            path_pattern="/claims",
            method="GET",
            upstream_path_template="/upstream/claims",
            priority=0,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            delete(Base.metadata.tables["normal_api_services"]).where(
                Base.metadata.tables["normal_api_services"].c.id == service_id
            )
        )


@pytest.mark.parametrize("priority,timeout_ms", [(-1, None), (0, 0), (0, -1)])
def test_invalid_route_priority_or_timeout_is_rejected(
    db_connection, priority, timeout_ms
):
    tenant_id = add_tenant(db_connection, f"route-limits-{priority}-{timeout_ms}")
    service_id = add_service(db_connection, tenant_id, f"limits-{uuid4().hex[:8]}")
    timestamp = now()
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["normal_api_routes"]).values(
                id=uuid4(),
                tenant_id=tenant_id,
                service_id=service_id,
                path_pattern="/limits",
                method="GET",
                upstream_path_template="/limits",
                priority=priority,
                timeout_ms=timeout_ms,
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )


@pytest.mark.parametrize(
    "status,revoked_at",
    [("REVOKED", None), ("ACTIVE", datetime.now(UTC))],
)
def test_api_key_revocation_state_is_consistent(db_connection, status, revoked_at):
    tenant_id = add_tenant(db_connection, f"key-state-{status}-{revoked_at is None}")
    application_id, _ = add_application_and_key(db_connection, tenant_id, "existing")
    timestamp = now()
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["api_keys"]).values(
                id=uuid4(),
                tenant_id=tenant_id,
                application_id=application_id,
                name="invalid-state",
                key_prefix="gw_invalid",
                key_hash=f"hash-{uuid4()}",
                status=status,
                revoked_at=revoked_at,
                expires_at=timestamp + timedelta(days=1),
                created_at=timestamp,
                updated_at=timestamp,
            )
        )


def test_revoked_admin_token_and_api_key_are_persistable(db_connection):
    tenant_id = add_tenant(db_connection, "revoked-principals")
    application_id, _ = add_application_and_key(db_connection, tenant_id, "active")
    timestamp = now()
    admin_user_id = uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=admin_user_id,
            tenant_id=tenant_id,
            name="admin",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    db_connection.execute(
        insert(Base.metadata.tables["admin_tokens"]).values(
            id=uuid4(),
            tenant_id=tenant_id,
            admin_user_id=admin_user_id,
            token_prefix="adm_revoked",
            token_hash=f"hash-{uuid4()}",
            status="REVOKED",
            revoked_at=timestamp,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    db_connection.execute(
        insert(Base.metadata.tables["api_keys"]).values(
            id=uuid4(),
            tenant_id=tenant_id,
            application_id=application_id,
            name="revoked",
            key_prefix="gw_revoked",
            key_hash=f"hash-{uuid4()}",
            status="REVOKED",
            revoked_at=timestamp,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )


def test_request_application_and_api_key_mismatch_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "request-attribution")
    application_a, key_id = add_application_and_key(db_connection, tenant_id, "key-a")
    application_b, _ = add_application_and_key(db_connection, tenant_id, "key-b")
    assert application_a != application_b
    timestamp = now()
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["requests"]).values(
                id=uuid_utils.uuid7(),
                tenant_id=tenant_id,
                request_id=uuid_utils.uuid7(),
                application_id=application_b,
                api_key_id=key_id,
                workload_type="LLM",
                method="POST",
                started_at=timestamp,
                completed_at=timestamp,
                latency_ms=0,
                degraded_mode=False,
            )
        )


def test_llm_extension_of_normal_request_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "llm-extension-normal")
    service_id = add_service(db_connection, tenant_id, "llm-extension")
    timestamp = now()
    route_id = uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["normal_api_routes"]).values(
            id=route_id,
            tenant_id=tenant_id,
            service_id=service_id,
            path_pattern="/normal",
            method="POST",
            upstream_path_template="/normal",
            priority=0,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    request_id = uuid_utils.uuid7()
    db_connection.execute(
        insert(Base.metadata.tables["requests"]).values(
            id=request_id,
            tenant_id=tenant_id,
            request_id=uuid_utils.uuid7(),
            workload_type="NORMAL",
            route_id=route_id,
            method="POST",
            started_at=timestamp,
            degraded_mode=False,
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_requests"]).values(
                id=uuid_utils.uuid7(),
                tenant_id=tenant_id,
                request_id_fk=request_id,
                requested_model="model",
                stream=False,
                tool_calling=False,
                attempt_count=0,
                cost_complete=False,
                output_committed=False,
            )
        )


def test_audit_admin_user_and_token_mismatch_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "audit-actor-mismatch")
    timestamp = now()
    users = [uuid4(), uuid4()]
    db_connection.execute(
        insert(Base.metadata.tables["admin_users"]),
        [
            {
                "id": user_id,
                "tenant_id": tenant_id,
                "name": f"admin-{index}",
                "status": "ACTIVE",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            for index, user_id in enumerate(users)
        ],
    )
    token_id = uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["admin_tokens"]).values(
            id=token_id,
            tenant_id=tenant_id,
            admin_user_id=users[0],
            token_prefix="adm_actor",
            token_hash=f"hash-{uuid4()}",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["audit_logs"]).values(
                id=uuid4(),
                tenant_id=tenant_id,
                actor_admin_user_id=users[1],
                actor_admin_token_id=token_id,
                action="service.update",
                resource_type="SERVICE",
                result="SUCCESS",
                created_at=timestamp,
            )
        )


def test_llm_attempt_target_and_model_mismatch_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "attempt-attribution")
    timestamp = now()
    provider_id = uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["llm_providers"]).values(
            id=provider_id,
            tenant_id=tenant_id,
            name="provider",
            provider_type="OPENAI",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    targets = [uuid4(), uuid4()]
    for index, target_id in enumerate(targets):
        db_connection.execute(
            insert(Base.metadata.tables["llm_provider_targets"]).values(
                id=target_id,
                tenant_id=tenant_id,
                provider_id=provider_id,
                name=f"target-{index}",
                base_url="https://provider.example.test",
                timeout_ms=30000,
                pre_output_idle_timeout_ms=20000,
                pre_output_budget_ms=30000,
                post_output_idle_timeout_ms=60000,
                status="ACTIVE",
                certification_status="CERTIFIED",
                allow_uncertified_runtime=False,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    model_id = uuid4()
    db_connection.execute(
        insert(Base.metadata.tables["llm_models"]).values(
            id=model_id,
            tenant_id=tenant_id,
            provider_target_id=targets[0],
            provider_model_name="model",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    request_id = uuid_utils.uuid7()
    db_connection.execute(
        insert(Base.metadata.tables["requests"]).values(
            id=request_id,
            tenant_id=tenant_id,
            request_id=uuid_utils.uuid7(),
            workload_type="LLM",
            method="POST",
            started_at=timestamp,
            degraded_mode=False,
        )
    )
    llm_request_id = uuid_utils.uuid7()
    db_connection.execute(
        insert(Base.metadata.tables["llm_requests"]).values(
            id=llm_request_id,
            tenant_id=tenant_id,
            request_id_fk=request_id,
            requested_model="model",
            stream=True,
            tool_calling=False,
            attempt_count=1,
            cost_complete=False,
            output_committed=False,
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_attempts"]).values(
                id=uuid_utils.uuid7(),
                tenant_id=tenant_id,
                llm_request_id=llm_request_id,
                attempt_no=1,
                provider_target_id=targets[1],
                model_id=model_id,
                started_at=timestamp,
                status="FAILED",
                first_output_committed=False,
                provider_midstream_failure=False,
            )
        )
