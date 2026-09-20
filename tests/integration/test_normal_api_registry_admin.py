from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from app.control_plane.normal_api_registry import (
    RegistryResourceNotFoundError,
    create_normal_api_registry_service,
)
from app.core.config import get_settings
from app.core.security.secrets import EncryptedSecret, SecretService
from app.persistence.models import Base
from app.schemas.control_plane import (
    RouteCreate,
    RoutePatch,
    ServiceCreate,
    ServiceCredentialWrite,
    ServicePatch,
)


@pytest.fixture
def registry_fixture():
    from sqlalchemy import create_engine

    engine = create_engine(get_settings().postgres_migration_dsn)
    connection = engine.connect()
    transaction = connection.begin()
    now = datetime.now(UTC)
    tenant_a, tenant_b = uuid4(), uuid4()
    admin_a = uuid4()
    connection.execute(
        insert(Base.metadata.tables["tenants"]),
        [
            {
                "id": tenant_a,
                "name": f"m26-{tenant_a}",
                "status": "ACTIVE",
                "created_at": now,
                "updated_at": now,
            },
            {
                "id": tenant_b,
                "name": f"m26-{tenant_b}",
                "status": "ACTIVE",
                "created_at": now,
                "updated_at": now,
            },
        ],
    )
    connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=admin_a,
            tenant_id=tenant_a,
            name="m26-admin",
            status="ACTIVE",
            created_at=now,
            updated_at=now,
        )
    )
    encryption_key = b"E" * 32
    registry = create_normal_api_registry_service(
        idempotency_hmac_key=b"I" * 32,
        encryption_keys={1: encryption_key},
        current_encryption_key_version=1,
    )
    try:
        yield connection, registry, tenant_a, tenant_b, admin_a, encryption_key
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


def _create_service(connection, registry, tenant_id, slug="orders"):
    result = registry.create_service(
        connection,
        tenant_id=tenant_id,
        admin_user_id=uuid4(),
        request=ServiceCreate(
            name=slug,
            base_url=f"https://{slug}.example",
            slug=slug,
        ),
        raw_idempotency_key=None,
    )
    import json

    return UUID(json.loads(result.response.body)["id"])


def test_service_tenant_isolation_defaults_uniqueness_and_pagination(
    registry_fixture,
) -> None:
    connection, registry, tenant_a, tenant_b, _, _ = registry_fixture
    first_id = _create_service(connection, registry, tenant_a, "orders")
    second_id = _create_service(connection, registry, tenant_a, "billing")
    _create_service(connection, registry, tenant_b, "orders")

    service = registry.get_service(connection, tenant_id=tenant_a, service_id=first_id)
    assert (
        service.connect_timeout_seconds,
        service.pool_timeout_seconds,
        service.write_timeout_seconds,
        service.read_idle_timeout_seconds,
        service.pre_response_timeout_seconds,
    ) == (5, 5, 30, 60, 120)
    with pytest.raises(RegistryResourceNotFoundError):
        registry.get_service(connection, tenant_id=tenant_b, service_id=first_id)

    page = registry.list_services(connection, tenant_id=tenant_a, limit=1, cursor=None)
    assert len(page.data) == 1
    assert page.next_cursor is not None
    next_page = registry.list_services(
        connection, tenant_id=tenant_a, limit=1, cursor=page.next_cursor
    )
    assert {page.data[0].id, next_page.data[0].id} == {first_id, second_id}
    with pytest.raises(ValueError, match="Invalid pagination cursor"):
        registry.list_services(
            connection, tenant_id=tenant_b, limit=1, cursor=page.next_cursor
        )

    updated = registry.patch_service(
        connection,
        tenant_id=tenant_a,
        service_id=first_id,
        patch=ServicePatch(connect_timeout_seconds=9),
    )
    assert updated.connect_timeout_seconds == 9

    with pytest.raises(IntegrityError), connection.begin_nested():
        _create_service(connection, registry, tenant_a, "orders")


def test_method_specific_route_identity_relationship_and_patch(
    registry_fixture,
) -> None:
    connection, registry, tenant_a, tenant_b, _, _ = registry_fixture
    service_id = _create_service(connection, registry, tenant_a)
    route = registry.create_route(
        connection,
        tenant_id=tenant_a,
        request=RouteCreate(
            service_id=service_id,
            path_pattern="/orders/{id}",
            method="GET",
            upstream_path_template="/v1/orders/{id}",
            header_policy={"mode": "metadata-only"},
            priority=0,
        ),
    )
    assert route.method == "GET"
    assert route.upstream_path_template == "/v1/orders/{id}"
    assert route.header_policy == {"mode": "metadata-only"}

    patched = registry.patch_route(
        connection,
        tenant_id=tenant_a,
        route_id=route.id,
        patch=RoutePatch(method="POST", header_policy=None),
    )
    assert patched.id == route.id
    assert patched.method == "POST"
    assert patched.header_policy is None
    with pytest.raises(RegistryResourceNotFoundError):
        registry.get_route(connection, tenant_id=tenant_b, route_id=route.id)
    with pytest.raises(RegistryResourceNotFoundError):
        registry.create_route(
            connection,
            tenant_id=tenant_b,
            request=RouteCreate(
                service_id=service_id,
                path_pattern="/foreign",
                method="GET",
                upstream_path_template="/foreign",
                priority=0,
            ),
        )


def test_credential_encryption_rotation_none_and_failure_safety(
    registry_fixture, monkeypatch
) -> None:
    connection, registry, tenant_a, _, _, encryption_key = registry_fixture
    service_id = _create_service(connection, registry, tenant_a)
    first = registry.put_credential(
        connection,
        tenant_id=tenant_a,
        admin_user_id=uuid4(),
        service_id=service_id,
        request=ServiceCredentialWrite(
            auth_type="STATIC_HEADER", secret="plain-secret", header_name="X-Key"
        ),
        raw_idempotency_key=None,
    )
    import json

    first_id = UUID(json.loads(first.response.body)["id"])
    table = Base.metadata.tables["service_credentials"]
    row = (
        connection.execute(select(table).where(table.c.id == first_id)).mappings().one()
    )
    assert row["secret_ciphertext"] != b"plain-secret"
    assert b"plain-secret" not in row["secret_ciphertext"]
    decryptor = SecretService({1: encryption_key}, current_key_version=1)
    assert (
        decryptor.decrypt(
            EncryptedSecret(row["key_version"], row["secret_ciphertext"]),
            aad=registry.credential_aad(
                tenant_a, service_id, first_id, "STATIC_HEADER"
            ),
        )
        == b"plain-secret"
    )
    assert b"plain-secret" not in first.response.body
    assert b"secret_ciphertext" not in first.response.body

    registry.put_credential(
        connection,
        tenant_id=tenant_a,
        admin_user_id=uuid4(),
        service_id=service_id,
        request=ServiceCredentialWrite(auth_type="NONE"),
        raw_idempotency_key=None,
    )
    rows = (
        connection.execute(
            select(table)
            .where(table.c.service_id == service_id)
            .order_by(table.c.created_at)
        )
        .mappings()
        .all()
    )
    assert [_status(row["status"]) for row in rows] == ["DISABLED", "ACTIVE"]
    assert rows[-1]["secret_ciphertext"] is None
    assert rows[-1]["key_version"] is None

    def fail_encrypt(*args, **kwargs):
        raise RuntimeError("encryption failed")

    monkeypatch.setattr(registry._secret_service, "encrypt", fail_encrypt)
    with pytest.raises(RuntimeError, match="encryption failed"):
        registry.put_credential(
            connection,
            tenant_id=tenant_a,
            admin_user_id=uuid4(),
            service_id=service_id,
            request=ServiceCredentialWrite(
                auth_type="STATIC_BEARER", secret="replacement"
            ),
            raw_idempotency_key=None,
        )
    active = (
        connection.execute(
            select(table).where(
                table.c.service_id == service_id, table.c.status == "ACTIVE"
            )
        )
        .mappings()
        .all()
    )
    assert len(active) == 1
    assert _status(active[0]["auth_type"]) == "NONE"


def test_optional_idempotency_replays_service_and_credential(registry_fixture) -> None:
    connection, registry, tenant_a, _, admin_a, _ = registry_fixture
    service_request = ServiceCreate(
        name="idempotent", base_url="https://idempotent.example", slug="idempotent"
    )
    created = registry.create_service(
        connection,
        tenant_id=tenant_a,
        admin_user_id=admin_a,
        request=service_request,
        raw_idempotency_key="m26-service-create-key",
    )
    replayed = registry.create_service(
        connection,
        tenant_id=tenant_a,
        admin_user_id=admin_a,
        request=service_request,
        raw_idempotency_key="m26-service-create-key",
    )
    assert replayed.replayed
    assert replayed.response == created.response
    import json

    service_id = UUID(json.loads(created.response.body)["id"])
    credential_request = ServiceCredentialWrite(
        auth_type="STATIC_BEARER", secret="idempotent-secret"
    )
    credential = registry.put_credential(
        connection,
        tenant_id=tenant_a,
        admin_user_id=admin_a,
        service_id=service_id,
        request=credential_request,
        raw_idempotency_key="m26-credential-put-key",
    )
    credential_replay = registry.put_credential(
        connection,
        tenant_id=tenant_a,
        admin_user_id=admin_a,
        service_id=service_id,
        request=credential_request,
        raw_idempotency_key="m26-credential-put-key",
    )
    assert credential_replay.replayed
    assert credential_replay.response == credential.response
    table = Base.metadata.tables["service_credentials"]
    assert (
        len(
            connection.execute(
                select(table).where(table.c.service_id == service_id)
            ).all()
        )
        == 1
    )


def _status(value) -> str:
    return value.value if hasattr(value, "value") else str(value)
