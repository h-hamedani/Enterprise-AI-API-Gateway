from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.persistence.models import Base


@pytest.fixture
def db_connection():
    settings = get_settings()

    if settings.environment == "production":
        pytest.fail("Integration database tests must never run in production.")

    engine = create_engine(settings.postgres_migration_dsn)

    with engine.connect() as connection:
        transaction = connection.begin()

        try:
            yield connection
        finally:
            transaction.rollback()

    engine.dispose()


def now() -> datetime:
    return datetime.now(UTC)


def tenant_values(*, tenant_id, name: str) -> dict:
    timestamp = now()

    return {
        "id": tenant_id,
        "name": name,
        "status": "ACTIVE",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def service_values(
    *,
    service_id,
    tenant_id,
    slug: str,
    display_name: str = "Test Service",
) -> dict:
    timestamp = now()

    return {
        "id": service_id,
        "tenant_id": tenant_id,
        "slug": slug,
        "display_name": display_name,
        "upstream_base_url": "https://example.internal",
        "status": "ACTIVE",
        "request_body_limit_bytes": 10 * 1024 * 1024,
        "connect_timeout_seconds": 5,
        "pool_timeout_seconds": 5,
        "write_timeout_seconds": 30,
        "read_idle_timeout_seconds": 60,
        "pre_response_timeout_seconds": 120,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def route_values(
    *,
    route_id,
    tenant_id,
    service_id,
    method: str,
    path_pattern: str,
) -> dict:
    timestamp = now()

    return {
        "id": route_id,
        "tenant_id": tenant_id,
        "service_id": service_id,
        "method": method,
        "path_pattern": path_pattern,
        "upstream_path_template": path_pattern,
        "priority": 0,
        "timeout_ms": None,
        "header_policy": None,
        "status": "ACTIVE",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def test_duplicate_service_slug_in_same_tenant_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]

    tenant_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="normal-api-tenant-1",
            )
        )
    )

    db_connection.execute(
        insert(services).values(
            **service_values(
                service_id=uuid4(),
                tenant_id=tenant_id,
                slug="claims",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(services).values(
                **service_values(
                    service_id=uuid4(),
                    tenant_id=tenant_id,
                    slug="claims",
                )
            )
        )


def test_same_service_slug_in_different_tenants_is_allowed(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]

    tenant_a = uuid4()
    tenant_b = uuid4()

    db_connection.execute(
        insert(tenants),
        [
            tenant_values(
                tenant_id=tenant_a,
                name="normal-api-tenant-a",
            ),
            tenant_values(
                tenant_id=tenant_b,
                name="normal-api-tenant-b",
            ),
        ],
    )

    db_connection.execute(
        insert(services),
        [
            service_values(
                service_id=uuid4(),
                tenant_id=tenant_a,
                slug="claims",
            ),
            service_values(
                service_id=uuid4(),
                tenant_id=tenant_b,
                slug="claims",
            ),
        ],
    )


def test_invalid_service_slug_is_rejected(db_connection):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]

    tenant_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="normal-api-invalid-slug",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(services).values(
                **service_values(
                    service_id=uuid4(),
                    tenant_id=tenant_id,
                    slug="Claims_API",
                )
            )
        )


def test_head_method_is_rejected(db_connection):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    routes = Base.metadata.tables["normal_api_routes"]

    tenant_id = uuid4()
    service_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="normal-api-head-method",
            )
        )
    )

    db_connection.execute(
        insert(services).values(
            **service_values(
                service_id=service_id,
                tenant_id=tenant_id,
                slug="claims",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(routes).values(
                **route_values(
                    route_id=uuid4(),
                    tenant_id=tenant_id,
                    service_id=service_id,
                    method="HEAD",
                    path_pattern="/claims",
                )
            )
        )


def test_route_path_without_leading_slash_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    routes = Base.metadata.tables["normal_api_routes"]

    tenant_id = uuid4()
    service_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="normal-api-route-path",
            )
        )
    )

    db_connection.execute(
        insert(services).values(
            **service_values(
                service_id=service_id,
                tenant_id=tenant_id,
                slug="claims",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(routes).values(
                **route_values(
                    route_id=uuid4(),
                    tenant_id=tenant_id,
                    service_id=service_id,
                    method="GET",
                    path_pattern="claims",
                )
            )
        )


def test_cross_tenant_route_service_reference_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    routes = Base.metadata.tables["normal_api_routes"]

    tenant_a = uuid4()
    tenant_b = uuid4()
    service_id = uuid4()

    db_connection.execute(
        insert(tenants),
        [
            tenant_values(
                tenant_id=tenant_a,
                name="normal-api-route-tenant-a",
            ),
            tenant_values(
                tenant_id=tenant_b,
                name="normal-api-route-tenant-b",
            ),
        ],
    )

    db_connection.execute(
        insert(services).values(
            **service_values(
                service_id=service_id,
                tenant_id=tenant_a,
                slug="claims",
            )
        )
    )

    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(routes).values(
                **route_values(
                    route_id=uuid4(),
                    tenant_id=tenant_b,
                    service_id=service_id,
                    method="GET",
                    path_pattern="/claims",
                )
            )
        )


def test_duplicate_route_method_path_is_rejected(
    db_connection,
):
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    routes = Base.metadata.tables["normal_api_routes"]

    tenant_id = uuid4()
    service_id = uuid4()

    db_connection.execute(
        insert(tenants).values(
            **tenant_values(
                tenant_id=tenant_id,
                name="normal-api-route-duplicate",
            )
        )
    )

    db_connection.execute(
        insert(services).values(
            **service_values(
                service_id=service_id,
                tenant_id=tenant_id,
                slug="claims",
            )
        )
    )

    route = route_values(
        route_id=uuid4(),
        tenant_id=tenant_id,
        service_id=service_id,
        method="GET",
        path_pattern="/claims",
    )

    db_connection.execute(insert(routes).values(**route))

    route["id"] = uuid4()

    with pytest.raises(IntegrityError):
        db_connection.execute(insert(routes).values(**route))
