from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.control_plane.normal_api_registry import router as normal_registry_router
from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.core.errors import install_error_handlers
from app.core.request_context import request_context_middleware
from app.main import create_app
from app.schemas.control_plane import (
    RouteCreate,
    RouteHeaderPolicy,
    RoutePatch,
    ServiceCreate,
    ServiceCredentialWrite,
)


def test_m26_endpoint_surface_and_authentication() -> None:
    app = create_app()
    routes = {
        (method.upper(), path)
        for path, path_item in app.openapi()["paths"].items()
        for method in path_item
        if method.lower() in {"get", "post", "put", "patch", "delete"}
    }
    expected = {
        ("GET", "/api/v1/admin/services"),
        ("POST", "/api/v1/admin/services"),
        ("GET", "/api/v1/admin/services/{service_id}"),
        ("PATCH", "/api/v1/admin/services/{service_id}"),
        ("PUT", "/api/v1/admin/services/{service_id}/credential"),
        ("GET", "/api/v1/admin/routes"),
        ("POST", "/api/v1/admin/routes"),
        ("GET", "/api/v1/admin/routes/{route_id}"),
        ("PATCH", "/api/v1/admin/routes/{route_id}"),
    }
    assert expected <= routes
    assert not any(
        method == "DELETE" and path.startswith("/api/v1/admin/services")
        for method, path in routes
    )
    assert not any(
        method == "DELETE" and path.startswith("/api/v1/admin/routes")
        for method, path in routes
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/admin/services")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_error"


def test_service_schema_uses_phase_defaults_and_rejects_generic_timeout() -> None:
    service = ServiceCreate(
        name="orders", base_url="https://upstream.example", slug="orders"
    )
    assert service.connect_timeout_seconds == 5
    assert service.pool_timeout_seconds == 5
    assert service.write_timeout_seconds == 30
    assert service.read_idle_timeout_seconds == 60
    assert service.pre_response_timeout_seconds == 120
    assert "timeout_ms" not in ServiceCreate.model_fields


def test_route_schema_is_scalar_and_structural() -> None:
    route = RouteCreate(
        service_id="00000000-0000-0000-0000-000000000001",
        path_pattern="/orders/{id}",
        method="GET",
        upstream_path_template="/v1/orders/{id}",
        header_policy=None,
        priority=0,
    )
    assert route.method == "GET"
    assert "methods" not in RouteCreate.model_fields
    assert RoutePatch(method="POST").method == "POST"


def test_route_header_policy_canonical_and_null_semantics() -> None:
    base = {
        "service_id": "00000000-0000-0000-0000-000000000001",
        "path_pattern": "/orders/{id}",
        "method": "GET",
        "upstream_path_template": "/v1/orders/{id}",
        "priority": 0,
    }
    assert RouteCreate(**base).header_policy is None
    assert RouteCreate(**base, header_policy=None).header_policy is None
    assert (
        RouteCreate(
            **base, header_policy={"request_allowlist": []}
        ).header_policy.request_allowlist
        == []
    )
    assert RouteCreate(
        **base, header_policy={"request_allowlist": ["Accept", "X-API-Key"]}
    ).model_dump()["header_policy"] == {"request_allowlist": ["accept", "x-api-key"]}
    assert "header_policy" not in RoutePatch(method="POST").model_dump(
        exclude_unset=True
    )
    assert RoutePatch(header_policy=None).model_dump(exclude_unset=True) == {
        "header_policy": None
    }
    assert RoutePatch(header_policy={"request_allowlist": ["ACCEPT"]}).model_dump(
        exclude_unset=True
    ) == {"header_policy": {"request_allowlist": ["accept"]}}


@pytest.mark.parametrize(
    "policy",
    [
        {},
        {"mode": "metadata-only"},
        {"request_allowlist": None},
        {"request_allowlist": "accept"},
        {"request_allowlist": [""]},
        {"request_allowlist": ["bad name"]},
        {"request_allowlist": ["Accept", "accept"]},
        {"request_allowlist": ["authorization"]},
        {"request_allowlist": ["proxy-authorization"]},
        {"request_allowlist": ["proxy-authenticate"]},
        {"request_allowlist": ["Host"]},
        {"request_allowlist": ["content-length"]},
        {"request_allowlist": ["cookie"]},
        {"request_allowlist": ["forwarded"]},
        {"request_allowlist": ["x-request-id"]},
        {"request_allowlist": ["Connection"]},
        {"request_allowlist": ["keep-alive"]},
        {"request_allowlist": ["te"]},
        {"request_allowlist": ["trailer"]},
        {"request_allowlist": ["transfer-encoding"]},
        {"request_allowlist": ["upgrade"]},
        {"request_allowlist": ["X-Forwarded-For"]},
        {"request_allowlist": ["X-Forwarded-Host"]},
        {"request_allowlist": ["X-Forwarded-Proto"]},
        {"request_allowlist": ["X-Gateway-Key"]},
    ],
)
def test_route_header_policy_rejects_invalid_shape_and_names(policy) -> None:
    with pytest.raises(ValidationError):
        RouteHeaderPolicy.model_validate(policy)


def test_malformed_route_policy_uses_existing_admin_400_envelope() -> None:
    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    app.include_router(normal_registry_router)
    app.dependency_overrides[require_admin_context] = lambda: AdminContext(
        uuid4(), uuid4(), uuid4(), uuid4()
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/admin/routes",
            json={
                "service_id": str(uuid4()),
                "path_pattern": "/orders",
                "method": "GET",
                "upstream_path_template": "/orders",
                "priority": 0,
                "header_policy": {"request_allowlist": ["authorization"]},
            },
        )
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "invalid_request"
    assert body["error"]["type"] == "invalid_request"
    assert UUID(body["request_id"]).version == 7
    assert response.headers["X-Request-ID"] == body["request_id"]


def test_credential_auth_type_shapes() -> None:
    assert ServiceCredentialWrite(auth_type="NONE").secret is None
    assert (
        ServiceCredentialWrite(
            auth_type="STATIC_BEARER", secret="bearer-secret"
        ).header_name
        is None
    )
    assert (
        ServiceCredentialWrite(
            auth_type="STATIC_HEADER", secret="header-secret", header_name="X-Key"
        ).header_name
        == "X-Key"
    )
