from fastapi.testclient import TestClient

from app.main import create_app
from app.schemas.control_plane import (
    RouteCreate,
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
