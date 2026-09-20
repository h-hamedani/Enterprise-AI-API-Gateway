from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.main import create_app


def test_m24_routes_match_frozen_surface_and_require_authentication():
    app = create_app()
    routes = {
        (method.upper(), path)
        for path, path_item in app.openapi()["paths"].items()
        for method in path_item
        if method.lower()
        in {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
    }
    expected = {
        ("GET", "/api/v1/admin/applications"),
        ("POST", "/api/v1/admin/applications"),
        ("GET", "/api/v1/admin/applications/{application_id}"),
        ("PATCH", "/api/v1/admin/applications/{application_id}"),
        ("GET", "/api/v1/admin/api-keys"),
        ("POST", "/api/v1/admin/api-keys"),
        ("POST", "/api/v1/admin/api-keys/{api_key_id}/revoke"),
    }
    assert expected <= routes
    assert not any(path.startswith("/admin/v1") for _, path in routes)

    with TestClient(app) as client:
        response = client.get("/api/v1/admin/applications")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_error"


def test_api_key_creation_requires_idempotency_key_after_authentication():
    app = create_app()

    async def authenticated_context():
        return AdminContext(uuid4(), uuid4(), uuid4(), uuid4())

    app.dependency_overrides[require_admin_context] = authenticated_context
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/admin/api-keys",
            json={"application_id": str(uuid4()), "name": "missing-key"},
        )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.json()["error"]["param"] == "Idempotency-Key"
