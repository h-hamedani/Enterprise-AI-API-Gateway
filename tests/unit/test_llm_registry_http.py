from uuid import uuid4

from fastapi.testclient import TestClient

from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.main import create_app


def test_m27_exact_surface_requires_auth_and_excludes_credential_rotation():
    app = create_app()
    routes = {
        (method.upper(), path)
        for path, item in app.openapi()["paths"].items()
        for method in item
        if method in {"get", "post", "put", "patch", "delete"}
    }
    expected = {
        ("GET", "/api/v1/admin/llm/providers"),
        ("POST", "/api/v1/admin/llm/providers"),
        ("GET", "/api/v1/admin/llm/providers/{provider_id}"),
        ("PATCH", "/api/v1/admin/llm/providers/{provider_id}"),
        ("GET", "/api/v1/admin/llm/targets"),
        ("POST", "/api/v1/admin/llm/targets"),
        ("GET", "/api/v1/admin/llm/targets/{target_id}"),
        ("PATCH", "/api/v1/admin/llm/targets/{target_id}"),
        ("GET", "/api/v1/admin/llm/models"),
        ("POST", "/api/v1/admin/llm/models"),
        ("GET", "/api/v1/admin/llm/models/{model_id}"),
        ("PATCH", "/api/v1/admin/llm/models/{model_id}"),
        ("PUT", "/api/v1/admin/llm/models/{model_id}/capabilities"),
        ("GET", "/api/v1/admin/llm/aliases"),
        ("POST", "/api/v1/admin/llm/aliases"),
        ("GET", "/api/v1/admin/llm/aliases/{alias_id}"),
        ("PATCH", "/api/v1/admin/llm/aliases/{alias_id}"),
        ("PUT", "/api/v1/admin/llm/aliases/{alias_id}/targets"),
        ("GET", "/api/v1/admin/llm/models/{model_id}/prices"),
        ("POST", "/api/v1/admin/llm/models/{model_id}/prices"),
        ("PATCH", "/api/v1/admin/model-prices/{price_id}"),
    }
    assert expected <= routes
    assert ("PUT", "/api/v1/admin/llm/targets/{target_id}/credential") in routes
    with TestClient(app) as client:
        response = client.get("/api/v1/admin/llm/providers")
    assert response.status_code == 401


def test_llm_credential_rotation_requires_idempotency_key_after_authentication():
    app = create_app()

    async def authenticated_context():
        return AdminContext(uuid4(), uuid4(), uuid4(), uuid4())

    app.dependency_overrides[require_admin_context] = authenticated_context
    with TestClient(app) as client:
        response = client.put(
            f"/api/v1/admin/llm/targets/{uuid4()}/credential",
            json={"secret": "not-logged"},
        )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.json()["error"]["param"] == "Idempotency-Key"
