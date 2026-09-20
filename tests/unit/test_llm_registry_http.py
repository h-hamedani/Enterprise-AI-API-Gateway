from fastapi.testclient import TestClient

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
    assert ("PUT", "/api/v1/admin/llm/targets/{target_id}/credential") not in routes
    with TestClient(app) as client:
        response = client.get("/api/v1/admin/llm/providers")
    assert response.status_code == 401
