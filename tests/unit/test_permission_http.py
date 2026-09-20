from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.main import create_app


def test_permission_routes_match_frozen_get_and_replace_surface():
    app = create_app()
    path = "/api/v1/admin/api-keys/{api_key_id}/permissions"
    methods = {
        method.upper()
        for route_path, path_item in app.openapi()["paths"].items()
        if route_path == path
        for method in path_item
        if method in {"get", "put", "post", "delete", "patch"}
    }
    assert methods == {"GET", "PUT"}

    with TestClient(app) as client:
        response = client.get(f"/api/v1/admin/api-keys/{uuid4()}/permissions")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_error"


def test_invalid_permission_type_and_action_use_normalized_validation_error():
    app = create_app()

    async def authenticated_context():
        return AdminContext(uuid4(), uuid4(), uuid4(), uuid4())

    app.dependency_overrides[require_admin_context] = authenticated_context
    api_key_id = uuid4()
    payloads = [
        {
            "permissions": [
                {
                    "resource_type": "UNKNOWN",
                    "resource_id": str(uuid4()),
                    "action": "INVOKE",
                }
            ]
        },
        {
            "permissions": [
                {
                    "resource_type": "SERVICE",
                    "resource_id": str(uuid4()),
                    "action": "ADMIN",
                }
            ]
        },
    ]
    with TestClient(app) as client:
        responses = [
            client.put(
                f"/api/v1/admin/api-keys/{api_key_id}/permissions",
                json=payload,
            )
            for payload in payloads
        ]
    for response in responses:
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"
