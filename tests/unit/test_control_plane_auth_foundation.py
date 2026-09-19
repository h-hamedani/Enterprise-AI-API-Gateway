from __future__ import annotations

from uuid import UUID

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.api import dependencies
from app.core.errors import (
    authorization_error,
    install_error_handlers,
    resource_not_found,
)
from app.core.request_context import request_context_middleware


@pytest.mark.parametrize(
    ("raiser", "status", "code"),
    [
        (resource_not_found, 404, "resource_not_found"),
        (authorization_error, 403, "authorization_error"),
    ],
)
def test_authorization_helpers_use_normalized_safe_errors(raiser, status, code):
    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)

    @app.get("/probe")
    async def probe():
        raiser()

    with TestClient(app) as client:
        response = client.get("/probe")

    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert UUID(response.json()["request_id"]).version == 7


def test_pre_auth_context_exposes_only_ip_and_normalized_route_class():
    app = FastAPI()

    @app.get("/api/v1/admin/probe")
    async def probe(request: Request):
        context = dependencies.admin_pre_auth_context(request)
        return {
            "client_ip": context.client_ip,
            "route_class": context.route_class,
        }

    with TestClient(app) as client:
        response = client.get("/api/v1/admin/probe")

    assert response.json()["route_class"] == "ADMIN_AUTH_PROTECTED"
    assert response.json()["client_ip"]
