from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.testclient import TestClient
from pydantic import BaseModel, ValidationError

from app.api.control_plane import ADMIN_API_PREFIX
from app.core.errors import (
    authorization_error,
    install_error_handlers,
    resource_conflict,
    resource_not_found,
)
from app.core.pagination import (
    PAGINATION_ORDER,
    CursorPosition,
    PaginationCursorCodec,
    PaginationParameters,
    decode_cursor,
)
from app.core.request_context import request_context_middleware


class ProbeBody(BaseModel):
    status: str


def _app() -> FastAPI:
    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    router = APIRouter(prefix=ADMIN_API_PREFIX)

    @router.post("/_m22_validation")
    async def validation_probe(body: ProbeBody) -> ProbeBody:
        return body

    @router.get("/_m22_query")
    async def query_probe(limit: int = Query(ge=1, le=200)) -> dict[str, int]:
        return {"limit": limit}

    @router.get("/_m22_cursor")
    async def cursor_probe(cursor: str) -> dict[str, str]:
        decode_cursor(PaginationCursorCodec(b"p" * 32), cursor, tenant_id=UUID(int=1))
        return {"status": "ok"}

    @router.get("/_m22_error/{kind}")
    async def error_probe(kind: str, request: Request):
        if kind == "forbidden":
            authorization_error()
        if kind == "missing":
            resource_not_found()
        if kind == "conflict":
            resource_conflict()
        if kind == "internal":
            raise RuntimeError("sensitive-internal-detail")
        return {"request_id": str(request.state.request_id)}

    app.include_router(router)
    return app


def _client(*, raise_server_exceptions: bool = True) -> TestClient:
    return TestClient(
        _app(),
        raise_server_exceptions=raise_server_exceptions,
    )


def _assert_error(response, *, status: int, code: str) -> dict:
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"error", "request_id"}
    assert body["error"]["type"] == code
    assert body["error"]["code"] == code
    assert set(body["error"]) == {"message", "type", "param", "code", "metadata"}
    assert UUID(body["request_id"]).version == 7
    assert response.headers["X-Request-ID"] == body["request_id"]
    return body


@pytest.mark.parametrize(
    ("kind", "status", "code"),
    [
        ("forbidden", 403, "authorization_error"),
        ("missing", 404, "resource_not_found"),
        ("conflict", 409, "resource_conflict"),
    ],
)
def test_public_errors_use_canonical_envelope(kind, status, code):
    with _client() as client:
        response = client.get(f"{ADMIN_API_PREFIX}/_m22_error/{kind}")
    _assert_error(response, status=status, code=code)


@pytest.mark.parametrize(
    ("path", "request_kwargs", "expected_param"),
    [
        ("/_m22_validation", {"json": {}}, "status"),
        ("/_m22_validation", {"content": "{"}, None),
        ("/_m22_query", {"params": {"limit": 0}}, "limit"),
    ],
)
def test_validation_and_malformed_json_are_normalized(
    path, request_kwargs, expected_param
):
    with _client() as client:
        response = client.request(
            "POST" if "validation" in path else "GET",
            f"{ADMIN_API_PREFIX}{path}",
            **request_kwargs,
        )
    body = _assert_error(response, status=400, code="invalid_request")
    assert body["error"]["param"] == expected_param
    assert "input" not in response.text


def test_internal_error_is_safe_and_keeps_canonical_request_id(caplog):
    raw_token = "adm_" + "S" * 43
    with _client(raise_server_exceptions=False) as client:
        response = client.get(
            f"{ADMIN_API_PREFIX}/_m22_error/internal",
            headers={"Authorization": f"Bearer {raw_token}", "X-Request-ID": "client"},
        )
    body = _assert_error(response, status=500, code="gateway_internal_error")
    assert body["request_id"] != "client"
    assert "sensitive-internal-detail" not in response.text
    assert raw_token not in response.text
    assert raw_token not in caplog.text
    assert "Authorization" not in caplog.text


def test_cors_is_fail_closed_without_a_frozen_allowlist():
    origin = "https://untrusted.example"
    with _client() as client:
        response = client.get(
            f"{ADMIN_API_PREFIX}/_m22_error/ok", headers={"Origin": origin}
        )
        preflight = client.options(
            f"{ADMIN_API_PREFIX}/_m22_error/ok",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
    assert "Access-Control-Allow-Origin" not in response.headers
    assert "Access-Control-Allow-Origin" not in preflight.headers
    assert preflight.status_code == 400


def test_pagination_cursor_is_signed_tenant_bound_and_round_trips():
    codec = PaginationCursorCodec(b"p" * 32)
    position = CursorPosition(
        tenant_id=uuid4(),
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        resource_id=uuid4(),
    )
    encoded = codec.encode(position)

    assert codec.decode(encoded, tenant_id=position.tenant_id) == position
    assert str(position.tenant_id) not in encoded
    assert str(position.resource_id) not in encoded
    with pytest.raises(ValueError, match="Invalid pagination cursor"):
        codec.decode(encoded, tenant_id=uuid4())
    changed_at = len(encoded) // 2
    replacement = "A" if encoded[changed_at] != "A" else "B"
    tampered = f"{encoded[:changed_at]}{replacement}{encoded[changed_at + 1 :]}"
    with pytest.raises(ValueError, match="Invalid pagination cursor"):
        codec.decode(tampered, tenant_id=position.tenant_id)


def test_invalid_cursor_uses_canonical_validation_error():
    with _client() as client:
        response = client.get(
            f"{ADMIN_API_PREFIX}/_m22_cursor", params={"cursor": "not-a-cursor"}
        )
    body = _assert_error(response, status=400, code="invalid_request")
    assert body["error"]["param"] == "cursor"


def test_pagination_limits_and_order_are_frozen():
    assert PaginationParameters().limit == 50
    assert PaginationParameters(limit=1).limit == 1
    assert PaginationParameters(limit=200).limit == 200
    assert PAGINATION_ORDER == ("created_at DESC", "id DESC")
    with pytest.raises(ValidationError):
        PaginationParameters(limit=0)
    with pytest.raises(ValidationError):
        PaginationParameters(limit=201)
