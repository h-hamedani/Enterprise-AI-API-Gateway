from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.api.idempotency import (
    API_KEY_CREATE_POLICY,
    LLM_CREDENTIAL_ROTATION_POLICY,
    IdempotencyPolicy,
    idempotency_key,
    raise_idempotency_http_error,
    replay_response,
)
from app.core.errors import install_error_handlers
from app.core.idempotency import (
    IdempotencyConflictError,
    IdempotencyInProgressError,
    StoredHttpResponse,
)
from app.core.request_context import request_context_middleware


def _app() -> FastAPI:
    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    policy = IdempotencyPolicy("api-key.create", required=True)

    @app.post("/probe")
    async def probe(request: Request, outcome: str = "ok"):
        idempotency_key(request, policy)
        if outcome == "progress":
            raise_idempotency_http_error(IdempotencyInProgressError())
        if outcome == "conflict":
            raise_idempotency_http_error(IdempotencyConflictError())
        return {"status": "ok"}

    return app


def test_required_idempotency_key_is_validated_without_echoing_value():
    with TestClient(_app()) as client:
        missing = client.post("/probe")
        invalid = client.post("/probe", headers={"Idempotency-Key": "short"})
    for response in (missing, invalid):
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"
        assert response.json()["error"]["param"] == "Idempotency-Key"
        assert "short" not in response.text


def test_in_progress_and_conflict_http_contract():
    headers = {"Idempotency-Key": "opaque-key-value"}
    with TestClient(_app()) as client:
        progress = client.post("/probe?outcome=progress", headers=headers)
        conflict = client.post("/probe?outcome=conflict", headers=headers)
    assert progress.status_code == 409
    assert progress.json()["error"]["code"] == "idempotency_in_progress"
    assert progress.headers["Retry-After"] == "1"
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert "opaque-key-value" not in progress.text + conflict.text


def test_frozen_mandatory_policies_and_exact_replay_response():
    assert API_KEY_CREATE_POLICY.required
    assert API_KEY_CREATE_POLICY.endpoint_key == "api-key.create"
    assert LLM_CREDENTIAL_ROTATION_POLICY.required
    assert LLM_CREDENTIAL_ROTATION_POLICY.endpoint_key == (
        "llm-target.credential.rotate"
    )
    stored = StoredHttpResponse(
        201,
        b'{"key":"gw_exact"}',
        (("Content-Type", "application/json"), ("Location", "/api-keys/one")),
    )
    response = replay_response(stored)
    assert response.status_code == 201
    assert response.body == stored.body
    assert response.headers["Location"] == "/api-keys/one"
