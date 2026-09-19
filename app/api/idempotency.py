from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn

from fastapi import Request
from starlette.responses import Response

from app.core.errors import GatewayHttpError, invalid_request
from app.core.idempotency import (
    IdempotencyConflictError,
    IdempotencyExpiredError,
    IdempotencyInProgressError,
    IdempotencyReplayError,
    StoredHttpResponse,
)


@dataclass(frozen=True, slots=True)
class IdempotencyPolicy:
    endpoint_key: str
    required: bool = False


API_KEY_CREATE_POLICY = IdempotencyPolicy("api-key.create", required=True)
LLM_CREDENTIAL_ROTATION_POLICY = IdempotencyPolicy(
    "llm-target.credential.rotate", required=True
)


def idempotency_key(request: Request, policy: IdempotencyPolicy) -> str | None:
    raw_key = request.headers.get("Idempotency-Key")
    if raw_key is None:
        if policy.required:
            invalid_request(param="Idempotency-Key")
        return None
    if not 8 <= len(raw_key) <= 128 or raw_key != raw_key.strip():
        invalid_request(param="Idempotency-Key")
    return raw_key


def raise_idempotency_http_error(error: Exception) -> NoReturn:
    if isinstance(error, IdempotencyInProgressError):
        raise GatewayHttpError(
            409,
            "idempotency_in_progress",
            "Idempotency operation is in progress.",
            headers={"Retry-After": "1"},
        )
    if isinstance(error, (IdempotencyConflictError, IdempotencyExpiredError)):
        raise GatewayHttpError(
            409,
            "idempotency_conflict",
            "Idempotency conflict.",
        )
    if isinstance(error, IdempotencyReplayError):
        raise GatewayHttpError(
            500,
            "gateway_internal_error",
            "Internal server error.",
        )
    raise error


def replay_response(stored: StoredHttpResponse) -> Response:
    return Response(
        content=stored.body,
        status_code=stored.status_code,
        headers=dict(stored.headers),
    )
