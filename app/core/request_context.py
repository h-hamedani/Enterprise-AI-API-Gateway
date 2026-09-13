from __future__ import annotations

from contextvars import ContextVar
from uuid import UUID

import uuid_utils.compat as uuid_utils
from fastapi import Request
from starlette.responses import Response

_request_id_ctx: ContextVar[UUID | None] = ContextVar(
    "request_id",
    default=None,
)


def new_request_id() -> UUID:
    return uuid_utils.uuid7()


def get_request_id() -> UUID | None:
    return _request_id_ctx.get()


async def request_context_middleware(
    request: Request,
    call_next,
) -> Response:
    request_id = new_request_id()

    token = _request_id_ctx.set(request_id)
    request.state.request_id = request_id

    client_request_id = request.headers.get("X-Request-ID")
    if client_request_id:
        request.state.client_request_id = client_request_id

    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = str(request_id)
        return response
    finally:
        _request_id_ctx.reset(token)
