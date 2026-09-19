from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.request_context import get_request_id, new_request_id


@dataclass(frozen=True, slots=True)
class GatewayHttpError(Exception):
    status_code: int
    code: str
    message: str


def authentication_error() -> NoReturn:
    raise GatewayHttpError(401, "authentication_error", "Authentication failed.")


def authorization_error() -> NoReturn:
    raise GatewayHttpError(403, "authorization_error", "Authorization failed.")


def resource_not_found() -> NoReturn:
    raise GatewayHttpError(404, "resource_not_found", "Resource not found.")


async def _gateway_error_handler(
    request: Request, exc: GatewayHttpError
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None) or get_request_id()
    if request_id is None:
        request_id = new_request_id()
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "message": exc.message,
                "type": exc.code,
                "param": None,
                "code": exc.code,
                "metadata": None,
            },
            "request_id": str(request_id),
        },
    )


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(GatewayHttpError, _gateway_error_handler)
