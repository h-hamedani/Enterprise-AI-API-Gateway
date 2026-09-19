from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, NoReturn

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.request_context import get_request_id, new_request_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GatewayHttpError(Exception):
    status_code: int
    code: str
    message: str
    param: str | None = None
    metadata: dict[str, Any] | None = None


def authentication_error() -> NoReturn:
    raise GatewayHttpError(401, "authentication_error", "Authentication failed.")


def authorization_error() -> NoReturn:
    raise GatewayHttpError(403, "authorization_error", "Authorization failed.")


def resource_not_found() -> NoReturn:
    raise GatewayHttpError(404, "resource_not_found", "Resource not found.")


def resource_conflict() -> NoReturn:
    raise GatewayHttpError(409, "resource_conflict", "Resource conflict.")


def invalid_request(*, param: str | None = None) -> NoReturn:
    raise GatewayHttpError(400, "invalid_request", "Invalid request.", param=param)


def error_response(request: Request, error: GatewayHttpError) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None) or get_request_id()
    if request_id is None:
        request_id = new_request_id()
    return JSONResponse(
        status_code=error.status_code,
        headers={"X-Request-ID": str(request_id)},
        content={
            "error": {
                "message": error.message,
                "type": error.code,
                "param": error.param,
                "code": error.code,
                "metadata": error.metadata,
            },
            "request_id": str(request_id),
        },
    )


async def _gateway_error_handler(
    request: Request, exc: GatewayHttpError
) -> JSONResponse:
    return error_response(request, exc)


async def _validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    param = None
    for issue in exc.errors():
        if issue.get("type") == "json_invalid":
            break
        location = issue.get("loc", ())
        if location == ("body",):
            break
        if location and isinstance(location[-1], (str, int)):
            param = str(location[-1])
            break
    return error_response(
        request,
        GatewayHttpError(400, "invalid_request", "Invalid request.", param=param),
    )


async def _http_error_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    mappings = {
        401: ("authentication_error", "Authentication failed."),
        403: ("authorization_error", "Authorization failed."),
        404: ("resource_not_found", "Resource not found."),
        409: ("resource_conflict", "Resource conflict."),
        413: ("request_too_large", "Request too large."),
    }
    code, message = mappings.get(
        exc.status_code, ("invalid_request", "Invalid request.")
    )
    status_code = exc.status_code if exc.status_code in mappings else 400
    return error_response(request, GatewayHttpError(status_code, code, message))


async def _internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None) or get_request_id()
    route = request.scope.get("route")
    route_template = getattr(route, "path", "unmatched")
    logger.error(
        "Unhandled request failure request_id=%s method=%s route=%s",
        request_id,
        request.method,
        route_template,
    )
    return error_response(
        request,
        GatewayHttpError(
            500,
            "gateway_internal_error",
            "Internal server error.",
        ),
    )


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(GatewayHttpError, _gateway_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, _http_error_handler)
    app.add_exception_handler(Exception, _internal_error_handler)
