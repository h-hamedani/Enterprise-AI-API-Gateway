from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.exc import IntegrityError

from app.api.dependencies import require_admin_context
from app.api.idempotency import (
    IdempotencyPolicy,
    idempotency_key,
    raise_idempotency_http_error,
    replay_response,
)
from app.control_plane.auth import AdminContext
from app.control_plane.normal_api_registry import (
    NormalApiRegistryAdminService,
    RegistryConflictError,
    RegistryResourceNotFoundError,
)
from app.core.errors import (
    GatewayHttpError,
    invalid_request,
    resource_conflict,
    resource_not_found,
)
from app.core.idempotency import (
    IdempotencyConflictError,
    IdempotencyExpiredError,
    IdempotencyInProgressError,
    IdempotencyReplayError,
)
from app.schemas.control_plane import (
    CredentialMetadata,
    RouteCreate,
    RoutePage,
    RoutePatch,
    RouteResponse,
    ServiceCreate,
    ServiceCredentialWrite,
    ServicePage,
    ServicePatch,
    ServiceResponse,
)

router = APIRouter(prefix="/api/v1/admin")
ADMIN_CONTEXT_DEPENDENCY = Depends(require_admin_context)
SERVICE_CREATE_POLICY = IdempotencyPolicy("normal-service.create")
CREDENTIAL_PUT_POLICY = IdempotencyPolicy("normal-service.credential.put")
_IDEMPOTENCY_ERRORS = (
    IdempotencyConflictError,
    IdempotencyExpiredError,
    IdempotencyInProgressError,
    IdempotencyReplayError,
)


def _registry(request: Request) -> NormalApiRegistryAdminService:
    service = getattr(request.app.state, "normal_api_registry_service", None)
    if not isinstance(service, NormalApiRegistryAdminService):
        raise GatewayHttpError(500, "gateway_internal_error", "Internal server error.")
    return service


def _map_error(error: Exception) -> None:
    if isinstance(error, RegistryResourceNotFoundError):
        resource_not_found()
    if isinstance(error, (RegistryConflictError, IntegrityError)):
        resource_conflict()
    if isinstance(error, _IDEMPOTENCY_ERRORS):
        raise_idempotency_http_error(error)
    if isinstance(error, ValueError) and str(error) == "Invalid pagination cursor.":
        invalid_request(param="cursor")
    raise error


@router.get("/services", response_model=ServicePage)
async def list_services(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ServicePage:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).list_services(
                    sync, tenant_id=context.tenant_id, limit=limit, cursor=cursor
                )
            )
    except (RegistryResourceNotFoundError, IntegrityError, ValueError) as error:
        _map_error(error)


@router.post("/services", response_model=ServiceResponse, status_code=201)
async def create_service(
    payload: ServiceCreate,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
):
    raw_key = idempotency_key(request, SERVICE_CREATE_POLICY)
    try:
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _registry(request).create_service(
                    sync,
                    tenant_id=context.tenant_id,
                    admin_user_id=context.admin_user_id,
                    request=payload,
                    raw_idempotency_key=raw_key,
                )
            )
        return replay_response(result.response)
    except (
        RegistryConflictError,
        IntegrityError,
        *_IDEMPOTENCY_ERRORS,
    ) as error:
        _map_error(error)


@router.get("/services/{service_id}", response_model=ServiceResponse)
async def get_service(
    service_id: UUID,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ServiceResponse:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).get_service(
                    sync, tenant_id=context.tenant_id, service_id=service_id
                )
            )
    except RegistryResourceNotFoundError as error:
        _map_error(error)


@router.patch("/services/{service_id}", response_model=ServiceResponse)
async def patch_service(
    service_id: UUID,
    payload: ServicePatch,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ServiceResponse:
    try:
        async with request.app.state.db_engine.begin() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).patch_service(
                    sync,
                    tenant_id=context.tenant_id,
                    service_id=service_id,
                    patch=payload,
                )
            )
    except (RegistryResourceNotFoundError, IntegrityError) as error:
        _map_error(error)


@router.put("/services/{service_id}/credential", response_model=CredentialMetadata)
async def put_service_credential(
    service_id: UUID,
    payload: ServiceCredentialWrite,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
):
    raw_key = idempotency_key(request, CREDENTIAL_PUT_POLICY)
    try:
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _registry(request).put_credential(
                    sync,
                    tenant_id=context.tenant_id,
                    admin_user_id=context.admin_user_id,
                    service_id=service_id,
                    request=payload,
                    raw_idempotency_key=raw_key,
                )
            )
        return replay_response(result.response)
    except (
        RegistryResourceNotFoundError,
        IntegrityError,
        *_IDEMPOTENCY_ERRORS,
    ) as error:
        _map_error(error)


@router.get("/routes", response_model=RoutePage)
async def list_routes(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> RoutePage:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).list_routes(
                    sync, tenant_id=context.tenant_id, limit=limit, cursor=cursor
                )
            )
    except ValueError as error:
        _map_error(error)


@router.post("/routes", response_model=RouteResponse, status_code=201)
async def create_route(
    payload: RouteCreate,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> RouteResponse:
    try:
        async with request.app.state.db_engine.begin() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).create_route(
                    sync, tenant_id=context.tenant_id, request=payload
                )
            )
    except (RegistryResourceNotFoundError, IntegrityError) as error:
        _map_error(error)


@router.get("/routes/{route_id}", response_model=RouteResponse)
async def get_route(
    route_id: UUID,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> RouteResponse:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).get_route(
                    sync, tenant_id=context.tenant_id, route_id=route_id
                )
            )
    except RegistryResourceNotFoundError as error:
        _map_error(error)


@router.patch("/routes/{route_id}", response_model=RouteResponse)
async def patch_route(
    route_id: UUID,
    payload: RoutePatch,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> RouteResponse:
    try:
        async with request.app.state.db_engine.begin() as connection:
            return await connection.run_sync(
                lambda sync: _registry(request).patch_route(
                    sync,
                    tenant_id=context.tenant_id,
                    route_id=route_id,
                    patch=payload,
                )
            )
    except (RegistryResourceNotFoundError, IntegrityError) as error:
        _map_error(error)
