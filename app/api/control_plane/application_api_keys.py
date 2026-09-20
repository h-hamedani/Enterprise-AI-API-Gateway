from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.exc import IntegrityError

from app.api.control_plane.invalidation import publish_committed_mutation
from app.api.dependencies import require_admin_context
from app.api.idempotency import (
    API_KEY_CREATE_POLICY,
    IdempotencyPolicy,
    idempotency_key,
    raise_idempotency_http_error,
    replay_response,
)
from app.control_plane.application_api_keys import (
    ControlPlaneAdminServices,
    ResourceNotFoundError,
)
from app.control_plane.auth import AdminContext
from app.control_plane.mutation_coordinator import (
    AuditAction,
    ResourceType,
    mutation_coordinator,
    response_resource_id,
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
    ApiKeyCreate,
    ApiKeyMetadata,
    ApiKeyPage,
    ApplicationCreate,
    ApplicationPage,
    ApplicationPatch,
    ApplicationResponse,
)

router = APIRouter(prefix="/api/v1/admin")
APPLICATION_CREATE_POLICY = IdempotencyPolicy("application.create")
ADMIN_CONTEXT_DEPENDENCY = Depends(require_admin_context)
_HANDLED_ERRORS = (
    ResourceNotFoundError,
    IntegrityError,
    IdempotencyConflictError,
    IdempotencyExpiredError,
    IdempotencyInProgressError,
    IdempotencyReplayError,
    ValueError,
)


def _services(request: Request) -> ControlPlaneAdminServices:
    services = getattr(request.app.state, "control_plane_admin_services", None)
    if not isinstance(services, ControlPlaneAdminServices):
        raise GatewayHttpError(500, "gateway_internal_error", "Internal server error.")
    return services


def _map_service_error(error: Exception) -> None:
    if isinstance(error, ResourceNotFoundError):
        resource_not_found()
    if isinstance(error, IntegrityError):
        resource_conflict()
    if isinstance(
        error,
        (
            IdempotencyConflictError,
            IdempotencyExpiredError,
            IdempotencyInProgressError,
            IdempotencyReplayError,
        ),
    ):
        raise_idempotency_http_error(error)
    if isinstance(error, ValueError) and str(error) == "Invalid pagination cursor.":
        invalid_request(param="cursor")
    raise error


@router.get("/applications", response_model=ApplicationPage)
async def list_applications(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApplicationPage:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _services(request).applications.list(
                    sync,
                    tenant_id=context.tenant_id,
                    limit=limit,
                    cursor=cursor,
                )
            )
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.post("/applications", status_code=201)
async def create_application(
    payload: ApplicationCreate,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
):
    raw_key = idempotency_key(request, APPLICATION_CREATE_POLICY)
    try:
        committed = None
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _services(request).applications.create(
                    sync,
                    tenant_id=context.tenant_id,
                    admin_user_id=context.admin_user_id,
                    request=payload,
                    raw_idempotency_key=raw_key,
                )
            )
            if not result.replayed:
                committed = await connection.run_sync(
                    lambda sync: mutation_coordinator.record_success(
                        sync,
                        context=context,
                        action=AuditAction.APPLICATION_CREATE,
                        resource_type=ResourceType.APPLICATION,
                        resource_id=response_resource_id(result.response),
                    )
                )
        if committed is not None:
            await publish_committed_mutation(
                request, committed, request_id=context.request_id
            )
        return replay_response(result.response)
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.get("/applications/{application_id}", response_model=ApplicationResponse)
async def get_application(
    application_id: UUID,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApplicationResponse:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _services(request).applications.get(
                    sync,
                    tenant_id=context.tenant_id,
                    application_id=application_id,
                )
            )
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.patch("/applications/{application_id}", response_model=ApplicationResponse)
async def update_application(
    application_id: UUID,
    payload: ApplicationPatch,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApplicationResponse:
    try:
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _services(request).applications.update(
                    sync,
                    tenant_id=context.tenant_id,
                    application_id=application_id,
                    patch=payload,
                )
            )
            committed = await connection.run_sync(
                lambda sync: mutation_coordinator.record_success(
                    sync,
                    context=context,
                    action=AuditAction.APPLICATION_UPDATE,
                    resource_type=ResourceType.APPLICATION,
                    resource_id=application_id,
                )
            )
        await publish_committed_mutation(
            request, committed, request_id=context.request_id
        )
        return result
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.get("/api-keys", response_model=ApiKeyPage)
async def list_api_keys(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApiKeyPage:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _services(request).api_keys.list(
                    sync,
                    tenant_id=context.tenant_id,
                    limit=limit,
                    cursor=cursor,
                )
            )
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.post("/api-keys", status_code=201)
async def create_api_key(
    payload: ApiKeyCreate,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
):
    raw_key = idempotency_key(request, API_KEY_CREATE_POLICY)
    if raw_key is None:
        invalid_request(param="Idempotency-Key")
    try:
        committed = None
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _services(request).api_keys.create(
                    sync,
                    tenant_id=context.tenant_id,
                    admin_user_id=context.admin_user_id,
                    request=payload,
                    raw_idempotency_key=raw_key,
                )
            )
            if not result.replayed:
                committed = await connection.run_sync(
                    lambda sync: mutation_coordinator.record_success(
                        sync,
                        context=context,
                        action=AuditAction.API_KEY_CREATE,
                        resource_type=ResourceType.API_KEY,
                        resource_id=response_resource_id(result.response),
                    )
                )
        if committed is not None:
            await publish_committed_mutation(
                request, committed, request_id=context.request_id
            )
        return replay_response(result.response)
    except _HANDLED_ERRORS as error:
        _map_service_error(error)


@router.post(
    "/api-keys/{api_key_id}/revoke",
    response_model=ApiKeyMetadata,
)
async def revoke_api_key(
    api_key_id: UUID,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApiKeyMetadata:
    try:
        async with request.app.state.db_engine.begin() as connection:
            result = await connection.run_sync(
                lambda sync: _services(request).api_keys.revoke(
                    sync,
                    tenant_id=context.tenant_id,
                    api_key_id=api_key_id,
                )
            )
            committed = await connection.run_sync(
                lambda sync: mutation_coordinator.record_success(
                    sync,
                    context=context,
                    action=AuditAction.API_KEY_REVOKE,
                    resource_type=ResourceType.API_KEY,
                    resource_id=api_key_id,
                )
            )
        await publish_committed_mutation(
            request, committed, request_id=context.request_id
        )
        return result
    except _HANDLED_ERRORS as error:
        _map_service_error(error)
