from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.exc import IntegrityError

from app.api.dependencies import require_admin_context
from app.control_plane.application_api_keys import ControlPlaneAdminServices
from app.control_plane.auth import AdminContext
from app.control_plane.mutation_coordinator import (
    AuditAction,
    ResourceType,
    mutation_coordinator,
)
from app.control_plane.permissions import (
    PermissionApiKeyNotFoundError,
    PermissionConflictError,
    PermissionTargetNotFoundError,
)
from app.core.errors import GatewayHttpError, resource_conflict, resource_not_found
from app.schemas.control_plane import (
    ApiKeyPermissionList,
    ApiKeyPermissionReplacement,
)

router = APIRouter(prefix="/api/v1/admin")
ADMIN_CONTEXT_DEPENDENCY = Depends(require_admin_context)


def _services(request: Request) -> ControlPlaneAdminServices:
    services = getattr(request.app.state, "control_plane_admin_services", None)
    if not isinstance(services, ControlPlaneAdminServices):
        raise GatewayHttpError(500, "gateway_internal_error", "Internal server error.")
    return services


@router.get(
    "/api-keys/{api_key_id}/permissions",
    response_model=ApiKeyPermissionList,
)
async def list_api_key_permissions(
    api_key_id: UUID,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> ApiKeyPermissionList:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _services(request).permissions.list(
                    sync,
                    tenant_id=context.tenant_id,
                    api_key_id=api_key_id,
                )
            )
    except (PermissionApiKeyNotFoundError, PermissionTargetNotFoundError):
        resource_not_found()


@router.put("/api-keys/{api_key_id}/permissions", status_code=200)
async def replace_api_key_permissions(
    api_key_id: UUID,
    payload: ApiKeyPermissionReplacement,
    request: Request,
    context: AdminContext = ADMIN_CONTEXT_DEPENDENCY,
) -> Response:
    try:
        async with request.app.state.db_engine.begin() as connection:
            await connection.run_sync(
                lambda sync: _services(request).permissions.replace(
                    sync,
                    tenant_id=context.tenant_id,
                    api_key_id=api_key_id,
                    permissions=payload.permissions,
                )
            )
            await connection.run_sync(
                lambda sync: mutation_coordinator.record_success(
                    sync,
                    context=context,
                    action=AuditAction.API_KEY_PERMISSIONS_REPLACE,
                    resource_type=ResourceType.API_KEY,
                    resource_id=api_key_id,
                )
            )
        return Response(status_code=200)
    except (PermissionApiKeyNotFoundError, PermissionTargetNotFoundError):
        resource_not_found()
    except (PermissionConflictError, IntegrityError):
        resource_conflict()
