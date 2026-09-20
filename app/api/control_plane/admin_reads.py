from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import text

from app.api.dependencies import require_admin_context
from app.control_plane.admin_reads import AdminReadService
from app.control_plane.auth import AdminContext
from app.core.errors import GatewayHttpError, invalid_request
from app.schemas.control_plane import (
    AdminHealthResponse,
    AuditPage,
    ConfigVersionResponse,
    DependencyHealth,
)

router = APIRouter(prefix="/api/v1/admin")
AUTH = Depends(require_admin_context)


def _service(request: Request) -> AdminReadService:
    service = getattr(request.app.state, "admin_read_service", None)
    if not isinstance(service, AdminReadService):
        raise GatewayHttpError(500, "gateway_internal_error", "Internal server error.")
    return service


@router.get("/audit-logs", response_model=AuditPage, response_model_exclude_none=True)
async def list_audit_logs(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = AUTH,
) -> AuditPage:
    try:
        async with request.app.state.db_engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: _service(request).list_audit_logs(
                    sync,
                    tenant_id=context.tenant_id,
                    limit=limit,
                    cursor=cursor,
                )
            )
    except ValueError as error:
        if str(error) == "Invalid pagination cursor.":
            invalid_request(param="cursor")
        raise


@router.get("/config-version", response_model=ConfigVersionResponse)
async def get_config_version(
    request: Request,
    context: AdminContext = AUTH,
) -> ConfigVersionResponse:
    async with request.app.state.db_engine.connect() as connection:
        result = await connection.run_sync(
            lambda sync: _service(request).get_config_version(
                sync, tenant_id=context.tenant_id
            )
        )
    return ConfigVersionResponse(config_version=result.config_version)


@router.get("/health", response_model=AdminHealthResponse)
async def get_admin_health(
    request: Request,
    context: AdminContext = AUTH,
) -> AdminHealthResponse:
    del context
    async with request.app.state.db_engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    redis_health = DependencyHealth(status="AVAILABLE")
    try:
        await request.app.state.redis.ping()
    except Exception:  # noqa: BLE001 - health reports dependency state safely
        redis_health = DependencyHealth(
            status="UNAVAILABLE", reason="redis_unreachable"
        )
    degraded = redis_health.status != "AVAILABLE"
    return AdminHealthResponse(
        status="DEGRADED" if degraded else "AVAILABLE",
        degraded_mode=degraded,
        dependencies={
            "postgresql": DependencyHealth(status="AVAILABLE"),
            "redis": redis_health,
        },
    )
