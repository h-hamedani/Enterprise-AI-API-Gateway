from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.exc import IntegrityError

from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.control_plane.llm_registry import (
    LlmRegistryAdminService,
    LlmRegistryConflictError,
    LlmRegistryNotFoundError,
    PriceWindowConflictError,
)
from app.core.errors import (
    GatewayHttpError,
    invalid_request,
    resource_conflict,
    resource_not_found,
)
from app.schemas.control_plane import *

router = APIRouter(prefix="/api/v1/admin/llm")
AUTH = Depends(require_admin_context)


def _svc(request):
    service = getattr(request.app.state, "llm_registry_service", None)
    if not isinstance(service, LlmRegistryAdminService):
        raise GatewayHttpError(500, "gateway_internal_error", "Internal server error.")
    return service


def _map(error):
    if isinstance(error, LlmRegistryNotFoundError):
        resource_not_found()
    if isinstance(error, (LlmRegistryConflictError, IntegrityError)):
        resource_conflict()
    if isinstance(error, ValueError) and str(error) == "Invalid pagination cursor.":
        invalid_request(param="cursor")
    if isinstance(error, ValueError):
        invalid_request()
    raise error


def _map_price_error(error):
    if isinstance(error, LlmRegistryNotFoundError):
        resource_not_found()
    if isinstance(error, (PriceWindowConflictError, IntegrityError)):
        raise GatewayHttpError(409, "price_window_conflict", "Price window conflict.")
    if isinstance(error, ValueError):
        invalid_request()
    raise error


async def _read(request, fn):
    try:
        async with request.app.state.db_engine.connect() as c:
            return await c.run_sync(fn)
    except (
        LlmRegistryNotFoundError,
        LlmRegistryConflictError,
        IntegrityError,
        ValueError,
    ) as e:
        _map(e)


async def _write(request, fn):
    try:
        async with request.app.state.db_engine.begin() as c:
            return await c.run_sync(fn)
    except (
        LlmRegistryNotFoundError,
        LlmRegistryConflictError,
        IntegrityError,
        ValueError,
    ) as e:
        _map(e)


async def _write_price(request, fn):
    try:
        async with request.app.state.db_engine.begin() as connection:
            return await connection.run_sync(fn)
    except (
        LlmRegistryNotFoundError,
        PriceWindowConflictError,
        IntegrityError,
        ValueError,
    ) as error:
        _map_price_error(error)


@router.get("/providers")
async def list_providers(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = AUTH,
):
    return await _read(
        request,
        lambda c: _svc(request).list_providers(c, context.tenant_id, limit, cursor),
    )


@router.post("/providers", response_model=LlmProviderResponse, status_code=201)
async def create_provider(
    payload: LlmProviderCreate, request: Request, context: AdminContext = AUTH
):
    return await _write(
        request, lambda c: _svc(request).create_provider(c, context.tenant_id, payload)
    )


@router.get("/providers/{provider_id}", response_model=LlmProviderResponse)
async def get_provider(
    provider_id: UUID, request: Request, context: AdminContext = AUTH
):
    return await _read(
        request, lambda c: _svc(request).get_provider(c, context.tenant_id, provider_id)
    )


@router.patch("/providers/{provider_id}", response_model=LlmProviderResponse)
async def patch_provider(
    provider_id: UUID,
    payload: LlmProviderPatch,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write(
        request,
        lambda c: _svc(request).patch_provider(
            c, context.tenant_id, provider_id, payload
        ),
    )


@router.get("/targets")
async def list_targets(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = AUTH,
):
    return await _read(
        request,
        lambda c: _svc(request).list_targets(c, context.tenant_id, limit, cursor),
    )


@router.post("/targets", response_model=LlmTargetResponse, status_code=201)
async def create_target(
    payload: LlmTargetCreate, request: Request, context: AdminContext = AUTH
):
    return await _write(
        request, lambda c: _svc(request).create_target(c, context.tenant_id, payload)
    )


@router.get("/targets/{target_id}", response_model=LlmTargetResponse)
async def get_target(target_id: UUID, request: Request, context: AdminContext = AUTH):
    return await _read(
        request, lambda c: _svc(request).get_target(c, context.tenant_id, target_id)
    )


@router.patch("/targets/{target_id}", response_model=LlmTargetResponse)
async def patch_target(
    target_id: UUID,
    payload: LlmTargetPatch,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write(
        request,
        lambda c: _svc(request).patch_target(c, context.tenant_id, target_id, payload),
    )


@router.get("/models")
async def list_models(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = AUTH,
):
    return await _read(
        request,
        lambda c: _svc(request).list_models(c, context.tenant_id, limit, cursor),
    )


@router.post("/models", response_model=LlmModelResponse, status_code=201)
async def create_model(
    payload: LlmModelCreate, request: Request, context: AdminContext = AUTH
):
    return await _write(
        request, lambda c: _svc(request).create_model(c, context.tenant_id, payload)
    )


@router.get("/models/{model_id}", response_model=LlmModelResponse)
async def get_model(model_id: UUID, request: Request, context: AdminContext = AUTH):
    return await _read(
        request, lambda c: _svc(request).get_model(c, context.tenant_id, model_id)
    )


@router.patch("/models/{model_id}", response_model=LlmModelResponse)
async def patch_model(
    model_id: UUID,
    payload: LlmModelPatch,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write(
        request,
        lambda c: _svc(request).patch_model(c, context.tenant_id, model_id, payload),
    )


@router.put("/models/{model_id}/capabilities", status_code=200)
async def replace_capabilities(
    model_id: UUID,
    payload: CapabilityReplacement,
    request: Request,
    context: AdminContext = AUTH,
):
    await _write(
        request,
        lambda c: _svc(request).replace_capabilities(
            c, context.tenant_id, model_id, payload.capabilities
        ),
    )
    return Response(status_code=200)


@router.get("/aliases")
async def list_aliases(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    context: AdminContext = AUTH,
):
    return await _read(
        request,
        lambda c: _svc(request).list_aliases(c, context.tenant_id, limit, cursor),
    )


@router.post("/aliases", response_model=LlmAliasResponse, status_code=201)
async def create_alias(
    payload: LlmAliasCreate, request: Request, context: AdminContext = AUTH
):
    return await _write(
        request, lambda c: _svc(request).create_alias(c, context.tenant_id, payload)
    )


@router.get("/aliases/{alias_id}", response_model=LlmAliasResponse)
async def get_alias(alias_id: UUID, request: Request, context: AdminContext = AUTH):
    return await _read(
        request, lambda c: _svc(request).get_alias(c, context.tenant_id, alias_id)
    )


@router.patch("/aliases/{alias_id}", response_model=LlmAliasResponse)
async def patch_alias(
    alias_id: UUID,
    payload: LlmAliasPatch,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write(
        request,
        lambda c: _svc(request).patch_alias(c, context.tenant_id, alias_id, payload),
    )


@router.put("/aliases/{alias_id}/targets", status_code=200)
async def replace_alias_targets(
    alias_id: UUID,
    payload: AliasTargetReplacement,
    request: Request,
    context: AdminContext = AUTH,
):
    await _write(
        request,
        lambda c: _svc(request).replace_alias_targets(
            c, context.tenant_id, alias_id, payload.targets
        ),
    )
    return Response(status_code=200)


@router.get("/models/{model_id}/prices", response_model=list[ModelPriceResponse])
async def list_prices(model_id: UUID, request: Request, context: AdminContext = AUTH):
    return await _read(
        request, lambda c: _svc(request).list_prices(c, context.tenant_id, model_id)
    )


@router.post(
    "/models/{model_id}/prices", response_model=ModelPriceResponse, status_code=201
)
async def create_price(
    model_id: UUID,
    payload: ModelPriceCreate,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write_price(
        request,
        lambda c: _svc(request).create_price(c, context.tenant_id, model_id, payload),
    )


price_router = APIRouter(prefix="/api/v1/admin")


@price_router.patch("/model-prices/{price_id}", response_model=ModelPriceResponse)
async def patch_price(
    price_id: UUID,
    payload: ModelPricePatch,
    request: Request,
    context: AdminContext = AUTH,
):
    return await _write_price(
        request,
        lambda c: _svc(request).patch_price(c, context.tenant_id, price_id, payload),
    )
