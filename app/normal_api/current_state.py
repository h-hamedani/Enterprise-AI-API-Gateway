"""R18 process-local current state; no request-time persistence fallback."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry
from app.core.errors import GatewayHttpError
from app.persistence.models import Base

CONSUMER = "normal_api_current_state"


def freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True, repr=False)
class TenantProjection:
    tenant_id: UUID
    services: tuple[Mapping, ...]
    routes: tuple[Mapping, ...]
    credentials: tuple[Mapping, ...]
    identity: object = field(default_factory=object)


@dataclass(frozen=True, slots=True, repr=False)
class ConfigurationSnapshot:
    tenant_id: UUID
    observed_version: int
    projection: TenantProjection


class CurrentStateRepository:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def load(self, tenant_id: UUID) -> TenantProjection:
        tables = Base.metadata.tables
        services = tables["normal_api_services"]
        routes = tables["normal_api_routes"]
        credentials = tables["service_credentials"]
        async with self.engine.connect() as connection:
            service_rows = (
                (
                    await connection.execute(
                        select(services).where(services.c.tenant_id == tenant_id)
                    )
                )
                .mappings()
                .all()
            )
            route_rows = (
                (
                    await connection.execute(
                        select(routes).where(routes.c.tenant_id == tenant_id)
                    )
                )
                .mappings()
                .all()
            )
            credential_rows = (
                (
                    await connection.execute(
                        select(
                            credentials.c.id,
                            credentials.c.tenant_id,
                            credentials.c.service_id,
                            credentials.c.auth_type,
                            credentials.c.header_name,
                            credentials.c.key_version,
                        ).where(
                            credentials.c.tenant_id == tenant_id,
                            credentials.c.status == "ACTIVE",
                        )
                    )
                )
                .mappings()
                .all()
            )
        active = {row["id"] for row in service_rows if row["status"] == "ACTIVE"}
        return TenantProjection(
            tenant_id,
            tuple(freeze(row) for row in service_rows if row["id"] in active),
            tuple(
                freeze(row)
                for row in route_rows
                if row["status"] == "ACTIVE" and row["service_id"] in active
            ),
            tuple(
                freeze(row) for row in credential_rows if row["service_id"] in active
            ),
        )


class NormalApiCurrentState:
    def __init__(
        self,
        registry: InvalidationRegistry,
        load: Callable[[UUID], Awaitable[TenantProjection]],
    ):
        self.registry = registry
        self.load = load
        self._published: dict[UUID, TenantProjection] = {}
        self._candidates: dict[UUID, TenantProjection] = {}
        self._closed = False

    async def register(self, tenant_id: UUID) -> None:
        if self._closed:
            raise RuntimeError("Current-state consumer is closed.")
        # Registration is idempotent and serialized with capture/reconciliation.
        await self.registry.ensure_tenant_consumer(
            tenant_id,
            CONSUMER,
            frozenset({"SERVICE", "ROUTE"}),
            self._invalidate,
            self.reconcile_current_state,
            lambda: self._publish(tenant_id),
        )

    def _invalidate(self, event: InvalidationEvent) -> None:
        self._candidates.pop(event.tenant_id, None)

    async def reconcile_current_state(self, tenant_id: UUID) -> None:
        if self._closed:
            raise RuntimeError("Current-state consumer is closed.")
        candidate = await self.load(tenant_id)
        service_ids = {row["id"] for row in candidate.services}
        if (
            candidate.tenant_id != tenant_id
            or any(
                row.get("tenant_id") != tenant_id
                for rows in (
                    candidate.services,
                    candidate.routes,
                    candidate.credentials,
                )
                for row in rows
            )
            or any(
                row["service_id"] not in service_ids
                for rows in (candidate.routes, candidate.credentials)
                for row in rows
            )
        ):
            raise ValueError("Incomplete current state.")
        # Freeze even injected loaders; callers never receive mutable row objects.
        self._candidates[tenant_id] = TenantProjection(
            tenant_id,
            tuple(freeze(row) for row in candidate.services),
            tuple(freeze(row) for row in candidate.routes),
            tuple(freeze(row) for row in candidate.credentials),
        )

    def _publish(self, tenant_id: UUID) -> None:
        self._published[tenant_id] = self._candidates.pop(tenant_id)

    async def capture(self, tenant_id: UUID) -> ConfigurationSnapshot:
        def read(version: int):
            projection = self._published.get(tenant_id)
            if self._closed or projection is None:
                return None
            return ConfigurationSnapshot(tenant_id, version, projection)

        snapshot = await self.registry.capture_required(tenant_id, CONSUMER, read)
        if snapshot is None:
            raise GatewayHttpError(
                500, "gateway_internal_error", "Internal gateway error."
            )
        return snapshot

    async def close(self) -> None:
        self._closed = True
        self._published.clear()
        self._candidates.clear()
