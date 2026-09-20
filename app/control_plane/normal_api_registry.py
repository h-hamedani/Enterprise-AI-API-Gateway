from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import and_, desc, insert, or_, select, update
from sqlalchemy.engine import Connection, RowMapping

from app.control_plane.application_api_keys import MutationHttpResult
from app.core.idempotency import (
    ClaimState,
    IdempotencyCoordinator,
    IdempotencyRepository,
    StoredHttpResponse,
)
from app.core.pagination import CursorPosition, PaginationCursorCodec
from app.core.security.idempotency import IdempotencyDigester
from app.core.security.secrets import SecretService
from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository
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


class RegistryResourceNotFoundError(RuntimeError):
    pass


class RegistryConflictError(RuntimeError):
    pass


_RESERVED_SLUGS = {"v1", "admin", "health"}


def _status(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _service(row: RowMapping) -> ServiceResponse:
    return ServiceResponse(
        id=row["id"],
        name=row["display_name"],
        base_url=row["upstream_base_url"],
        slug=row["slug"],
        connect_timeout_seconds=row["connect_timeout_seconds"],
        pool_timeout_seconds=row["pool_timeout_seconds"],
        write_timeout_seconds=row["write_timeout_seconds"],
        read_idle_timeout_seconds=row["read_idle_timeout_seconds"],
        pre_response_timeout_seconds=row["pre_response_timeout_seconds"],
        status=_status(row["status"]),
    )


def _route(row: RowMapping) -> RouteResponse:
    return RouteResponse(
        id=row["id"],
        service_id=row["service_id"],
        path_pattern=row["path_pattern"],
        method=row["method"],
        upstream_path_template=row["upstream_path_template"],
        header_policy=row["header_policy"],
        priority=row["priority"],
        timeout_ms=row["timeout_ms"],
        status=_status(row["status"]),
    )


def _credential(row: RowMapping) -> CredentialMetadata:
    return CredentialMetadata(
        id=row["id"],
        status=_status(row["status"]),
        secret_type=_status(row["auth_type"]),
        created_at=row["created_at"],
        rotated_at=row["rotated_at"],
    )


def _json_response(model: BaseModel, status_code: int) -> StoredHttpResponse:
    return StoredHttpResponse(
        status_code,
        json.dumps(
            model.model_dump(mode="json"), separators=(",", ":"), sort_keys=True
        ).encode(),
        (("Content-Type", "application/json"),),
    )


class NormalApiRegistryAdminService:
    SERVICE_CREATE_ENDPOINT = "normal-service.create"
    CREDENTIAL_PUT_ENDPOINT = "normal-service.credential.put"

    def __init__(
        self,
        cursor_codec: PaginationCursorCodec,
        secret_service: SecretService,
        idempotency: IdempotencyCoordinator,
    ) -> None:
        self._services = Base.metadata.tables["normal_api_services"]
        self._routes = Base.metadata.tables["normal_api_routes"]
        self._credentials = Base.metadata.tables["service_credentials"]
        self._service_repository = TenantScopedRepository(self._services)
        self._route_repository = TenantScopedRepository(self._routes)
        self._cursor_codec = cursor_codec
        self._secret_service = secret_service
        self._idempotency = idempotency

    @staticmethod
    def credential_aad(
        tenant_id: UUID, service_id: UUID, credential_id: UUID, auth_type: str
    ) -> bytes:
        return (
            f"service-credential:v1:{tenant_id}:{service_id}:"
            f"{credential_id}:{auth_type}"
        ).encode()

    def create_service(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        request: ServiceCreate,
        raw_idempotency_key: str | None,
    ) -> MutationHttpResult:
        if request.slug in _RESERVED_SLUGS:
            raise RegistryConflictError("Service slug is reserved.")
        claim = None
        if raw_idempotency_key is not None:
            fingerprint = self._idempotency.fingerprint(
                method="POST",
                endpoint_key=self.SERVICE_CREATE_ENDPOINT,
                body=request.model_dump(mode="json"),
            )
            claim = self._idempotency.claim(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.SERVICE_CREATE_ENDPOINT,
                raw_key=raw_idempotency_key,
                request_fingerprint=fingerprint,
            )
            if claim.state is ClaimState.REPLAY:
                if claim.response is None:
                    raise RuntimeError("Idempotency replay response is unavailable.")
                return MutationHttpResult(claim.response, replayed=True)

        now = datetime.now(UTC)
        service_id = uuid4()
        connection.execute(
            insert(self._services).values(
                id=service_id,
                tenant_id=tenant_id,
                slug=request.slug,
                display_name=request.name,
                upstream_base_url=request.base_url,
                status=request.status,
                request_body_limit_bytes=10 * 1024 * 1024,
                connect_timeout_seconds=request.connect_timeout_seconds,
                pool_timeout_seconds=request.pool_timeout_seconds,
                write_timeout_seconds=request.write_timeout_seconds,
                read_idle_timeout_seconds=request.read_idle_timeout_seconds,
                pre_response_timeout_seconds=request.pre_response_timeout_seconds,
                created_at=now,
                updated_at=now,
            )
        )
        response = _json_response(
            self.get_service(connection, tenant_id=tenant_id, service_id=service_id),
            201,
        )
        if claim is not None:
            if claim.record_id is None:
                raise RuntimeError("Idempotency claim has no record identifier.")
            self._idempotency.complete(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.SERVICE_CREATE_ENDPOINT,
                record_id=claim.record_id,
                response=response,
            )
        return MutationHttpResult(response)

    def get_service(
        self, connection: Connection, *, tenant_id: UUID, service_id: UUID
    ) -> ServiceResponse:
        row = self._service_repository.get(
            connection, tenant_id=tenant_id, resource_id=service_id
        )
        if row is None:
            raise RegistryResourceNotFoundError("Service was not found.")
        return _service(row)

    def patch_service(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        service_id: UUID,
        patch: ServicePatch,
    ) -> ServiceResponse:
        values = patch.model_dump(exclude_unset=True)
        if "name" in values:
            values["display_name"] = values.pop("name")
        if "base_url" in values:
            values["upstream_base_url"] = values.pop("base_url")
        values["updated_at"] = datetime.now(UTC)
        if not self._service_repository.update(
            connection,
            tenant_id=tenant_id,
            resource_id=service_id,
            values=values,
        ):
            raise RegistryResourceNotFoundError("Service was not found.")
        return self.get_service(connection, tenant_id=tenant_id, service_id=service_id)

    def list_services(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        limit: int,
        cursor: str | None,
    ) -> ServicePage:
        rows, next_cursor = self._page(
            connection, self._services, tenant_id, limit, cursor
        )
        return ServicePage(
            data=[_service(row) for row in rows], next_cursor=next_cursor
        )

    def create_route(
        self, connection: Connection, *, tenant_id: UUID, request: RouteCreate
    ) -> RouteResponse:
        self.get_service(connection, tenant_id=tenant_id, service_id=request.service_id)
        now = datetime.now(UTC)
        route_id = uuid4()
        connection.execute(
            insert(self._routes).values(
                id=route_id,
                tenant_id=tenant_id,
                **request.model_dump(),
                created_at=now,
                updated_at=now,
            )
        )
        return self.get_route(connection, tenant_id=tenant_id, route_id=route_id)

    def get_route(
        self, connection: Connection, *, tenant_id: UUID, route_id: UUID
    ) -> RouteResponse:
        row = self._route_repository.get(
            connection, tenant_id=tenant_id, resource_id=route_id
        )
        if row is None:
            raise RegistryResourceNotFoundError("Route was not found.")
        return _route(row)

    def patch_route(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        route_id: UUID,
        patch: RoutePatch,
    ) -> RouteResponse:
        values = patch.model_dump(exclude_unset=True)
        values["updated_at"] = datetime.now(UTC)
        if not self._route_repository.update(
            connection,
            tenant_id=tenant_id,
            resource_id=route_id,
            values=values,
        ):
            raise RegistryResourceNotFoundError("Route was not found.")
        return self.get_route(connection, tenant_id=tenant_id, route_id=route_id)

    def list_routes(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        limit: int,
        cursor: str | None,
    ) -> RoutePage:
        rows, next_cursor = self._page(
            connection, self._routes, tenant_id, limit, cursor
        )
        return RoutePage(data=[_route(row) for row in rows], next_cursor=next_cursor)

    def put_credential(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        service_id: UUID,
        request: ServiceCredentialWrite,
        raw_idempotency_key: str | None,
    ) -> MutationHttpResult:
        self.get_service(connection, tenant_id=tenant_id, service_id=service_id)
        claim = None
        if raw_idempotency_key is not None:
            fingerprint = self._idempotency.fingerprint(
                method="PUT",
                endpoint_key=self.CREDENTIAL_PUT_ENDPOINT,
                body={"service_id": str(service_id), **request.model_dump(mode="json")},
            )
            claim = self._idempotency.claim(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.CREDENTIAL_PUT_ENDPOINT,
                raw_key=raw_idempotency_key,
                request_fingerprint=fingerprint,
            )
            if claim.state is ClaimState.REPLAY:
                if claim.response is None:
                    raise RuntimeError("Idempotency replay response is unavailable.")
                return MutationHttpResult(claim.response, replayed=True)

        now = datetime.now(UTC)
        credential_id = uuid4()
        encrypted = None
        if request.secret is not None:
            encrypted = self._secret_service.encrypt(
                request.secret.encode(),
                aad=self.credential_aad(
                    tenant_id, service_id, credential_id, request.auth_type
                ),
            )
        connection.execute(
            select(self._credentials.c.id)
            .where(
                self._credentials.c.tenant_id == tenant_id,
                self._credentials.c.service_id == service_id,
                self._credentials.c.status == "ACTIVE",
            )
            .with_for_update()
        ).all()
        connection.execute(
            update(self._credentials)
            .where(
                self._credentials.c.tenant_id == tenant_id,
                self._credentials.c.service_id == service_id,
                self._credentials.c.status == "ACTIVE",
            )
            .values(status="DISABLED", rotated_at=now)
        )
        connection.execute(
            insert(self._credentials).values(
                id=credential_id,
                tenant_id=tenant_id,
                service_id=service_id,
                auth_type=request.auth_type,
                header_name=request.header_name,
                secret_ciphertext=None if encrypted is None else encrypted.ciphertext,
                key_version=None if encrypted is None else encrypted.key_version,
                status="ACTIVE",
                created_at=now,
                rotated_at=None,
            )
        )
        row = (
            connection.execute(
                select(self._credentials).where(
                    self._credentials.c.tenant_id == tenant_id,
                    self._credentials.c.id == credential_id,
                )
            )
            .mappings()
            .one()
        )
        response = _json_response(_credential(row), 200)
        if claim is not None:
            if claim.record_id is None:
                raise RuntimeError("Idempotency claim has no record identifier.")
            self._idempotency.complete(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.CREDENTIAL_PUT_ENDPOINT,
                record_id=claim.record_id,
                response=response,
            )
        return MutationHttpResult(response)

    def _page(self, connection, table, tenant_id, limit, cursor):
        statement = select(table).where(table.c.tenant_id == tenant_id)
        if cursor is not None:
            position = self._cursor_codec.decode(cursor, tenant_id=tenant_id)
            statement = statement.where(
                or_(
                    table.c.created_at < position.created_at,
                    and_(
                        table.c.created_at == position.created_at,
                        table.c.id < position.resource_id,
                    ),
                )
            )
        rows = (
            connection.execute(
                statement.order_by(desc(table.c.created_at), desc(table.c.id)).limit(
                    limit + 1
                )
            )
            .mappings()
            .all()
        )
        page_rows = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page_rows[-1]
            next_cursor = self._cursor_codec.encode(
                CursorPosition(tenant_id, last["created_at"], last["id"])
            )
        return page_rows, next_cursor


def create_normal_api_registry_service(
    *,
    idempotency_hmac_key: bytes,
    encryption_keys: dict[int, bytes],
    current_encryption_key_version: int,
) -> NormalApiRegistryAdminService:
    secret_service = SecretService(
        encryption_keys, current_key_version=current_encryption_key_version
    )
    idempotency = IdempotencyCoordinator(
        IdempotencyRepository(IdempotencyDigester(idempotency_hmac_key)),
        secret_service,
    )
    cursor_key = hmac.new(
        idempotency_hmac_key,
        b"normal-api-registry-pagination-cursor-v1",
        hashlib.sha256,
    ).digest()
    return NormalApiRegistryAdminService(
        PaginationCursorCodec(cursor_key), secret_service, idempotency
    )
