from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import and_, desc, insert, or_, select
from sqlalchemy.engine import Connection, RowMapping

from app.core.idempotency import (
    ClaimState,
    IdempotencyCoordinator,
    IdempotencyRepository,
    StoredHttpResponse,
)
from app.core.pagination import CursorPosition, PaginationCursorCodec
from app.core.security.credentials import CredentialHasher
from app.core.security.idempotency import IdempotencyDigester
from app.core.security.secrets import SecretService
from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository
from app.schemas.control_plane import (
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyMetadata,
    ApiKeyPage,
    ApplicationCreate,
    ApplicationPage,
    ApplicationPatch,
    ApplicationResponse,
)


class ResourceNotFoundError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MutationHttpResult:
    response: StoredHttpResponse
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ControlPlaneAdminServices:
    applications: ApplicationAdminService
    api_keys: ApiKeyAdminService


def _status_value(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _application(row: RowMapping) -> ApplicationResponse:
    return ApplicationResponse(
        id=row["id"], name=row["name"], status=_status_value(row["status"])
    )


def _api_key(row: RowMapping) -> ApiKeyMetadata:
    return ApiKeyMetadata(
        id=row["id"],
        name=row["name"],
        key_prefix=row["key_prefix"],
        status=_status_value(row["status"]),
        expires_at=row["expires_at"],
    )


def _json_response(model: BaseModel, *, status_code: int) -> StoredHttpResponse:
    body = json.dumps(
        model.model_dump(mode="json"),
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return StoredHttpResponse(
        status_code,
        body,
        (("Content-Type", "application/json"),),
    )


class ApplicationAdminService:
    ENDPOINT_CREATE = "application.create"

    def __init__(
        self,
        cursor_codec: PaginationCursorCodec,
        idempotency: IdempotencyCoordinator,
    ) -> None:
        self._table = Base.metadata.tables["applications"]
        self._repository = TenantScopedRepository(self._table)
        self._cursor_codec = cursor_codec
        self._idempotency = idempotency

    def create(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        request: ApplicationCreate,
        raw_idempotency_key: str | None,
    ) -> MutationHttpResult:
        fingerprint = self._idempotency.fingerprint(
            method="POST",
            endpoint_key=self.ENDPOINT_CREATE,
            body=request.model_dump(mode="json"),
        )
        claim = None
        if raw_idempotency_key is not None:
            claim = self._idempotency.claim(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.ENDPOINT_CREATE,
                raw_key=raw_idempotency_key,
                request_fingerprint=fingerprint,
            )
            if claim.state is ClaimState.REPLAY:
                if claim.response is None:
                    raise RuntimeError("Idempotency replay response is unavailable.")
                return MutationHttpResult(claim.response, replayed=True)

        timestamp = datetime.now(UTC)
        application_id = uuid4()
        connection.execute(
            insert(self._table).values(
                id=application_id,
                tenant_id=tenant_id,
                name=request.name,
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        response = _json_response(
            ApplicationResponse(id=application_id, name=request.name, status="ACTIVE"),
            status_code=201,
        )
        if claim is not None:
            if claim.record_id is None:
                raise RuntimeError("Idempotency claim has no record identifier.")
            self._idempotency.complete(
                connection,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=self.ENDPOINT_CREATE,
                record_id=claim.record_id,
                response=response,
            )
        return MutationHttpResult(response)

    def get(
        self, connection: Connection, *, tenant_id: UUID, application_id: UUID
    ) -> ApplicationResponse:
        row = self._repository.get(
            connection, tenant_id=tenant_id, resource_id=application_id
        )
        if row is None:
            raise ResourceNotFoundError("Application was not found.")
        return _application(row)

    def update(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        application_id: UUID,
        patch: ApplicationPatch,
    ) -> ApplicationResponse:
        values = patch.model_dump(exclude_none=True)
        values["updated_at"] = datetime.now(UTC)
        if not self._repository.update(
            connection,
            tenant_id=tenant_id,
            resource_id=application_id,
            values=values,
        ):
            raise ResourceNotFoundError("Application was not found.")
        return self.get(connection, tenant_id=tenant_id, application_id=application_id)

    def list(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        limit: int,
        cursor: str | None,
    ) -> ApplicationPage:
        statement = select(self._table).where(self._table.c.tenant_id == tenant_id)
        if cursor is not None:
            position = self._cursor_codec.decode(cursor, tenant_id=tenant_id)
            statement = statement.where(
                or_(
                    self._table.c.created_at < position.created_at,
                    and_(
                        self._table.c.created_at == position.created_at,
                        self._table.c.id < position.resource_id,
                    ),
                )
            )
        rows = (
            connection.execute(
                statement.order_by(
                    desc(self._table.c.created_at), desc(self._table.c.id)
                ).limit(limit + 1)
            )
            .mappings()
            .all()
        )
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        next_cursor = None
        if has_more:
            last = page_rows[-1]
            next_cursor = self._cursor_codec.encode(
                CursorPosition(tenant_id, last["created_at"], last["id"])
            )
        return ApplicationPage(
            data=[_application(row) for row in page_rows], next_cursor=next_cursor
        )


class ApiKeyAdminService:
    ENDPOINT_CREATE = "api-key.create"

    def __init__(
        self,
        credential_hasher: CredentialHasher,
        cursor_codec: PaginationCursorCodec,
        idempotency: IdempotencyCoordinator,
    ) -> None:
        self._table = Base.metadata.tables["api_keys"]
        self._applications = TenantScopedRepository(
            Base.metadata.tables["applications"]
        )
        self._repository = TenantScopedRepository(self._table)
        self._credential_hasher = credential_hasher
        self._cursor_codec = cursor_codec
        self._idempotency = idempotency

    def create(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        request: ApiKeyCreate,
        raw_idempotency_key: str,
    ) -> MutationHttpResult:
        if (
            self._applications.get(
                connection,
                tenant_id=tenant_id,
                resource_id=request.application_id,
            )
            is None
        ):
            raise ResourceNotFoundError("Application was not found.")
        fingerprint = self._idempotency.fingerprint(
            method="POST",
            endpoint_key=self.ENDPOINT_CREATE,
            body=request.model_dump(mode="json"),
        )
        claim = self._idempotency.claim(
            connection,
            tenant_id=tenant_id,
            admin_user_id=admin_user_id,
            endpoint_key=self.ENDPOINT_CREATE,
            raw_key=raw_idempotency_key,
            request_fingerprint=fingerprint,
        )
        if claim.state is ClaimState.REPLAY:
            if claim.response is None:
                raise RuntimeError("Idempotency replay response is unavailable.")
            return MutationHttpResult(claim.response, replayed=True)
        if claim.record_id is None:
            raise RuntimeError("Idempotency claim has no record identifier.")

        issued = self._credential_hasher.issue("gw_")
        timestamp = datetime.now(UTC)
        api_key_id = uuid4()
        connection.execute(
            insert(self._table).values(
                id=api_key_id,
                tenant_id=tenant_id,
                application_id=request.application_id,
                name=request.name,
                key_prefix=issued.safe_prefix,
                key_hash=issued.verifier,
                status="ACTIVE",
                expires_at=request.expires_at,
                revoked_at=None,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        response = _json_response(
            ApiKeyCreated(
                id=api_key_id,
                name=request.name,
                key_prefix=issued.safe_prefix,
                status="ACTIVE",
                expires_at=request.expires_at,
                key=issued.raw,
            ),
            status_code=201,
        )
        self._idempotency.complete(
            connection,
            tenant_id=tenant_id,
            admin_user_id=admin_user_id,
            endpoint_key=self.ENDPOINT_CREATE,
            record_id=claim.record_id,
            response=response,
        )
        return MutationHttpResult(response)

    def revoke(
        self, connection: Connection, *, tenant_id: UUID, api_key_id: UUID
    ) -> ApiKeyMetadata:
        row = self._repository.get(
            connection, tenant_id=tenant_id, resource_id=api_key_id
        )
        if row is None:
            raise ResourceNotFoundError("API key was not found.")
        if _status_value(row["status"]) != "REVOKED":
            timestamp = datetime.now(UTC)
            self._repository.update(
                connection,
                tenant_id=tenant_id,
                resource_id=api_key_id,
                values={
                    "status": "REVOKED",
                    "revoked_at": timestamp,
                    "updated_at": timestamp,
                },
            )
            row = self._repository.get(
                connection, tenant_id=tenant_id, resource_id=api_key_id
            )
            if row is None:
                raise ResourceNotFoundError("API key was not found.")
        return _api_key(row)

    def list(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        limit: int,
        cursor: str | None,
    ) -> ApiKeyPage:
        statement = select(self._table).where(self._table.c.tenant_id == tenant_id)
        if cursor is not None:
            position = self._cursor_codec.decode(cursor, tenant_id=tenant_id)
            statement = statement.where(
                or_(
                    self._table.c.created_at < position.created_at,
                    and_(
                        self._table.c.created_at == position.created_at,
                        self._table.c.id < position.resource_id,
                    ),
                )
            )
        rows = (
            connection.execute(
                statement.order_by(
                    desc(self._table.c.created_at), desc(self._table.c.id)
                ).limit(limit + 1)
            )
            .mappings()
            .all()
        )
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        next_cursor = None
        if has_more:
            last = page_rows[-1]
            next_cursor = self._cursor_codec.encode(
                CursorPosition(tenant_id, last["created_at"], last["id"])
            )
        return ApiKeyPage(
            data=[_api_key(row) for row in page_rows], next_cursor=next_cursor
        )


def create_control_plane_admin_services(
    *,
    credential_hmac_key: bytes,
    idempotency_hmac_key: bytes,
    encryption_keys: dict[int, bytes],
    current_encryption_key_version: int,
) -> ControlPlaneAdminServices:
    idempotency = IdempotencyCoordinator(
        IdempotencyRepository(IdempotencyDigester(idempotency_hmac_key)),
        SecretService(
            encryption_keys,
            current_key_version=current_encryption_key_version,
        ),
    )
    cursor_key = hmac.new(
        idempotency_hmac_key,
        b"control-plane-pagination-cursor-v1",
        hashlib.sha256,
    ).digest()
    cursor_codec = PaginationCursorCodec(cursor_key)
    return ControlPlaneAdminServices(
        applications=ApplicationAdminService(cursor_codec, idempotency),
        api_keys=ApiKeyAdminService(
            CredentialHasher(credential_hmac_key), cursor_codec, idempotency
        ),
    )
