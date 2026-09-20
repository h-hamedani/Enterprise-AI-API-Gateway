from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from sqlalchemy import delete, insert, select
from sqlalchemy.engine import Connection

from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository
from app.schemas.control_plane import (
    ApiKeyPermission,
    ApiKeyPermissionList,
    ApiKeyPermissionWrite,
)


class PermissionValidationError(ValueError):
    pass


class PermissionTargetNotFoundError(PermissionValidationError):
    pass


class PermissionApiKeyNotFoundError(PermissionValidationError):
    pass


class PermissionConflictError(PermissionValidationError):
    pass


class PolymorphicPermissionValidator:
    _TABLES: ClassVar[dict[str, str]] = {
        "SERVICE": "normal_api_services",
        "ROUTE": "normal_api_routes",
        "LLM_ALIAS": "llm_aliases",
        "LLM_MODEL": "llm_models",
    }

    def validate(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        resource_type: str,
        resource_id: UUID,
        action: str,
    ) -> None:
        table_name = self._TABLES.get(resource_type)
        if table_name is None or action != "INVOKE":
            raise PermissionValidationError("Unsupported permission scope.")
        repository = TenantScopedRepository(Base.metadata.tables[table_name])
        if (
            repository.get(connection, tenant_id=tenant_id, resource_id=resource_id)
            is None
        ):
            raise PermissionTargetNotFoundError("Permission resource was not found.")


class PermissionAdminService:
    def __init__(self, validator: PolymorphicPermissionValidator | None = None) -> None:
        self._validator = validator or PolymorphicPermissionValidator()
        self._table = Base.metadata.tables["api_key_permissions"]
        self._api_keys = TenantScopedRepository(Base.metadata.tables["api_keys"])

    def list(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        api_key_id: UUID,
    ) -> ApiKeyPermissionList:
        self._require_api_key(connection, tenant_id=tenant_id, api_key_id=api_key_id)
        rows = (
            connection.execute(
                select(self._table)
                .where(
                    self._table.c.tenant_id == tenant_id,
                    self._table.c.api_key_id == api_key_id,
                )
                .order_by(
                    self._table.c.resource_type,
                    self._table.c.resource_id,
                    self._table.c.action,
                    self._table.c.id,
                )
            )
            .mappings()
            .all()
        )
        return ApiKeyPermissionList(
            data=[
                ApiKeyPermission(
                    id=row["id"],
                    resource_type=_enum_value(row["resource_type"]),
                    resource_id=row["resource_id"],
                    action=_enum_value(row["action"]),
                )
                for row in rows
            ]
        )

    def replace(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        api_key_id: UUID,
        permissions: list[ApiKeyPermissionWrite],
    ) -> None:
        self._require_api_key(connection, tenant_id=tenant_id, api_key_id=api_key_id)
        identities = [
            (item.resource_type, item.resource_id, item.action) for item in permissions
        ]
        if len(identities) != len(set(identities)):
            raise PermissionConflictError("Duplicate permission assignment.")
        for permission in permissions:
            self._validator.validate(
                connection,
                tenant_id=tenant_id,
                resource_type=permission.resource_type,
                resource_id=permission.resource_id,
                action=permission.action,
            )

        connection.execute(
            delete(self._table).where(
                self._table.c.tenant_id == tenant_id,
                self._table.c.api_key_id == api_key_id,
            )
        )
        timestamp = datetime.now(UTC)
        if permissions:
            connection.execute(
                insert(self._table),
                [
                    {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "api_key_id": api_key_id,
                        "resource_type": permission.resource_type,
                        "resource_id": permission.resource_id,
                        "action": permission.action,
                        "created_at": timestamp,
                    }
                    for permission in permissions
                ],
            )

    def _require_api_key(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        api_key_id: UUID,
    ) -> None:
        if (
            self._api_keys.get(connection, tenant_id=tenant_id, resource_id=api_key_id)
            is None
        ):
            raise PermissionApiKeyNotFoundError("API key was not found.")


def _enum_value(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


class RateScopeValidator:
    def validate_admin_token(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_token_id: UUID,
    ) -> None:
        repository = TenantScopedRepository(Base.metadata.tables["admin_tokens"])
        if (
            repository.get(
                connection,
                tenant_id=tenant_id,
                resource_id=admin_token_id,
            )
            is None
        ):
            raise PermissionValidationError("Rate-limit scope was not found.")
