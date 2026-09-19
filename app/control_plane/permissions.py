from __future__ import annotations

from typing import ClassVar
from uuid import UUID

from sqlalchemy.engine import Connection

from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository


class PermissionValidationError(ValueError):
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
            raise PermissionValidationError("Permission resource was not found.")


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
