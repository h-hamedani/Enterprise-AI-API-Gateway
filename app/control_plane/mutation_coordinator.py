from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.engine import Connection
from starlette.responses import Response

from app.control_plane.auth import AdminContext
from app.persistence.models import Base


class AuditAction(StrEnum):
    APPLICATION_CREATE = "APPLICATION_CREATE"
    APPLICATION_UPDATE = "APPLICATION_UPDATE"
    API_KEY_CREATE = "API_KEY_CREATE"
    API_KEY_REVOKE = "API_KEY_REVOKE"
    API_KEY_PERMISSIONS_REPLACE = "API_KEY_PERMISSIONS_REPLACE"
    SERVICE_CREATE = "SERVICE_CREATE"
    SERVICE_UPDATE = "SERVICE_UPDATE"
    SERVICE_CREDENTIAL_REPLACE = "SERVICE_CREDENTIAL_REPLACE"
    ROUTE_CREATE = "ROUTE_CREATE"
    ROUTE_UPDATE = "ROUTE_UPDATE"
    LLM_PROVIDER_CREATE = "LLM_PROVIDER_CREATE"
    LLM_PROVIDER_UPDATE = "LLM_PROVIDER_UPDATE"
    LLM_TARGET_CREATE = "LLM_TARGET_CREATE"
    LLM_TARGET_UPDATE = "LLM_TARGET_UPDATE"
    LLM_TARGET_CREDENTIAL_ROTATE = "LLM_TARGET_CREDENTIAL_ROTATE"
    LLM_MODEL_CREATE = "LLM_MODEL_CREATE"
    LLM_MODEL_UPDATE = "LLM_MODEL_UPDATE"
    LLM_MODEL_CAPABILITIES_REPLACE = "LLM_MODEL_CAPABILITIES_REPLACE"
    LLM_ALIAS_CREATE = "LLM_ALIAS_CREATE"
    LLM_ALIAS_UPDATE = "LLM_ALIAS_UPDATE"
    LLM_ALIAS_TARGETS_REPLACE = "LLM_ALIAS_TARGETS_REPLACE"
    MODEL_PRICE_CREATE = "MODEL_PRICE_CREATE"
    MODEL_PRICE_UPDATE = "MODEL_PRICE_UPDATE"


class ResourceType(StrEnum):
    APPLICATION = "APPLICATION"
    API_KEY = "API_KEY"
    SERVICE = "SERVICE"
    ROUTE = "ROUTE"
    LLM_PROVIDER = "LLM_PROVIDER"
    LLM_TARGET = "LLM_TARGET"
    LLM_MODEL = "LLM_MODEL"
    LLM_ALIAS = "LLM_ALIAS"
    MODEL_PRICE = "MODEL_PRICE"


class MutationCoordinationError(RuntimeError):
    """Safe boundary for audit/config-version persistence failures."""


@dataclass(frozen=True, slots=True)
class CommittedMutation:
    tenant_id: UUID
    version: int
    resource_type: ResourceType
    resource_id: UUID


class MutationCoordinator:
    """Append the mutation audit and advance its tenant version in one transaction."""

    def record_success(
        self,
        connection: Connection,
        *,
        context: AdminContext,
        action: AuditAction,
        resource_type: ResourceType,
        resource_id: UUID,
    ) -> CommittedMutation:
        timestamp = datetime.now(UTC)
        try:
            self._insert_audit(
                connection,
                context=context,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                timestamp=timestamp,
            )
            version = self._increment_config_version(
                connection,
                context=context,
                timestamp=timestamp,
            )
        except Exception as error:
            raise MutationCoordinationError("Mutation coordination failed.") from error
        return CommittedMutation(
            tenant_id=context.tenant_id,
            version=version,
            resource_type=resource_type,
            resource_id=resource_id,
        )

    def _insert_audit(
        self,
        connection: Connection,
        *,
        context: AdminContext,
        action: AuditAction,
        resource_type: ResourceType,
        resource_id: UUID,
        timestamp: datetime,
    ) -> None:
        audit_logs = Base.metadata.tables["audit_logs"]
        connection.execute(
            insert(audit_logs).values(
                id=uuid4(),
                tenant_id=context.tenant_id,
                actor_admin_user_id=context.admin_user_id,
                actor_admin_token_id=context.admin_token_id,
                request_id=context.request_id,
                action=action.value,
                resource_type=resource_type.value,
                resource_id=resource_id,
                result="SUCCESS",
                metadata=None,
                created_at=timestamp,
            )
        )

    def _increment_config_version(
        self,
        connection: Connection,
        *,
        context: AdminContext,
        timestamp: datetime,
    ) -> int:
        versions = Base.metadata.tables["config_versions"]
        statement = (
            postgresql_insert(versions)
            .values(
                tenant_id=context.tenant_id,
                version=1,
                updated_at=timestamp,
                updated_by_admin_user_id=context.admin_user_id,
            )
            .on_conflict_do_update(
                index_elements=[versions.c.tenant_id],
                set_={
                    "version": versions.c.version + 1,
                    "updated_at": timestamp,
                    "updated_by_admin_user_id": context.admin_user_id,
                },
            )
            .returning(versions.c.version)
        )
        return int(connection.scalar(statement))


def response_resource_id(response: Response) -> UUID:
    value = json.loads(response.body)["id"]
    return UUID(value)


mutation_coordinator = MutationCoordinator()
