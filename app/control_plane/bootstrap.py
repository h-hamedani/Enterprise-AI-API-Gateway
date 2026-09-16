from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select
from sqlalchemy.engine import Connection

from app.core.security.credentials import CredentialHasher
from app.persistence.models import Base


class BootstrapUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    tenant_id: UUID
    admin_user_id: UUID
    raw_token: str


class FirstAdminBootstrapService:
    def __init__(self, credential_hasher: CredentialHasher) -> None:
        self._credential_hasher = credential_hasher

    def bootstrap(self, connection: Connection, *, admin_name: str) -> BootstrapResult:
        tenants = Base.metadata.tables["tenants"]
        users = Base.metadata.tables["admin_users"]
        tokens = Base.metadata.tables["admin_tokens"]
        audit = Base.metadata.tables["audit_logs"]
        timestamp = datetime.now(UTC)

        connection.execute(select(func.pg_advisory_xact_lock(0x45414947)))
        active_count = connection.scalar(
            select(func.count()).select_from(users).where(users.c.status == "ACTIVE")
        )
        if active_count:
            raise BootstrapUnavailableError("An active administrator already exists.")

        tenant_id = connection.scalar(
            select(tenants.c.id).where(tenants.c.name == "default")
        )
        if tenant_id is None:
            tenant_id = uuid4()
            connection.execute(
                insert(tenants).values(
                    id=tenant_id,
                    name="default",
                    status="ACTIVE",
                    created_at=timestamp,
                    updated_at=timestamp,
                )
            )

        admin_user_id = uuid4()
        issued = self._credential_hasher.issue("adm_")
        connection.execute(
            insert(users).values(
                id=admin_user_id,
                tenant_id=tenant_id,
                name=admin_name,
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        token_id = uuid4()
        connection.execute(
            insert(tokens).values(
                id=token_id,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                token_prefix=issued.safe_prefix,
                token_hash=issued.verifier,
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        connection.execute(
            insert(audit).values(
                id=uuid4(),
                tenant_id=tenant_id,
                actor_admin_user_id=admin_user_id,
                actor_admin_token_id=token_id,
                action="ADMIN_BOOTSTRAP",
                resource_type="ADMIN_USER",
                resource_id=admin_user_id,
                result="SUCCESS",
                metadata={"token_prefix": issued.safe_prefix},
                created_at=timestamp,
            )
        )
        return BootstrapResult(tenant_id, admin_user_id, issued.raw)
