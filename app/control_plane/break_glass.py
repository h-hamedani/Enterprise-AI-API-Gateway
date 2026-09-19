from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Connection

from app.core.security.credentials import CredentialHasher
from app.persistence.models import Base


class BreakGlassFailureCategory(StrEnum):
    INVALID_RECOVERY_SECRET = "INVALID_RECOVERY_SECRET"
    ADMIN_NOT_FOUND = "ADMIN_NOT_FOUND"
    RECOVERY_TRANSACTION_FAILED = "RECOVERY_TRANSACTION_FAILED"


class BreakGlassDeniedError(RuntimeError):
    def __init__(self, failure_category: BreakGlassFailureCategory) -> None:
        super().__init__("Break-glass recovery was denied.")
        self.failure_category = failure_category


@dataclass(frozen=True, slots=True)
class BreakGlassResult:
    raw_replacement_token: str


class BreakGlassRecoveryService:
    def __init__(
        self,
        credential_hasher: CredentialHasher,
        *,
        recovery_secret_hash: str,
    ) -> None:
        self._credential_hasher = credential_hasher
        self._recovery_secret_hash = recovery_secret_hash

    def recover(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        recovery_secret: str,
        recovery_mechanism: str,
    ) -> BreakGlassResult:
        presented = hashlib.sha256(recovery_secret.encode()).hexdigest()
        if not hmac.compare_digest(presented, self._recovery_secret_hash):
            raise BreakGlassDeniedError(
                BreakGlassFailureCategory.INVALID_RECOVERY_SECRET
            )

        users = Base.metadata.tables["admin_users"]
        tokens = Base.metadata.tables["admin_tokens"]
        audit = Base.metadata.tables["audit_logs"]
        timestamp = datetime.now(UTC)
        user = connection.execute(
            select(users.c.id).where(
                users.c.tenant_id == tenant_id,
                users.c.id == admin_user_id,
            )
        ).one_or_none()
        if user is None:
            raise BreakGlassDeniedError(BreakGlassFailureCategory.ADMIN_NOT_FOUND)

        connection.execute(
            update(users)
            .where(users.c.tenant_id == tenant_id, users.c.id == admin_user_id)
            .values(status="ACTIVE", updated_at=timestamp)
        )
        previous_active_token_count = connection.execute(
            update(tokens)
            .where(
                tokens.c.tenant_id == tenant_id,
                tokens.c.admin_user_id == admin_user_id,
                tokens.c.status == "ACTIVE",
            )
            .values(status="REVOKED", revoked_at=timestamp, updated_at=timestamp)
        ).rowcount
        issued = self._credential_hasher.issue("adm_")
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
                action="ADMIN_BREAK_GLASS_RECOVERY",
                resource_type="ADMIN_USER",
                resource_id=admin_user_id,
                result="SUCCESS",
                metadata={
                    "previous_active_token_count": previous_active_token_count,
                    "replacement_token_prefix": issued.safe_prefix,
                    "recovery_mechanism": recovery_mechanism,
                },
                created_at=timestamp,
            )
        )
        return BreakGlassResult(issued.raw)
