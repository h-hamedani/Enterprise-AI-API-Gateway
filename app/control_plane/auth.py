from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.security.credentials import CredentialAuthenticator, CredentialHasher
from app.persistence.models import Base


class AdminAuthenticationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AdminContext:
    tenant_id: UUID
    admin_user_id: UUID
    admin_token_id: UUID
    request_id: UUID


class AdminAuthenticator:
    def __init__(self, credential_hasher: CredentialHasher) -> None:
        self._hasher = credential_hasher
        self._credential_authenticator = CredentialAuthenticator(credential_hasher)

    async def authenticate(
        self,
        engine: AsyncEngine,
        *,
        raw_token: str,
        request_id: UUID,
        now: datetime | None = None,
    ) -> AdminContext:
        tokens = Base.metadata.tables["admin_tokens"]
        users = Base.metadata.tables["admin_users"]
        checked_at = now or datetime.now(UTC)
        statement = (
            select(
                tokens.c.id.label("admin_token_id"),
                tokens.c.tenant_id,
                tokens.c.admin_user_id,
                tokens.c.token_hash,
                tokens.c.status.label("token_status"),
                tokens.c.expires_at,
                tokens.c.revoked_at,
                users.c.status.label("admin_user_status"),
            )
            .select_from(
                tokens.join(
                    users,
                    (users.c.tenant_id == tokens.c.tenant_id)
                    & (users.c.id == tokens.c.admin_user_id),
                )
            )
            .where(tokens.c.token_prefix == raw_token[:12])
        )
        async with engine.connect() as connection:
            candidates = (await connection.execute(statement)).mappings().all()

        matches = [
            candidate
            for candidate in candidates
            if self._hasher.verify(raw_token, candidate["token_hash"])
        ]
        if not candidates:
            self._hasher.verify(raw_token, "0" * 64)
        if len(matches) != 1:
            raise AdminAuthenticationError("Admin authentication failed.")

        match = matches[0]
        token_usable = self._credential_authenticator.authenticate(
            raw_token,
            verifier=match["token_hash"],
            status=match["token_status"],
            expires_at=match["expires_at"],
            now=checked_at,
        )
        if (
            not token_usable
            or match["revoked_at"] is not None
            or match["admin_user_status"] != "ACTIVE"
        ):
            raise AdminAuthenticationError("Admin authentication failed.")

        return AdminContext(
            tenant_id=match["tenant_id"],
            admin_user_id=match["admin_user_id"],
            admin_token_id=match["admin_token_id"],
            request_id=request_id,
        )
