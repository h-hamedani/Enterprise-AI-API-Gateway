"""Gateway credentials: existing HMAC/verifier and strict TTL=0 status lookup."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.errors import authentication_error
from app.core.security.credentials import CredentialAuthenticator, CredentialHasher
from app.normal_api.ingress import unavailable
from app.persistence.models import Base


@dataclass(frozen=True, slots=True, repr=False)
class GatewayIdentity:
    tenant_id: UUID
    application_id: UUID
    api_key_id: UUID
    permissions: frozenset[tuple[str, UUID]]


class GatewayAuthenticator:
    def __init__(self, engine: AsyncEngine, hasher: CredentialHasher):
        self.engine = engine
        self.hasher = hasher
        self.verifier = CredentialAuthenticator(hasher)

    async def authenticate(self, scope: dict) -> GatewayIdentity:
        headers = [
            value
            for name, value in scope.get("headers", ())
            if name.lower() == b"authorization"
        ]
        if len(headers) != 1:
            authentication_error()
        try:
            scheme, _, raw = headers[0].decode("ascii").partition(" ")
        except UnicodeDecodeError:
            authentication_error()
        if scheme.lower() != "bearer" or not re.fullmatch(r"gw_[A-Za-z0-9_-]{43}", raw):
            authentication_error()
        tables = Base.metadata.tables
        keys, apps, tenants = (
            tables[name] for name in ("api_keys", "applications", "tenants")
        )
        statement = (
            select(
                keys.c.id,
                keys.c.tenant_id,
                keys.c.application_id,
                keys.c.key_hash,
                keys.c.status,
                keys.c.expires_at,
                keys.c.revoked_at,
                apps.c.status.label("application_status"),
                tenants.c.status.label("tenant_status"),
            )
            .select_from(
                keys.join(
                    apps,
                    (apps.c.tenant_id == keys.c.tenant_id)
                    & (apps.c.id == keys.c.application_id),
                ).join(tenants, tenants.c.id == keys.c.tenant_id)
            )
            .where(keys.c.key_prefix == raw[:12])
        )
        try:
            async with self.engine.connect() as connection:
                candidates = (await connection.execute(statement)).mappings().all()
                matches = [
                    row
                    for row in candidates
                    if self.hasher.verify(raw, row["key_hash"])
                ]
                if not candidates:
                    self.hasher.verify(raw, "0" * 64)
                if len(matches) != 1:
                    authentication_error()
                row = matches[0]
                if (
                    not self.verifier.authenticate(
                        raw,
                        verifier=row["key_hash"],
                        status=row["status"],
                        expires_at=row["expires_at"],
                        now=datetime.now(UTC),
                    )
                    or row["revoked_at"] is not None
                    or row["application_status"] != "ACTIVE"
                    or row["tenant_status"] != "ACTIVE"
                ):
                    authentication_error()
                permissions = tables["api_key_permissions"]
                grants = (
                    await connection.execute(
                        select(
                            permissions.c.resource_type, permissions.c.resource_id
                        ).where(
                            permissions.c.tenant_id == row["tenant_id"],
                            permissions.c.api_key_id == row["id"],
                            permissions.c.action == "INVOKE",
                        )
                    )
                ).all()
        except SQLAlchemyError:
            unavailable()
        return GatewayIdentity(
            row["tenant_id"],
            row["application_id"],
            row["id"],
            frozenset((kind, resource_id) for kind, resource_id in grants),
        )
