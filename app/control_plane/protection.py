from __future__ import annotations

import hashlib
import hmac
import ipaddress
from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings
from app.core.errors import GatewayHttpError
from app.persistence.models import Base
from app.persistence.models.enums import RateScopeType
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RedisTokenBucket,
    ResolvedRatePolicy,
)
from app.redis.runtime import RedisRuntime

PRE_AUTH_REQUESTS = 20
PRE_AUTH_WINDOW_SECONDS = 60
PRE_AUTH_CLASS = "ADMIN_AUTH_PROTECTED"


def resolve_client_ip(request, trusted_proxy_cidrs: Iterable[str] = ()) -> str:
    peer = request.client.host if request.client is not None else "unknown"
    try:
        peer_ip = ipaddress.ip_address(peer)
    except ValueError:
        return peer
    trusted = []
    for cidr in trusted_proxy_cidrs:
        try:
            trusted.append(ipaddress.ip_network(cidr, strict=False))
        except ValueError:
            continue
    if not any(peer_ip in network for network in trusted):
        return peer
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded:
        return peer
    values = [value.strip() for value in forwarded.split(",")]
    if not values or any(not value for value in values):
        return peer
    try:
        return str(ipaddress.ip_address(values[0]))
    except ValueError:
        return peer


def pre_auth_policy(client_ip: str, secret: bytes) -> ResolvedRatePolicy:
    digest = hmac.new(secret, client_ip.encode("utf-8"), hashlib.sha256).hexdigest()
    scope_id = UUID(digest[:32])
    return ResolvedRatePolicy(
        policy_id=scope_id,
        tenant_id=UUID(int=0),
        scope_type=RateScopeType.ADMIN_TOKEN,
        scope_id=scope_id,
        requests_per_window=PRE_AUTH_REQUESTS,
        window_seconds=PRE_AUTH_WINDOW_SECONDS,
        key_override=f"gw:v1:adminpre:{PRE_AUTH_CLASS}:{{{digest}}}",
    )


async def enabled_admin_token_policies(
    engine: AsyncEngine, tenant_id: UUID, admin_token_id: UUID
) -> list[ResolvedRatePolicy]:
    statement = _admin_policy_statement(tenant_id, admin_token_id)
    async with engine.connect() as connection:
        rows = (await connection.execute(statement)).mappings().all()
    return _resolved_admin_policies(rows, tenant_id, admin_token_id)


def enabled_admin_token_policies_sync(
    connection, tenant_id: UUID, admin_token_id: UUID
):
    rows = (
        connection.execute(_admin_policy_statement(tenant_id, admin_token_id))
        .mappings()
        .all()
    )
    return _resolved_admin_policies(rows, tenant_id, admin_token_id)


def _admin_policy_statement(tenant_id: UUID, admin_token_id: UUID):
    table = Base.metadata.tables["rate_limit_policies"]
    return select(table).where(
        table.c.tenant_id == tenant_id,
        table.c.scope_type == RateScopeType.ADMIN_TOKEN.value,
        table.c.scope_id == admin_token_id,
        table.c.enabled.is_(True),
    )


def _resolved_admin_policies(rows, tenant_id: UUID, admin_token_id: UUID):
    return [
        ResolvedRatePolicy(
            policy_id=row["id"],
            tenant_id=tenant_id,
            scope_type=RateScopeType.ADMIN_TOKEN,
            scope_id=admin_token_id,
            requests_per_window=row["requests_per_window"],
            window_seconds=row["window_seconds"],
        )
        for row in rows
        if row["requests_per_window"] is not None and row["window_seconds"] is not None
    ]


class ControlPlaneProtection:
    def __init__(self, runtime: RedisRuntime, settings: Settings) -> None:
        self._bucket = RedisTokenBucket(runtime)
        self._settings = settings

    async def pre_auth(self, request) -> None:
        secret = self._settings.credential_hmac_secret
        if secret is None:
            raise GatewayHttpError(
                503, "upstream_unavailable", "Dependency unavailable."
            )
        import base64

        try:
            key = base64.b64decode(secret, validate=True)
        except Exception:  # noqa: BLE001 - fail closed on invalid security config
            raise GatewayHttpError(
                503, "upstream_unavailable", "Dependency unavailable."
            ) from None
        policy = pre_auth_policy(
            resolve_client_ip(request, self._settings.trusted_proxy_cidrs), key
        )
        await self._evaluate([policy])

    async def post_auth(self, request, context) -> None:
        policies = await enabled_admin_token_policies(
            request.app.state.db_engine, context.tenant_id, context.admin_token_id
        )
        await self._evaluate(policies)

    async def _evaluate(self, policies: list[ResolvedRatePolicy]) -> None:
        try:
            result = await self._bucket.evaluate(policies)
        except RateLimitDependencyError:
            raise GatewayHttpError(
                503, "upstream_unavailable", "Dependency unavailable."
            ) from None
        if not result.allowed:
            retry_after = max(1, (result.retry_after_ms + 999) // 1000)
            raise GatewayHttpError(
                429,
                "rate_limit_exceeded",
                "Rate limit exceeded.",
                headers={"Retry-After": str(retry_after)},
            )
