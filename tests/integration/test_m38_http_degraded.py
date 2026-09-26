"""Full ASGI admin path with a genuinely refused Redis protection socket."""

import base64
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings
from app.core.security.credentials import CredentialHasher
from app.main import create_app
from app.persistence.models import Base
from app.redis.runtime import RedisRuntime


@pytest.mark.asyncio
async def test_admin_http_pre_auth_degrades_and_rejects_sixth_attempt(monkeypatch):
    monkeypatch.setenv("CREDENTIAL_HMAC_SECRET", base64.b64encode(b"m" * 32).decode())
    get_settings.cache_clear()
    app = create_app()
    try:
        async with app.router.lifespan_context(app):
            refused = RedisRuntime("redis://127.0.0.1:1/0", 0.2)
            await refused.start()
            try:
                guard = app.state.control_plane_protection
                assert app.state.local_degraded_protection is guard.local
                guard._bucket._runtime = refused
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    token = "adm_" + "A" * 43
                    headers = {"Authorization": f"Bearer {token}"}
                    for _ in range(5):
                        response = await client.get(
                            "/api/v1/admin/applications", headers=headers
                        )
                        assert response.status_code == 401
                    rejected = await client.get(
                        "/api/v1/admin/applications", headers=headers
                    )
                    assert rejected.status_code == 429
                    assert rejected.headers["Retry-After"]
                    assert token not in rejected.text
                    assert "127.0.0.1" not in rejected.text
                    traffic = await client.get("/health/traffic")
                    assert traffic.status_code == 200
                    assert traffic.json()["runtime_mode"] == "DEGRADED"
            finally:
                await refused.close()
    finally:
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_admin_http_post_auth_policy_uses_persisted_degraded_factor(monkeypatch):
    key = b"m" * 32
    monkeypatch.setenv("CREDENTIAL_HMAC_SECRET", base64.b64encode(key).decode())
    get_settings.cache_clear()
    settings = get_settings()
    db = create_async_engine(settings.postgres_dsn, pool_pre_ping=True)
    tenant_id, admin_id, token_id = uuid4(), uuid4(), uuid4()
    issued = CredentialHasher(key).issue("adm_")
    now = datetime.now(UTC)
    try:
        async with db.begin() as connection:
            for name, values in (
                (
                    "tenants",
                    {"id": tenant_id, "name": f"m38-{tenant_id}", "status": "ACTIVE"},
                ),
                (
                    "admin_users",
                    {
                        "id": admin_id,
                        "tenant_id": tenant_id,
                        "name": "m38",
                        "status": "ACTIVE",
                    },
                ),
                (
                    "admin_tokens",
                    {
                        "id": token_id,
                        "tenant_id": tenant_id,
                        "admin_user_id": admin_id,
                        "token_prefix": issued.safe_prefix,
                        "token_hash": issued.verifier,
                        "status": "ACTIVE",
                    },
                ),
                (
                    "rate_limit_policies",
                    {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "scope_type": "ADMIN_TOKEN",
                        "scope_id": token_id,
                        "requests_per_window": 8,
                        "window_seconds": 60,
                        "max_concurrency": None,
                        "degraded_factor": Decimal("0.25"),
                        "enabled": True,
                    },
                ),
            ):
                await connection.execute(
                    insert(Base.metadata.tables[name]).values(
                        **values, created_at=now, updated_at=now
                    )
                )
        app = create_app()
        async with app.router.lifespan_context(app):
            refused = RedisRuntime("redis://127.0.0.1:1/0", 0.2)
            await refused.start()
            try:
                app.state.control_plane_protection._bucket._runtime = refused
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    headers = {"Authorization": f"Bearer {issued.raw}"}
                    for _ in range(2):
                        response = await client.get(
                            "/api/v1/admin/health", headers=headers
                        )
                        assert response.status_code == 200
                    blocked = await client.get("/api/v1/admin/health", headers=headers)
                    assert blocked.status_code == 429
                    assert blocked.headers["Retry-After"]
                    assert issued.raw not in blocked.text
            finally:
                await refused.close()
    finally:
        async with db.begin() as connection:
            for name in (
                "rate_limit_policies",
                "admin_tokens",
                "admin_users",
            ):
                table = Base.metadata.tables[name]
                await connection.execute(
                    table.delete().where(table.c.tenant_id == tenant_id)
                )
            tenants = Base.metadata.tables["tenants"]
            await connection.execute(tenants.delete().where(tenants.c.id == tenant_id))
        await db.dispose()
        get_settings.cache_clear()
