from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.control_plane.auth import AdminAuthenticator
from app.core.config import get_settings
from app.core.security.credentials import CredentialHasher
from app.main import create_app
from app.persistence.models import Base
from app.redis.namespace import (
    HISTORICAL_M2_INVALIDATION_CHANNEL,
    M3_INVALIDATION_CHANNEL,
)


async def _next_matching(pubsub, tenant_id):
    async with asyncio.timeout(3):
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=2
            )
            if message is None:
                continue
            event = json.loads(message["data"])
            if event["tenant_id"] == str(tenant_id):
                return event


@pytest.fixture
def m36_settings(monkeypatch):
    key = base64.b64encode(b"c" * 32).decode()
    monkeypatch.setenv("CREDENTIAL_HMAC_SECRET", key)
    monkeypatch.setenv("IDEMPOTENCY_HMAC_SECRET", key)
    monkeypatch.setenv("ENCRYPTION_KEYS", '{"1":"' + key + '"}')
    monkeypatch.setenv("ENCRYPTION_CURRENT_KEY_VERSION", "1")
    get_settings.cache_clear()
    yield get_settings()
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_async_http_application_mutation_dual_publish_replay_and_rollback(
    m36_settings,
):
    settings = m36_settings
    db = create_async_engine(settings.postgres_dsn, pool_pre_ping=True)
    observer = Redis.from_url(settings.redis_url, decode_responses=True)
    tenant_id, admin_id, token_id = uuid4(), uuid4(), uuid4()
    issued = CredentialHasher(b"c" * 32).issue("adm_")
    now = datetime.now(UTC)
    async with db.begin() as connection:
        await connection.run_sync(
            lambda sync: sync.execute(
                insert(Base.metadata.tables["tenants"]).values(
                    id=tenant_id,
                    name=f"m36-{tenant_id}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
        )
        await connection.run_sync(
            lambda sync: sync.execute(
                insert(Base.metadata.tables["admin_users"]).values(
                    id=admin_id,
                    tenant_id=tenant_id,
                    name="m36",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
        )
        await connection.run_sync(
            lambda sync: sync.execute(
                insert(Base.metadata.tables["admin_tokens"]).values(
                    id=token_id,
                    tenant_id=tenant_id,
                    admin_user_id=admin_id,
                    token_prefix=issued.safe_prefix,
                    token_hash=issued.verifier,
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
        )
    canonical = observer.pubsub()
    legacy = observer.pubsub()
    await canonical.subscribe(M3_INVALIDATION_CHANNEL)
    await legacy.subscribe(HISTORICAL_M2_INVALIDATION_CHANNEL)
    await canonical.get_message(ignore_subscribe_messages=False, timeout=2)
    await legacy.get_message(ignore_subscribe_messages=False, timeout=2)
    app = create_app()
    app.state.admin_authenticator = AdminAuthenticator(CredentialHasher(b"c" * 32))
    headers = {
        "Authorization": f"Bearer {issued.raw}",
        "Idempotency-Key": f"m36-{uuid4()}",
    }
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                first = await client.post(
                    "/api/v1/admin/applications",
                    json={"name": f"app-{uuid4()}"},
                    headers=headers,
                )
                assert first.status_code == 201
                event_a = await _next_matching(canonical, tenant_id)
                event_b = await _next_matching(legacy, tenant_id)
                assert event_a == event_b
                assert set(event_a) == {
                    "tenant_id",
                    "resource_type",
                    "resource_id",
                    "version",
                }
                assert event_a["resource_type"] == "APPLICATION"
                assert event_a["resource_id"] == json.loads(first.text)["id"]
                async with db.connect() as connection:
                    persisted = await connection.scalar(
                        select(Base.metadata.tables["config_versions"].c.version).where(
                            Base.metadata.tables["config_versions"].c.tenant_id
                            == tenant_id
                        )
                    )
                    committed_application = await connection.scalar(
                        select(Base.metadata.tables["applications"].c.id).where(
                            Base.metadata.tables["applications"].c.id
                            == event_a["resource_id"]
                        )
                    )
                assert event_a["version"] == persisted
                assert str(committed_application) == event_a["resource_id"]
                replay = await client.post(
                    "/api/v1/admin/applications",
                    json={"name": json.loads(first.text)["name"]},
                    headers=headers,
                )
                assert replay.status_code == 201
                assert replay.content == first.content
                assert (
                    await canonical.get_message(
                        ignore_subscribe_messages=True, timeout=0.2
                    )
                    is None
                )
                assert (
                    await legacy.get_message(
                        ignore_subscribe_messages=True, timeout=0.2
                    )
                    is None
                )
                failed = await client.post(
                    "/api/v1/admin/applications",
                    json={"name": json.loads(first.text)["name"]},
                    headers={**headers, "Idempotency-Key": f"m36-failed-{uuid4()}"},
                )
                assert failed.status_code in {400, 409}
                async with db.connect() as connection:
                    version_after_failure = await connection.scalar(
                        select(Base.metadata.tables["config_versions"].c.version).where(
                            Base.metadata.tables["config_versions"].c.tenant_id
                            == tenant_id
                        )
                    )
                    matching_applications = await connection.scalar(
                        select(func.count())
                        .select_from(Base.metadata.tables["applications"])
                        .where(
                            Base.metadata.tables["applications"].c.tenant_id
                            == tenant_id,
                            Base.metadata.tables["applications"].c.name
                            == json.loads(first.text)["name"],
                        )
                    )
                assert version_after_failure == persisted
                assert matching_applications == 1
                assert (
                    await canonical.get_message(
                        ignore_subscribe_messages=True, timeout=0.2
                    )
                    is None
                )
                assert (
                    await legacy.get_message(
                        ignore_subscribe_messages=True, timeout=0.2
                    )
                    is None
                )
    finally:
        await canonical.aclose()
        await legacy.aclose()
        await observer.aclose()
        async with db.begin() as connection:
            for name in (
                "idempotency_records",
                "audit_logs",
                "applications",
                "config_versions",
                "admin_tokens",
                "admin_users",
            ):
                await connection.run_sync(
                    lambda sync, table=Base.metadata.tables[name]: sync.execute(
                        table.delete().where(table.c.tenant_id == tenant_id)
                    )
                )
            await connection.run_sync(
                lambda sync: sync.execute(
                    Base.metadata.tables["tenants"]
                    .delete()
                    .where(Base.metadata.tables["tenants"].c.id == tenant_id)
                )
            )
        await db.dispose()
