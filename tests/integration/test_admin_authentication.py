from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, update
from sqlalchemy.ext.asyncio import create_async_engine

from app.api import dependencies
from app.control_plane import auth
from app.core.config import get_settings
from app.core.errors import install_error_handlers
from app.core.request_context import request_context_middleware
from app.core.security.credentials import CredentialHasher
from app.persistence.models import Base

ADMIN_CONTEXT_DEPENDENCY = Depends(dependencies.require_admin_context)


def _test_client(app: FastAPI) -> TestClient:
    return TestClient(
        app,
        backend_options={"loop_factory": asyncio.SelectorEventLoop},
    )


@pytest.fixture
def admin_auth_fixture():
    settings = get_settings()
    sync_engine = create_engine(settings.postgres_migration_dsn)
    async_engine = create_async_engine(settings.postgres_dsn)
    hasher = CredentialHasher(b"c" * 32)
    issued = hasher.issue("adm_")
    tenant_id, admin_user_id, admin_token_id = uuid4(), uuid4(), uuid4()
    timestamp = datetime.now(UTC)

    with sync_engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]).values(
                id=tenant_id,
                name=f"m21-tenant-{tenant_id}",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        connection.execute(
            insert(Base.metadata.tables["admin_users"]).values(
                id=admin_user_id,
                tenant_id=tenant_id,
                name="m21-admin",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        connection.execute(
            insert(Base.metadata.tables["admin_tokens"]).values(
                id=admin_token_id,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                token_prefix=issued.safe_prefix,
                token_hash=issued.verifier,
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )

    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    app.state.db_engine = async_engine
    app.state.admin_authenticator = auth.AdminAuthenticator(hasher)

    @app.get("/api/v1/admin/_m21_probe")
    async def probe(
        request: Request,
        tenant_id: UUID | None = None,
        context: auth.AdminContext = ADMIN_CONTEXT_DEPENDENCY,
    ) -> dict[str, str]:
        return {
            "tenant_id": str(context.tenant_id),
            "admin_user_id": str(context.admin_user_id),
            "admin_token_id": str(context.admin_token_id),
            "request_id": str(context.request_id),
            "supplied_tenant_id": str(tenant_id) if tenant_id else "",
            "state_request_id": str(request.state.request_id),
        }

    yield {
        "app": app,
        "sync_engine": sync_engine,
        "async_engine": async_engine,
        "raw_token": issued.raw,
        "tenant_id": tenant_id,
        "admin_user_id": admin_user_id,
        "admin_token_id": admin_token_id,
    }

    with sync_engine.begin() as connection:
        connection.execute(
            Base.metadata.tables["audit_logs"]
            .delete()
            .where(Base.metadata.tables["audit_logs"].c.tenant_id == tenant_id)
        )
        connection.execute(
            Base.metadata.tables["admin_tokens"]
            .delete()
            .where(Base.metadata.tables["admin_tokens"].c.tenant_id == tenant_id)
        )
        connection.execute(
            Base.metadata.tables["admin_users"]
            .delete()
            .where(Base.metadata.tables["admin_users"].c.tenant_id == tenant_id)
        )
        connection.execute(
            Base.metadata.tables["tenants"]
            .delete()
            .where(Base.metadata.tables["tenants"].c.id == tenant_id)
        )
    loop = asyncio.SelectorEventLoop()
    try:
        loop.run_until_complete(async_engine.dispose())
    finally:
        loop.close()
    sync_engine.dispose()


def authenticate(fixture, token: str | None, **headers):
    request_headers = dict(headers)
    if token is not None:
        request_headers["Authorization"] = f"Bearer {token}"
    with _test_client(fixture["app"]) as client:
        return client.get("/api/v1/admin/_m21_probe", headers=request_headers)


def assert_authentication_error(response, raw_token: str | None = None):
    assert response.status_code == 401
    body = response.json()
    assert body["error"] == {
        "message": "Authentication failed.",
        "type": "authentication_error",
        "param": None,
        "code": "authentication_error",
        "metadata": None,
    }
    assert UUID(body["request_id"]).version == 7
    if raw_token is not None:
        assert raw_token not in response.text


def test_valid_admin_token_builds_tenant_scoped_context(admin_auth_fixture):
    supplied_tenant = uuid4()
    with _test_client(admin_auth_fixture["app"]) as client:
        response = client.get(
            "/api/v1/admin/_m21_probe",
            params={"tenant_id": str(supplied_tenant)},
            headers={
                "Authorization": f"Bearer {admin_auth_fixture['raw_token']}",
                "X-Request-ID": "client-request-id",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == str(admin_auth_fixture["tenant_id"])
    assert body["admin_user_id"] == str(admin_auth_fixture["admin_user_id"])
    assert body["admin_token_id"] == str(admin_auth_fixture["admin_token_id"])
    assert body["supplied_tenant_id"] == str(supplied_tenant)
    assert body["request_id"] == body["state_request_id"]
    assert body["request_id"] == response.headers["X-Request-ID"]
    assert body["request_id"] != "client-request-id"
    assert UUID(body["request_id"]).version == 7


@pytest.mark.parametrize(
    ("header", "token"),
    [
        (None, None),
        ("Basic abc", None),
        ("Bearer", None),
        ("Bearer gw_not-an-admin-key", None),
        ("Bearer adm_invalid!", None),
        (None, "adm_" + "A" * 43),
    ],
)
def test_invalid_admin_credentials_are_normalized(
    admin_auth_fixture, header, token, caplog
):
    if header is not None:
        with _test_client(admin_auth_fixture["app"]) as client:
            response = client.get(
                "/api/v1/admin/_m21_probe", headers={"Authorization": header}
            )
        raw = header
    else:
        response = authenticate(admin_auth_fixture, token)
        raw = token

    assert_authentication_error(response, raw)
    if raw:
        assert raw not in caplog.text


def test_wrong_secret_with_matching_safe_prefix_is_rejected(admin_auth_fixture, caplog):
    raw_token = admin_auth_fixture["raw_token"]
    replacement = "A" if raw_token[-1] != "A" else "B"
    wrong_token = f"{raw_token[:-1]}{replacement}"

    response = authenticate(admin_auth_fixture, wrong_token)

    assert_authentication_error(response, wrong_token)
    assert wrong_token not in caplog.text


@pytest.mark.parametrize("status", ["DISABLED", "REVOKED"])
def test_inactive_admin_token_is_rejected(admin_auth_fixture, status):
    tokens = Base.metadata.tables["admin_tokens"]
    timestamp = datetime.now(UTC)
    values = {
        "status": status,
        "revoked_at": timestamp if status == "REVOKED" else None,
        "updated_at": timestamp,
    }
    with admin_auth_fixture["sync_engine"].begin() as connection:
        connection.execute(
            update(tokens)
            .where(tokens.c.id == admin_auth_fixture["admin_token_id"])
            .values(**values)
        )

    assert_authentication_error(
        authenticate(admin_auth_fixture, admin_auth_fixture["raw_token"]),
        admin_auth_fixture["raw_token"],
    )


def test_expired_admin_token_is_rejected(admin_auth_fixture):
    tokens = Base.metadata.tables["admin_tokens"]
    with admin_auth_fixture["sync_engine"].begin() as connection:
        connection.execute(
            update(tokens)
            .where(tokens.c.id == admin_auth_fixture["admin_token_id"])
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    assert_authentication_error(
        authenticate(admin_auth_fixture, admin_auth_fixture["raw_token"]),
        admin_auth_fixture["raw_token"],
    )


def test_disabled_admin_user_is_rejected(admin_auth_fixture):
    users = Base.metadata.tables["admin_users"]
    with admin_auth_fixture["sync_engine"].begin() as connection:
        connection.execute(
            update(users)
            .where(users.c.id == admin_auth_fixture["admin_user_id"])
            .values(status="DISABLED")
        )

    assert_authentication_error(
        authenticate(admin_auth_fixture, admin_auth_fixture["raw_token"]),
        admin_auth_fixture["raw_token"],
    )
