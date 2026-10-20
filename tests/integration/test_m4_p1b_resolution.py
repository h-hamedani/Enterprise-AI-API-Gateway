import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete, event, insert, update

from app.control_plane.config_reconciler import ConfigVersionRepository
from app.control_plane.config_subscriber import InvalidationRegistry
from app.core.errors import GatewayHttpError, install_error_handlers
from app.core.request_context import new_request_id, request_context_middleware
from app.core.security.credentials import CredentialHasher
from app.main import create_app
from app.normal_api.auth import GatewayAuthenticator
from app.normal_api.current_state import CurrentStateRepository, NormalApiCurrentState
from app.normal_api.resolution import METHODS, NormalApiResolver
from app.persistence.models import Base
from tests.integration.test_m4_p1a_current_state import persisted_state
from tests.unit.test_m4_p1b_resolution import scope


@asynccontextmanager
async def world(*, initialize=True):
    async with persisted_state() as (engine, tenant, service, route):
        tables = Base.metadata.tables
        application, key, permission = uuid4(), uuid4(), uuid4()
        hasher = CredentialHasher(b"p1b-synthetic-test-key-material!!" * 2)
        issued = hasher.issue("gw_")
        now = datetime.now(UTC)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    insert(tables["applications"]).values(
                        id=application,
                        tenant_id=tenant,
                        name=f"p1b-{application}",
                        status="ACTIVE",
                        created_at=now,
                        updated_at=now,
                    )
                )
                await connection.execute(
                    insert(tables["api_keys"]).values(
                        id=key,
                        tenant_id=tenant,
                        application_id=application,
                        name="p1b",
                        key_prefix=issued.safe_prefix,
                        key_hash=issued.verifier,
                        status="ACTIVE",
                        created_at=now,
                        updated_at=now,
                    )
                )
                await connection.execute(
                    insert(tables["api_key_permissions"]).values(
                        id=permission,
                        tenant_id=tenant,
                        api_key_id=key,
                        resource_type="SERVICE",
                        resource_id=service,
                        action="INVOKE",
                        created_at=now,
                    )
                )
            registry = InvalidationRegistry()
            current = NormalApiCurrentState(
                registry, CurrentStateRepository(engine).load
            )
            lookup = ConfigVersionRepository(engine).lookup
            if initialize:
                await current.register(tenant)
                await registry.reconcile_fenced(tenant, lookup)
            resolver = NormalApiResolver(GatewayAuthenticator(engine, hasher), current)
            yield {
                "engine": engine,
                "tenant": tenant,
                "service": service,
                "route": route,
                "application": application,
                "key": key,
                "issued": issued,
                "registry": registry,
                "current": current,
                "lookup": lookup,
                "resolver": resolver,
            }
            await current.close()
        finally:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(tables["api_keys"]).where(tables["api_keys"].c.id == key)
                )
                await connection.execute(
                    delete(tables["applications"]).where(
                        tables["applications"].c.id == application
                    )
                )


def request_scope(context, *, method="GET", raw=b"/api/p1a/old", headers=()):
    return scope(
        raw,
        method,
        headers=[
            (b"authorization", f"Bearer {context['issued'].raw}".encode()),
            *headers,
        ],
    )


@pytest.mark.asyncio
async def test_real_authenticated_selection_and_strict_status_without_config_fallback(
    caplog,
):
    async with world() as context:
        caplog.set_level(logging.DEBUG, logger="app.normal_api")
        statements = []

        def observe(connection, cursor, statement, parameters, execution_context, many):
            statements.append(statement)

        event.listen(context["engine"].sync_engine, "before_cursor_execute", observe)
        try:
            selected = await context["resolver"].resolve(
                request_scope(context), request_id=new_request_id()
            )
        finally:
            event.remove(
                context["engine"].sync_engine, "before_cursor_execute", observe
            )
        assert selected.identity.tenant_id == context["tenant"]
        assert selected.identity.application_id == context["application"]
        assert selected.identity.api_key_id == context["key"]
        assert (
            selected.service["id"] == context["service"]
            and selected.route["id"] == context["route"]
        )
        assert not any(
            f"FROM {name}" in statement
            for name in (
                "normal_api_services",
                "normal_api_routes",
                "service_credentials",
                "config_versions",
            )
            for statement in statements
        )
        assert context["issued"].raw not in caplog.text
        assert context["issued"].verifier not in caplog.text
        table = Base.metadata.tables["applications"]
        async with context["engine"].begin() as connection:
            await connection.execute(
                update(table)
                .where(table.c.id == context["application"])
                .values(status="DISABLED")
            )
        assert context["registry"].is_tenant_reconciled(context["tenant"])
        with pytest.raises(GatewayHttpError) as caught:
            await context["resolver"].resolve(
                request_scope(context), request_id=new_request_id()
            )
        assert (caught.value.status_code, caught.value.code) == (
            401,
            "authentication_error",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "malformed",
        "admin",
        "unknown",
        "disabled",
        "revoked",
        "expired",
        "tenant_disabled",
        "duplicate",
    ],
)
async def test_real_gateway_credential_rejections(case):
    async with world() as context:
        request = request_scope(context)
        if case == "missing":
            request["headers"] = []
        elif case == "malformed":
            request["headers"] = [(b"authorization", b"Bearer gw_bad")]
        elif case == "admin":
            request["headers"] = [(b"authorization", b"Bearer adm_" + b"a" * 43)]
        elif case == "unknown":
            request["headers"] = [(b"authorization", b"Bearer gw_" + b"a" * 43)]
        elif case == "duplicate":
            request["headers"] *= 2
        else:
            table = Base.metadata.tables[
                "tenants" if case == "tenant_disabled" else "api_keys"
            ]
            values = {"status": "DISABLED"}
            if case == "revoked":
                values = {"status": "REVOKED", "revoked_at": datetime.now(UTC)}
            if case == "expired":
                values = {"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
            target = context["tenant"] if case == "tenant_disabled" else context["key"]
            async with context["engine"].begin() as connection:
                await connection.execute(
                    update(table).where(table.c.id == target).values(**values)
                )
        request["raw_path"] = b"/api//invalid"
        with pytest.raises(GatewayHttpError) as caught:
            await context["resolver"].resolve(request, request_id=new_request_id())
        assert (caught.value.status_code, caught.value.code) == (
            401,
            "authentication_error",
        )


@pytest.mark.asyncio
async def test_first_use_registration_defers_to_existing_reconciler():
    async with world(initialize=False) as context:
        assert not context["registry"].tenant_ids()
        with pytest.raises(GatewayHttpError) as caught:
            await context["resolver"].resolve(
                request_scope(context), request_id=new_request_id()
            )
        assert caught.value.status_code == 500
        assert context["registry"].required_initialization_pending(context["tenant"])
        await context["registry"].reconcile_fenced(context["tenant"], context["lookup"])
        await context["resolver"].resolve(
            request_scope(context), request_id=new_request_id()
        )


@pytest.mark.asyncio
async def test_two_tenants_and_foreign_permission_cannot_change_binding():
    async with world() as a, world() as b:
        for context in (a, b):
            selected = await context["resolver"].resolve(
                request_scope(
                    context, headers=[(b"x-tenant-id", str(b["tenant"]).encode())]
                ),
                request_id=new_request_id(),
            )
            assert selected.identity.tenant_id == context["tenant"]
            assert selected.service["id"] == context["service"]
        permissions = Base.metadata.tables["api_key_permissions"]
        services = Base.metadata.tables["normal_api_services"]
        async with a["engine"].begin() as connection:
            await connection.execute(
                delete(permissions).where(permissions.c.api_key_id == a["key"])
            )
            await connection.execute(
                insert(permissions).values(
                    id=uuid4(),
                    tenant_id=a["tenant"],
                    api_key_id=a["key"],
                    resource_type="SERVICE",
                    resource_id=b["service"],
                    action="INVOKE",
                    created_at=datetime.now(UTC),
                )
            )
            await connection.execute(
                update(services)
                .where(services.c.id == b["service"])
                .values(slug="foreign")
            )
        with pytest.raises(GatewayHttpError) as denied:
            await a["resolver"].resolve(request_scope(a), request_id=new_request_id())
        assert denied.value.status_code == 403
        with pytest.raises(GatewayHttpError) as absent:
            await a["resolver"].resolve(
                request_scope(a, raw=b"/api/foreign/old"), request_id=new_request_id()
            )
        assert absent.value.status_code == 404 and not absent.value.headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method, authenticated",
    [
        ("GET", True),
        ("POST", True),
        ("HEAD", True),
        ("OPTIONS", True),
        ("OPTIONS", False),
    ],
)
async def test_raw_asgi_error_envelope_without_body_or_protection(
    method, authenticated, monkeypatch
):
    async with world() as context:
        app = FastAPI(redirect_slashes=False)
        app.middleware("http")(request_context_middleware)
        install_error_handlers(app)

        @app.api_route(
            "/{rest:path}", methods=[*METHODS, "HEAD", "OPTIONS", "TRACE", "CONNECT"]
        )
        async def probe(request: Request):
            await context["resolver"].resolve(
                request.scope, request_id=request.state.request_id
            )
            return JSONResponse({"resolved": True})

        async def forbidden(*args, **kwargs):
            pytest.fail("Premature protection/upstream/body activity")

        import httpx

        from app.redis.protection_facade import (
            RecoveryCircuitStore,
            RecoveryConcurrencySemaphore,
            RecoveryRateLimiter,
        )

        monkeypatch.setattr(RecoveryRateLimiter, "evaluate", forbidden)
        monkeypatch.setattr(RecoveryConcurrencySemaphore, "acquire", forbidden)
        monkeypatch.setattr(
            RecoveryCircuitStore, "check_or_claim_eligibility", forbidden
        )
        monkeypatch.setattr(httpx.AsyncClient, "send", forbidden)
        request = request_scope(
            context,
            method=method,
            headers=[
                (b"origin", b"https://example.invalid"),
                (b"access-control-request-method", b"GET"),
                (b"x-request-id", b"caller-controlled"),
            ],
        )
        request.update(
            {
                "asgi": {"version": "3.0", "spec_version": "2.4"},
                "http_version": "1.1",
                "scheme": "http",
                "root_path": "",
                "query_string": b"a=1&a=2",
                "server": ("test", 80),
                "client": ("127.0.0.1", 1),
            }
        )
        if not authenticated:
            request["headers"] = [
                (name, value)
                for name, value in request["headers"]
                if name != b"authorization"
            ]
        messages = []

        async def send(message):
            messages.append(message)

        await app(request, forbidden, send)
        start = next(
            message for message in messages if message["type"] == "http.response.start"
        )
        headers = dict(start["headers"])
        assert start["status"] == (
            401 if not authenticated else 200 if method == "GET" else 405
        )
        if authenticated and method != "GET":
            assert headers[b"allow"] == b"GET"
        assert (
            b"x-request-id" in headers
            and headers[b"x-request-id"] != b"caller-controlled"
        )
        assert (
            b"location" not in headers and b"access-control-allow-origin" not in headers
        )
        assert messages[-1]["type"] == "http.response.body"
        assert context["issued"].raw.encode() not in b"".join(
            message.get("body", b"") for message in messages
        )
        # The only dispatch endpoint above is test-local, never registered in production.
        assert not any(
            getattr(route, "path", "") == "/{rest:path}"
            for route in create_app().routes
        )


@pytest.mark.asyncio
async def test_real_persisted_overlap_fails_closed_without_admin_changes():
    async with world() as context:
        routes = Base.metadata.tables["normal_api_routes"]
        now = datetime.now(UTC)
        async with context["engine"].begin() as connection:
            await connection.execute(
                insert(routes).values(
                    id=uuid4(),
                    tenant_id=context["tenant"],
                    service_id=context["service"],
                    method="GET",
                    path_pattern="/{id}",
                    upstream_path_template="/ignored",
                    priority=999,
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            versions = Base.metadata.tables["config_versions"]
            await connection.execute(
                update(versions)
                .where(versions.c.tenant_id == context["tenant"])
                .values(version=2)
            )
        await context["registry"].reconcile_fenced(context["tenant"], context["lookup"])
        with pytest.raises(GatewayHttpError) as caught:
            await context["resolver"].resolve(
                request_scope(context), request_id=new_request_id()
            )
        assert (caught.value.status_code, caught.value.code) == (
            500,
            "gateway_internal_error",
        )


@pytest.mark.asyncio
async def test_real_route_permission_and_immediate_revocation():
    async with world() as context:
        permissions = Base.metadata.tables["api_key_permissions"]
        keys = Base.metadata.tables["api_keys"]
        async with context["engine"].begin() as connection:
            await connection.execute(
                update(permissions)
                .where(permissions.c.api_key_id == context["key"])
                .values(resource_type="ROUTE", resource_id=context["route"])
            )
        selected = await context["resolver"].resolve(
            request_scope(context), request_id=new_request_id()
        )
        assert selected.route["id"] == context["route"]
        async with context["engine"].begin() as connection:
            await connection.execute(
                update(keys)
                .where(keys.c.id == context["key"])
                .values(status="REVOKED", revoked_at=datetime.now(UTC))
            )
        with pytest.raises(GatewayHttpError) as caught:
            await context["resolver"].resolve(
                request_scope(context), request_id=new_request_id()
            )
        assert (caught.value.status_code, caught.value.code) == (
            401,
            "authentication_error",
        )
