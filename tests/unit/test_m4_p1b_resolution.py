from dataclasses import replace
from urllib.parse import unquote_to_bytes
from uuid import uuid4

import pytest

from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry
from app.core.errors import GatewayHttpError, authentication_error
from app.core.request_context import new_request_id
from app.normal_api.auth import GatewayIdentity
from app.normal_api.current_state import NormalApiCurrentState, TenantProjection
from app.normal_api.ingress import canonical_ingress
from app.normal_api.resolution import NormalApiResolver


def scope(raw=b"/api/billing/orders/42", method="GET", **extra):
    return {
        "type": "http",
        "method": method,
        "raw_path": raw,
        "path": unquote_to_bytes(raw).decode("utf-8", "replace"),
        "headers": [],
        **extra,
    }


async def foundation():
    tenant, application, key, service_id, route_id = (uuid4() for _ in range(5))
    service = {
        "id": service_id,
        "tenant_id": tenant,
        "slug": "billing",
        "status": "ACTIVE",
        "upstream_base_url": "http://upstream.invalid",
        "request_body_limit_bytes": 10485760,
        "connect_timeout_seconds": 5,
        "pool_timeout_seconds": 5,
        "write_timeout_seconds": 30,
        "read_idle_timeout_seconds": 60,
        "pre_response_timeout_seconds": 120,
    }
    route = {
        "id": route_id,
        "tenant_id": tenant,
        "service_id": service_id,
        "status": "ACTIVE",
        "path_pattern": "/orders/{id}",
        "method": "GET",
        "priority": 0,
        "timeout_ms": None,
        "header_policy": None,
        "upstream_path_template": "/ignored",
    }
    rows = {"services": [service], "routes": [route]}
    identity = GatewayIdentity(
        tenant, application, key, frozenset({("SERVICE", service_id)})
    )

    class Auth:
        fail = False

        async def authenticate(self, request_scope):
            if self.fail:
                authentication_error()
            return self.identity

    auth = Auth()
    auth.identity = identity
    registry = InvalidationRegistry()

    async def load(tenant_id):
        return TenantProjection(
            tenant_id, tuple(rows["services"]), tuple(rows["routes"]), ()
        )

    current = NormalApiCurrentState(registry, load)
    resolver = NormalApiResolver(auth, current)

    async def reconcile(version=1):
        async def lookup(tenants):
            return dict.fromkeys(tenants, version)

        return await registry.reconcile_fenced(tenant, lookup)

    await current.register(tenant)
    await reconcile()
    return resolver, registry, current, rows, auth, reconcile


async def rejected(resolver, request_scope, status, code):
    with pytest.raises(GatewayHttpError) as caught:
        await resolver.resolve(request_scope, request_id=new_request_id())
    assert (caught.value.status_code, caught.value.code) == (status, code)
    assert caught.value.metadata is None
    return caught.value


@pytest.mark.asyncio
async def test_valid_service_and_route_permissions_and_snapshot_identity():
    resolver, _registry, current, rows, auth, _reconcile = await foundation()
    request_id = new_request_id()
    result = await resolver.resolve(scope(), request_id=request_id)
    assert result.request_id == request_id and request_id.version == 7
    assert result.route["id"] == rows["routes"][0]["id"]
    assert (
        result.snapshot.projection
        is (await current.capture(auth.identity.tenant_id)).projection
    )
    assert result.ingress.encoded_remaining_path == b"/orders/42"
    assert result.effective_request_body_limit_bytes == 10485760
    limited = NormalApiResolver(auth, current, deployment_body_limit_bytes=524288)
    assert (
        await limited.resolve(scope(), request_id=new_request_id())
    ).effective_request_body_limit_bytes == 524288
    auth.identity = replace(
        auth.identity, permissions=frozenset({("ROUTE", result.route["id"])})
    )
    await resolver.resolve(scope(), request_id=new_request_id())
    auth.identity = replace(auth.identity, permissions=frozenset())
    await rejected(resolver, scope(), 403, "authorization_error")


@pytest.mark.parametrize(
    "raw",
    [
        b"/api/billing/a//b",
        b"/api//x",
        b"/api/billing/a///",
        b"/api/billing/a%2fb",
        b"/api/billing/a%2Fb",
        b"/api/billing/a%5Cb",
        b"/api/billing/a\\b",
        b"/api/billing/%",
        b"/api/billing/%2",
        b"/api/billing/%GG",
        b"/api/billing/%FF",
        b"/api/billing/.",
        b"/api/billing/../x",
        b"/api/billing/%2e%2E/x",
        b"/api/billing/cafe%CC%81",
    ],
)
def test_raw_path_rejections_without_client_url_normalization(raw):
    with pytest.raises(GatewayHttpError) as caught:
        canonical_ingress(scope(raw))
    assert caught.value.status_code == 400
    assert caught.value.code == "invalid_request"
    assert raw.decode("ascii", "replace") not in caught.value.message


@pytest.mark.parametrize(
    "request_scope",
    [
        {"path": "/api/billing/orders/42"},
        {"raw_path": None, "path": "/api/billing/orders/42"},
        {"raw_path": b"/api/billing/orders/42", "path": "/api/other/orders/42"},
    ],
)
def test_missing_or_inconsistent_raw_scope_is_internal_failure(request_scope):
    with pytest.raises(GatewayHttpError) as caught:
        canonical_ingress(request_scope)
    assert (caught.value.status_code, caught.value.code) == (
        500,
        "gateway_internal_error",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["POST", "HEAD", "OPTIONS", "CONNECT", "TRACE"])
async def test_r19_method_boundary_and_exact_allow(method):
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    original = rows["routes"][0]
    rows["routes"] += [
        dict(original, id=uuid4(), method="DELETE"),
        dict(original, id=uuid4(), method="PATCH"),
        dict(original, id=uuid4(), method="PUT", status="DISABLED"),
    ]
    await registry.observe(
        InvalidationEvent(auth.identity.tenant_id, "ROUTE", original["id"], 2)
    )
    await reconcile(2)
    error = await rejected(resolver, scope(method=method), 405, "invalid_request")
    assert error.headers == {"Allow": "GET, PATCH, DELETE"}
    missing = await rejected(
        resolver, scope(b"/api/billing/missing", method), 404, "resource_not_found"
    )
    assert not missing.headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "unknown_service",
        "inactive_service",
        "inactive_route",
        "unknown_path",
        "trailing_slash",
    ],
)
async def test_absent_or_inactive_paths_are_404_without_redirect(case):
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    raw = b"/api/billing/orders/42"
    if case == "unknown_service":
        raw = b"/api/other/orders/42"
    elif case == "inactive_service":
        rows["services"][0]["status"] = "DISABLED"
    elif case == "inactive_route":
        rows["routes"][0]["status"] = "DISABLED"
    elif case == "unknown_path":
        raw = b"/api/billing/missing"
    else:
        raw += b"/"
    await registry.observe(
        InvalidationEvent(
            auth.identity.tenant_id, "SERVICE", rows["services"][0]["id"], 2
        )
    )
    await reconcile(2)
    error = await rejected(resolver, scope(raw), 404, "resource_not_found")
    assert not error.headers


@pytest.mark.asyncio
async def test_canonical_unicode_percent_and_exact_trailing_slash_patterns():
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    route = rows["routes"][0]
    for version, pattern, raw, encoded in [
        (2, "/caf\u00e9/a b", b"/api/billing/caf%C3%A9/a%20b", b"/caf%C3%A9/a%20b"),
        (3, "/%2F", b"/api/billing/%252F", b"/%252F"),
        (4, "/orders/{id}/", b"/api/billing/orders/42/", b"/orders/42/"),
    ]:
        route["path_pattern"] = pattern
        await registry.observe(
            InvalidationEvent(auth.identity.tenant_id, "ROUTE", route["id"], version)
        )
        await reconcile(version)
        result = await resolver.resolve(scope(raw), request_id=new_request_id())
        assert result.ingress.encoded_remaining_path == encoded


@pytest.mark.asyncio
async def test_ambiguous_persisted_patterns_never_choose_priority_winner():
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    rows["routes"].append(
        dict(rows["routes"][0], id=uuid4(), path_pattern="/orders/42", priority=99)
    )
    await registry.observe(
        InvalidationEvent(auth.identity.tenant_id, "ROUTE", rows["routes"][0]["id"], 2)
    )
    await reconcile(2)
    await rejected(resolver, scope(), 500, "gateway_internal_error")


@pytest.mark.asyncio
async def test_authentication_precedes_path_and_configuration_disclosure():
    resolver, _registry, _current, _rows, auth, _reconcile = await foundation()
    auth.fail = True
    await rejected(
        resolver, scope(b"/api//bad", "OPTIONS"), 401, "authentication_error"
    )


@pytest.mark.asyncio
async def test_first_use_and_dirty_state_never_fall_back_to_persistence():
    resolver, registry, current, _rows, auth, reconcile = await foundation()
    await registry.observe(
        InvalidationEvent(auth.identity.tenant_id, "SERVICE", uuid4(), 2)
    )
    await rejected(resolver, scope(), 500, "gateway_internal_error")
    await reconcile(2)
    foreign_identity = replace(auth.identity, tenant_id=uuid4())
    auth.identity = foreign_identity
    await rejected(resolver, scope(), 500, "gateway_internal_error")
    assert registry.required_initialization_pending(foreign_identity.tenant_id)
    assert current.registry is registry


@pytest.mark.asyncio
async def test_request_does_not_wait_for_suspended_reconciliation():
    import asyncio

    resolver, registry, current, _rows, auth, _reconcile = await foundation()
    entered, release = asyncio.Event(), asyncio.Event()
    original = current.load

    async def load(tenant):
        entered.set()
        await release.wait()
        return await original(tenant)

    current.load = load
    await registry.observe(
        InvalidationEvent(auth.identity.tenant_id, "SERVICE", uuid4(), 2)
    )

    async def lookup(tenants):
        return dict.fromkeys(tenants, 2)

    task = asyncio.create_task(
        registry.reconcile_fenced(auth.identity.tenant_id, lookup)
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.wait_for(
            rejected(resolver, scope(), 500, "gateway_internal_error"), 3
        )
    finally:
        release.set()
        await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
async def test_selection_retains_captured_generation_during_local_invalidation(
    monkeypatch,
):
    import app.normal_api.resolution as module

    resolver, registry, _current, _rows, auth, _reconcile = await foundation()
    original = module.pattern_matches

    def match(pattern, segments):
        # Synchronous matching cannot interleave an async registry event; simulate
        # the observable certificate invalidation after the capture boundary.
        registry._invalidate_certificate(auth.identity.tenant_id)
        return original(pattern, segments)

    monkeypatch.setattr(module, "pattern_matches", match)
    result = await resolver.resolve(scope(), request_id=new_request_id())
    assert result.route["path_pattern"] == "/orders/{id}"
    assert not registry.is_tenant_reconciled(auth.identity.tenant_id)


@pytest.mark.asyncio
async def test_legacy_invalid_patterns_do_not_poison_valid_routes():
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    rows["routes"] += [
        dict(rows["routes"][0], id=uuid4(), path_pattern="/cafe\u0301"),
        dict(rows["routes"][0], id=uuid4(), path_pattern="/orders//{id}"),
    ]
    await registry.observe(
        InvalidationEvent(auth.identity.tenant_id, "ROUTE", uuid4(), 2)
    )
    await reconcile(2)
    await resolver.resolve(scope(), request_id=new_request_id())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "header_policy",
        "body_limit",
        "timeout",
        "priority",
        "url_scheme",
        "url_userinfo",
        "url_query",
        "url_port",
        "url_empty_query",
        "url_empty_fragment",
    ],
)
async def test_invalid_execution_configuration_rejects_before_admission(case):
    resolver, registry, _current, rows, auth, reconcile = await foundation()
    if case == "header_policy":
        rows["routes"][0]["header_policy"] = {"request_allowlist": ["Accept", "accept"]}
    elif case == "body_limit":
        rows["services"][0]["request_body_limit_bytes"] = 0
    elif case == "timeout":
        rows["routes"][0]["timeout_ms"] = True
    elif case == "priority":
        rows["routes"][0]["priority"] = -1
    else:
        rows["services"][0]["upstream_base_url"] = {
            "url_scheme": "ftp://example.invalid",
            "url_userinfo": "http://user:pass@example.invalid",
            "url_query": "http://example.invalid?query=1",
            "url_port": "http://example.invalid:bad",
            "url_empty_query": "http://example.invalid?",
            "url_empty_fragment": "http://example.invalid#",
        }[case]
    await registry.observe(
        InvalidationEvent(
            auth.identity.tenant_id, "SERVICE", rows["services"][0]["id"], 2
        )
    )
    await reconcile(2)
    await rejected(resolver, scope(), 500, "gateway_internal_error")
