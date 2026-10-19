import asyncio
from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest

from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry
from app.core.errors import GatewayHttpError
from app.normal_api.current_state import NormalApiCurrentState, TenantProjection


def setup_state():
    tenant, service, route = uuid4(), uuid4(), uuid4()
    rows = {
        "service": {
            "id": service,
            "tenant_id": tenant,
            "upstream_base_url": "http://old",
        },
        "route": {
            "id": route,
            "tenant_id": tenant,
            "service_id": service,
            "path_pattern": "/old",
            "header_policy": {"request_allowlist": ["accept"]},
        },
    }
    calls = []
    registry = InvalidationRegistry()

    async def load(tenant_id):
        calls.append(tenant_id)
        return TenantProjection(tenant_id, (rows["service"],), (rows["route"],), ())

    consumer = NormalApiCurrentState(registry, load)

    async def lookup(tenants):
        return dict.fromkeys(tenants, 1)

    return tenant, rows, calls, registry, consumer, lookup


@pytest.mark.asyncio
async def test_registration_initialization_and_immutable_capture():
    tenant, rows, calls, registry, consumer, lookup = setup_state()
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)
    await consumer.register(tenant)
    await consumer.register(tenant)
    assert registry.tenant_ids() == frozenset({tenant})
    assert registry.required_initialization_pending(tenant)
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)
    assert await registry.reconcile_fenced(tenant, lookup) == "initialized"
    snapshot = await consumer.capture(tenant)
    assert snapshot.tenant_id == tenant and snapshot.observed_version == 1
    assert calls == [tenant]
    assert snapshot.projection.services[0]["id"] == rows["service"]["id"]
    with pytest.raises(TypeError):
        snapshot.projection.routes[0]["header_policy"]["request_allowlist"][0] = (
            "cookie"
        )
    with pytest.raises(FrozenInstanceError):
        snapshot.observed_version = 99
    for _ in range(3):
        await consumer.capture(tenant)
    assert calls == [tenant]  # Capture cannot query the loader.


@pytest.mark.asyncio
async def test_required_event_failure_later_success_cannot_clear_dirty():
    tenant, _rows, _calls, registry, consumer, lookup = setup_state()
    await consumer.register(tenant)
    await registry.reconcile_fenced(tenant, lookup)

    async def failed(event):
        raise RuntimeError("synthetic required callback failure")

    key = (tenant, "normal_api_current_state")
    registry._wide_callbacks[key] = (frozenset({"SERVICE"}), failed)
    with pytest.raises(RuntimeError):
        await registry.observe(InvalidationEvent(tenant, "SERVICE", uuid4(), 2))
    await registry.observe(InvalidationEvent(tenant, "ROUTE", uuid4(), 3))
    assert registry.observed_version(tenant) == 3
    assert registry.reconciliation_required(tenant)
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)

    async def current(tenants):
        return dict.fromkeys(tenants, 3)

    await registry.reconcile_fenced(tenant, current)
    assert not registry.reconciliation_required(tenant)
    assert (await consumer.capture(tenant)).observed_version == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["SERVICE", "ROUTE", "OTHER"])
async def test_invalidation_equal_version_repair_and_inflight_identity(kind):
    tenant, rows, calls, registry, consumer, lookup = setup_state()
    await consumer.register(tenant)
    await registry.reconcile_fenced(tenant, lookup)
    old = await consumer.capture(tenant)
    rows["service"]["upstream_base_url"] = "http://new"
    rows["route"]["path_pattern"] = "/new"
    await registry.observe(InvalidationEvent(tenant, kind, uuid4(), 2))
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)

    async def current(tenants):
        return dict.fromkeys(tenants, 2)

    await registry.reconcile_fenced(tenant, current)
    new = await consumer.capture(tenant)
    assert old.projection.services[0]["upstream_base_url"] == "http://old"
    assert old.projection.routes[0]["path_pattern"] == "/old"
    assert new.projection.services[0]["upstream_base_url"] == "http://new"
    assert new.projection.routes[0]["path_pattern"] == "/new"
    assert old.projection.identity is not new.projection.identity
    assert calls == [tenant, tenant]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["loader", "parent", "tenant", "end_read", "changed_fence"]
)
async def test_incomplete_or_failed_fence_never_publishes(failure):
    tenant, rows, _calls, registry, consumer, _lookup = setup_state()
    await consumer.register(tenant)
    if failure == "parent":
        rows["route"]["service_id"] = uuid4()
    if failure == "tenant":
        rows["service"]["tenant_id"] = uuid4()
    if failure == "loader":

        async def broken(tenant_id):
            raise RuntimeError("synthetic loader failure")

        consumer.load = broken
    reads = 0

    async def fence(tenants):
        nonlocal reads
        reads += 1
        if reads == 2 and failure == "end_read":
            raise OSError("synthetic DB failure")
        return dict.fromkeys(
            tenants, 2 if reads == 2 and failure == "changed_fence" else 1
        )

    if failure == "changed_fence":
        assert await registry.reconcile_fenced(tenant, fence) == "fence_mismatch"
    else:
        with pytest.raises((RuntimeError, ValueError, OSError)):
            await registry.reconcile_fenced(tenant, fence)
    assert registry.required_initialization_pending(tenant)
    assert registry.reconciliation_required(tenant)
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_capture_waits_for_coherent_reconciliation_then_invalidation():
    tenant, _rows, _calls, registry, consumer, lookup = setup_state()
    entered, release = asyncio.Event(), asyncio.Event()
    original = consumer.load

    async def gated(tenant_id):
        entered.set()
        await release.wait()
        return await original(tenant_id)

    consumer.load = gated
    await consumer.register(tenant)
    reconcile = asyncio.create_task(registry.reconcile_fenced(tenant, lookup))
    await entered.wait()
    capture = asyncio.create_task(consumer.capture(tenant))
    event = asyncio.create_task(
        registry.observe(InvalidationEvent(tenant, "ROUTE", uuid4(), 2))
    )
    release.set()
    await reconcile
    # FIFO lock: capture was queued before invalidation, so may retain G.
    captured = await capture
    await event
    assert captured.projection.routes[0]["path_pattern"] == "/old"
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_resource_only_compatibility_late_registration_and_shutdown():
    tenant, _rows, calls, registry, consumer, lookup = setup_state()
    resource, events = uuid4(), []
    await registry.register(tenant, "SERVICE", resource, events.append)
    await registry.reconcile_fenced(tenant, lookup)
    assert not events and not calls
    await consumer.register(tenant)
    assert registry.required_initialization_pending(tenant)
    await registry.reconcile_fenced(tenant, lookup)
    await consumer.capture(tenant)
    await registry.observe(InvalidationEvent(tenant, "SERVICE", resource, 2))
    assert len(events) == 1 and events[0].resource_id == resource
    await consumer.close()
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)
    with pytest.raises(RuntimeError):
        await consumer.register(uuid4())
    assert registry.tenant_ids() == frozenset({tenant})


@pytest.mark.asyncio
async def test_registration_waiting_on_active_pass_stays_pending():
    tenant, _rows, calls, registry, consumer, lookup = setup_state()
    entered, release = asyncio.Event(), asyncio.Event()

    async def legacy(version):
        entered.set()
        await release.wait()

    await registry.register_tenant_callback(tenant, legacy)
    running = asyncio.create_task(registry.reconcile_fenced(tenant, lookup))
    await entered.wait()
    registration = asyncio.create_task(consumer.register(tenant))
    release.set()
    await running
    await registration
    assert registry.required_initialization_pending(tenant) and not calls
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)
    await registry.reconcile_fenced(tenant, lookup)
    assert calls == [tenant]
    await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_async_dispatch_survives_cross_tenant_registration():
    registry = InvalidationRegistry()
    a, b = uuid4(), uuid4()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def callback(event):
        calls.append("a_entered")
        entered.set()
        await release.wait()
        calls.append("a_completed")

    await registry.ensure_tenant_consumer(
        a, "a", frozenset({"SERVICE"}), callback, lambda _: None, lambda: None
    )
    dispatch = asyncio.create_task(
        registry.observe(InvalidationEvent(a, "SERVICE", uuid4(), 1))
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await registry.ensure_tenant_consumer(
            b,
            "b",
            frozenset({"SERVICE"}),
            lambda _: calls.append("b"),
            lambda _: None,
            lambda: None,
        )
        assert registry.required_initialization_pending(b)
        assert not registry.is_tenant_reconciled(b)
    finally:
        release.set()
        await asyncio.gather(dispatch, return_exceptions=True)
    assert await dispatch == "APPLIED"
    assert calls == ["a_entered", "a_completed"]
    assert registry.observed_version(a) == 1
    assert registry.observed_version(b) is None
    assert registry.required_initialization_pending(b)
    assert not registry.is_tenant_reconciled(b)


@pytest.mark.asyncio
async def test_dispatch_snapshot_order_and_mutation_isolation():
    registry = InvalidationRegistry()
    tenant = uuid4()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def first(event):
        calls.append("first")
        entered.set()
        await release.wait()

    async def register(name, callback):
        await registry.ensure_tenant_consumer(
            tenant,
            name,
            frozenset({"SERVICE"}),
            callback,
            lambda _: None,
            lambda: None,
        )

    await register("first", first)
    await register("second", lambda _: calls.append("second"))
    dispatch = asyncio.create_task(
        registry.observe(InvalidationEvent(tenant, "SERVICE", uuid4(), 1))
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        # Deliberate test-only mutation: public same-tenant registration waits
        # for the dispatch lock. The captured batch must retain old membership.
        registry._wide_callbacks[(tenant, "second")] = (
            frozenset({"SERVICE"}),
            lambda _: calls.append("replacement"),
        )
        registry._wide_callbacks[(tenant, "late")] = (
            frozenset({"SERVICE"}),
            lambda _: calls.append("late"),
        )
    finally:
        release.set()
        await asyncio.gather(dispatch, return_exceptions=True)
    assert await dispatch == "APPLIED"
    assert calls == ["first", "second"]
    calls.clear()
    await registry.observe(InvalidationEvent(tenant, "SERVICE", uuid4(), 2))
    assert calls == ["first", "replacement", "late"]


@pytest.mark.asyncio
async def test_other_tenant_close_does_not_interrupt_dispatch():
    tenant, _rows, _calls, registry, consumer, _lookup = setup_state()
    other = uuid4()
    entered, release = asyncio.Event(), asyncio.Event()

    async def callback(event):
        entered.set()
        await release.wait()

    await registry.ensure_tenant_consumer(
        other,
        "other",
        frozenset({"SERVICE"}),
        callback,
        lambda _: None,
        lambda: None,
    )
    await consumer.register(tenant)
    dispatch = asyncio.create_task(
        registry.observe(InvalidationEvent(other, "SERVICE", uuid4(), 1))
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await consumer.close()
    finally:
        release.set()
        await asyncio.gather(dispatch, return_exceptions=True)
    assert await dispatch == "APPLIED"
    assert registry.observed_version(other) == 1
    assert tenant in registry.tenant_ids()  # No unregister lifecycle is invented.
    with pytest.raises(GatewayHttpError):
        await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_same_tenant_registration_cannot_join_active_dispatch():
    registry = InvalidationRegistry()
    tenant = uuid4()
    entered, release, attempted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def first(event):
        calls.append("first")
        entered.set()
        await release.wait()

    await registry.ensure_tenant_consumer(
        tenant,
        "first",
        frozenset({"SERVICE"}),
        first,
        lambda _: None,
        lambda: None,
    )
    dispatch = asyncio.create_task(
        registry.observe(InvalidationEvent(tenant, "SERVICE", uuid4(), 1))
    )

    async def register_late():
        attempted.set()
        await registry.ensure_tenant_consumer(
            tenant,
            "late",
            frozenset({"SERVICE"}),
            lambda _: calls.append("late"),
            lambda _: None,
            lambda: None,
        )

    registration = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        registration = asyncio.create_task(register_late())
        await asyncio.wait_for(attempted.wait(), 3)
        assert not registration.done()
    finally:
        release.set()
        await asyncio.gather(dispatch, return_exceptions=True)
        if registration is not None:
            await asyncio.gather(registration, return_exceptions=True)
    assert await dispatch == "APPLIED"
    assert calls == ["first"]
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)
    await registry.observe(InvalidationEvent(tenant, "SERVICE", uuid4(), 2))
    assert calls == ["first", "first", "late"]


@pytest.mark.asyncio
@pytest.mark.parametrize("primed", [False, True], ids=["initial", "reload"])
@pytest.mark.parametrize(
    "failure",
    [
        "load",
        "partial",
        "credentials",
        "callback",
        "end_read",
        "fence",
        "publish_before",
        "publish_after",
    ],
)
async def test_f2_failed_pass_never_exposes_candidate_and_recovers(primed, failure):
    tenant, rows, _calls, registry, consumer, lookup = setup_state()
    await consumer.register(tenant)
    old = None
    if primed:
        await registry.reconcile_fenced(tenant, lookup)
        old = await consumer.capture(tenant)
        await registry.observe(
            InvalidationEvent(tenant, "SERVICE", rows["service"]["id"], 2)
        )
    previous = registry.observed_version(tenant)
    original_load = consumer.load
    original_publish = consumer._publish
    rows["service"]["upstream_base_url"] = "http://new"
    rows["route"]["path_pattern"] = "/new"

    async def broken_load(tenant_id):
        if failure == "load":
            raise OSError("synthetic initial load failure")
        candidate = await original_load(tenant_id)
        if failure == "partial":
            # A partially constructed route cannot form a complete projection.
            return TenantProjection(
                tenant_id, candidate.services, ({"tenant_id": tenant_id},), ()
            )
        if failure == "credentials":
            raise OSError("synthetic credential metadata failure")
        return candidate

    consumer.load = broken_load
    if failure == "callback":

        async def other_required(tenant_id):
            raise RuntimeError("synthetic required callback failure")

        await registry.ensure_tenant_consumer(
            tenant,
            "failure_probe",
            frozenset({"SERVICE"}),
            lambda _: None,
            other_required,
            lambda: None,
        )

    publication_reads = []

    def broken_publish(tenant_id):
        publication_reads.append(reads)
        assert reads == 2  # Publication must occur after the end-version read.
        if not failure.startswith("publish"):
            original_publish(tenant_id)
            return
        if failure == "publish_after":
            original_publish(tenant_id)
        raise RuntimeError("synthetic publication failure")

    consumer._publish = broken_publish
    reads = 0

    async def fence(tenants):
        nonlocal reads
        reads += 1
        if reads == 2 and failure == "end_read":
            raise OSError("synthetic end read failure")
        return dict.fromkeys(tenants, 3 if reads == 2 and failure == "fence" else 2)

    if failure == "fence":
        assert await registry.reconcile_fenced(tenant, fence) == "fence_mismatch"
    else:
        with pytest.raises((OSError, KeyError, RuntimeError, ValueError)):
            await registry.reconcile_fenced(tenant, fence)
    assert registry.observed_version(tenant) == previous
    assert registry.reconciliation_required(tenant)
    assert not registry.is_tenant_reconciled(tenant)
    assert publication_reads == ([2] if failure.startswith("publish") else [])
    if failure != "publish_after":
        assert consumer._published.get(tenant) is (old.projection if old else None)
    if not primed:
        assert registry.required_initialization_pending(tenant)
    with pytest.raises(GatewayHttpError):
        await asyncio.wait_for(consumer.capture(tenant), 3)
    if old is not None:
        assert old.projection.services[0]["upstream_base_url"] == "http://old"
        assert old.projection.routes[0]["path_pattern"] == "/old"
    consumer.load = original_load
    consumer._publish = original_publish
    if failure == "callback":
        # Repair the synthetic consumer through the existing named registration's
        # current-state capability; do not remove its required membership.
        registry._required_current_state[(tenant, "failure_probe")] = lambda _: None

    async def recovered(tenants):
        return dict.fromkeys(tenants, 2)

    await registry.reconcile_fenced(tenant, recovered)
    current = await consumer.capture(tenant)
    assert current.observed_version == 2
    assert current.projection.services[0]["upstream_base_url"] == "http://new"
    assert current.projection.routes[0]["path_pattern"] == "/new"


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["load", "end_read"])
async def test_f2_cancelled_pass_releases_lock_and_recovers(phase):
    tenant, rows, _calls, registry, consumer, lookup = setup_state()
    await consumer.register(tenant)
    await registry.reconcile_fenced(tenant, lookup)
    old = await consumer.capture(tenant)
    await registry.observe(InvalidationEvent(tenant, "ROUTE", rows["route"]["id"], 2))
    entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_load = consumer.load

    async def suspend():
        entered.set()
        try:
            await release.wait()
        finally:
            exited.set()

    async def load(tenant_id):
        if phase == "load":
            await suspend()
        return await original_load(tenant_id)

    consumer.load = load
    reads = 0

    async def fence(tenants):
        nonlocal reads
        reads += 1
        if reads == 2 and phase == "end_read":
            await suspend()
        return dict.fromkeys(tenants, 2)

    task = asyncio.create_task(registry.reconcile_fenced(tenant, fence))
    try:
        await asyncio.wait_for(entered.wait(), 3)
    finally:
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 3)
    assert task.cancelled() and exited.is_set()
    assert registry.reconciliation_required(tenant)
    assert consumer._published[tenant] is old.projection
    with pytest.raises(GatewayHttpError):
        await asyncio.wait_for(consumer.capture(tenant), 3)
    consumer.load = original_load

    async def recovered(tenants):
        return dict.fromkeys(tenants, 2)

    await asyncio.wait_for(registry.reconcile_fenced(tenant, recovered), 3)
    await consumer.capture(tenant)


@pytest.mark.asyncio
async def test_f2_managed_reconciler_shutdown_awaits_cancelled_loader():
    from app.control_plane.config_reconciler import ConfigReconciler

    tenant, _rows, _calls, registry, consumer, lookup = setup_state()
    entered, exited = asyncio.Event(), asyncio.Event()
    blocked = asyncio.Event()

    async def load(tenant_id):
        entered.set()
        try:
            await blocked.wait()
        finally:
            exited.set()

    consumer.load = load
    await consumer.register(tenant)
    reconciler = ConfigReconciler(registry, lookup)
    await reconciler.start()
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task = reconciler._task
    finally:
        await asyncio.wait_for(reconciler.stop(), 3)
    assert task.done() and task.cancelled() and exited.is_set()
    assert not reconciler.running
    assert registry.required_initialization_pending(tenant)
    with pytest.raises(GatewayHttpError):
        await asyncio.wait_for(consumer.capture(tenant), 3)
    await consumer.close()
