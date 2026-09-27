"""M3-R3A version gaps and fenced completeness certification."""

import asyncio
from uuid import uuid4

import pytest

from app.control_plane.config_reconciler import ConfigReconciler
from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry


@pytest.mark.asyncio
async def test_missed_event_gap_marks_dirty_even_when_later_callback_succeeds():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, lambda _: None
    )
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 10))
        == "APPLIED"
    )
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 12))
        == "APPLIED"
    )
    assert registry.observed_version(tenant) == 12
    assert registry.reconciliation_required(tenant)
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_matching_start_and_end_versions_certify_all_required_consumers():
    registry = InvalidationRegistry()
    tenant = uuid4()
    calls = []
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, lambda t: calls.append(t)
    )
    reads = []

    async def lookup(tenant_ids):
        reads.append(tenant_ids)
        return {tenant: 7}

    assert await ConfigReconciler(registry, lookup).reconcile_once() == "pass_success"
    assert reads == [frozenset({tenant}), frozenset({tenant})]
    assert calls == [tenant]
    assert registry.observed_version(tenant) == 7
    assert registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_postgres_advance_during_callbacks_never_certifies_stale_pass():
    registry = InvalidationRegistry()
    tenant = uuid4()
    db_version = 7
    entered, release = asyncio.Event(), asyncio.Event()

    async def current_state(_):
        entered.set()
        await release.wait()

    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, current_state
    )

    async def lookup(tenant_ids):
        return {tenant: db_version}

    task = asyncio.create_task(ConfigReconciler(registry, lookup).reconcile_once())
    await entered.wait()
    db_version = 8
    release.set()
    assert await task == "pass_error"
    assert registry.reconciliation_required(tenant)
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_second_postgres_read_failure_preserves_incomplete_state():
    registry = InvalidationRegistry()
    tenant = uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, lambda _: None
    )
    reads = 0

    async def lookup(tenant_ids):
        nonlocal reads
        reads += 1
        if reads == 2:
            raise RuntimeError("database unavailable")
        return {tenant: 7}

    assert await ConfigReconciler(registry, lookup).reconcile_once() == "pass_error"
    assert reads == 2
    assert registry.observed_version(tenant) is None
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_event_waits_for_fenced_pass_then_invalidates_certificate():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    entered, release = asyncio.Event(), asyncio.Event()

    async def current_state(_):
        entered.set()
        await release.wait()

    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, current_state
    )

    async def lookup(tenant_ids):
        return {tenant: 10}

    pass_task = asyncio.create_task(ConfigReconciler(registry, lookup).reconcile_once())
    await entered.wait()
    attempted = asyncio.Event()

    async def deliver():
        attempted.set()
        return await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 11))

    event_task = asyncio.create_task(deliver())
    await attempted.wait()
    assert not event_task.done()
    release.set()
    assert await pass_task == "pass_success"
    assert await event_task == "APPLIED"
    assert registry.observed_version(tenant) == 11
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_empty_membership_has_no_certificate_or_database_read():
    registry = InvalidationRegistry()
    calls = []

    async def lookup(tenant_ids):
        calls.append(tenant_ids)
        return {}

    assert (
        await ConfigReconciler(registry, lookup).reconcile_once() == "empty_membership"
    )
    assert calls == []
    assert not registry.is_tenant_reconciled(uuid4())


@pytest.mark.asyncio
async def test_partial_three_consumer_failure_cannot_certify_then_retry_succeeds():
    registry = InvalidationRegistry()
    tenant = uuid4()
    calls = []
    failing = True

    def current(name):
        def callback(_):
            calls.append(name)
            if name == "B" and failing:
                raise RuntimeError("B unavailable")

        return callback

    for name in ("A", "B", "C"):
        await registry.register_required_consumer(
            tenant, "ROUTE", uuid4(), lambda _: None, current(name)
        )

    async def lookup(_):
        return {tenant: 9}

    reconciler = ConfigReconciler(registry, lookup)
    assert await reconciler.reconcile_once() == "pass_error"
    assert calls == ["A", "B", "C"]
    assert registry.required_initialization_pending(tenant)
    assert registry.reconciliation_required(tenant)
    assert not registry.is_tenant_reconciled(tenant)
    failing = False
    assert await reconciler.reconcile_once() == "pass_success"
    assert calls == ["A", "B", "C", "A", "B", "C"]
    assert registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_duplicate_and_stale_events_do_not_clear_gap_dirty_state():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, lambda _: None
    )
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 5))
    assert registry.reconciliation_required(tenant)
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 5))
        == "DUPLICATE"
    )
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 4))
        == "STALE"
    )
    assert registry.reconciliation_required(tenant)
    assert registry.observed_version(tenant) == 5


@pytest.mark.asyncio
async def test_db_behind_observed_version_cannot_certify_or_rollback():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, lambda _: None
    )
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 10))

    async def lookup(_):
        return {tenant: 9}

    assert await ConfigReconciler(registry, lookup).reconcile_once() == "pass_error"
    assert registry.observed_version(tenant) == 10
    assert registry.reconciliation_required(tenant)
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_first_postgres_read_failure_does_not_initialize_consumer():
    registry = InvalidationRegistry()
    tenant = uuid4()
    calls = []
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, lambda _: calls.append("run")
    )

    async def lookup(_):
        raise RuntimeError("database unavailable")

    assert await ConfigReconciler(registry, lookup).reconcile_once() == "pass_error"
    assert calls == []
    assert registry.observed_version(tenant) is None
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_required_replacement_during_pass_remains_pending():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def original(_):
        entered.set()
        await release.wait()
        calls.append("original")

    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, original
    )

    async def lookup(_):
        return {tenant: 3}

    reconciler = ConfigReconciler(registry, lookup)
    first = asyncio.create_task(reconciler.reconcile_once())
    await entered.wait()
    attempted = asyncio.Event()

    async def replace():
        attempted.set()
        await registry.register_required_consumer(
            tenant, "ROUTE", resource, lambda _: None, lambda _: calls.append("new")
        )

    replacement = asyncio.create_task(replace())
    await attempted.wait()
    release.set()
    assert await first == "pass_success"
    await replacement
    assert calls == ["original"]
    assert registry.required_initialization_pending(tenant)
    assert not registry.is_tenant_reconciled(tenant)
    assert await reconciler.reconcile_once() == "pass_success"
    assert calls == ["original", "new"]
    assert registry.is_tenant_reconciled(tenant)


@pytest.mark.asyncio
async def test_same_version_retry_after_required_callback_failure():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    failing = True

    def event_callback(_):
        if failing:
            raise RuntimeError("consumer unavailable")

    await registry.register_required_consumer(
        tenant, "ROUTE", resource, event_callback, lambda _: None
    )
    event = InvalidationEvent(tenant, "ROUTE", resource, 1)
    with pytest.raises(RuntimeError):
        await registry.observe(event)
    assert registry.observed_version(tenant) is None
    assert registry.reconciliation_required(tenant)
    failing = False
    assert await registry.observe(event) == "APPLIED"
    assert registry.observed_version(tenant) == 1
    assert registry.reconciliation_required(tenant)
    assert not registry.is_tenant_reconciled(tenant)
