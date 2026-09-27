"""M3-R3A required-consumer completeness, separate from observed version."""

import asyncio
from uuid import uuid4

import pytest

from app.control_plane.config_reconciler import ConfigReconciler
from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry


async def reconcile_at(registry, tenant, version):
    async def lookup(tenant_ids):
        assert tenant_ids == frozenset({tenant})
        return {tenant: version}

    return await registry.reconcile_fenced(tenant, lookup)


@pytest.mark.asyncio
async def test_failed_a_then_successful_b_advances_version_but_stays_dirty():
    registry = InvalidationRegistry()
    tenant, a, b = uuid4(), uuid4(), uuid4()

    def fail_a(event):
        raise RuntimeError("A failed")

    await registry.register_required_consumer(
        tenant, "ROUTE", a, fail_a, lambda _: None
    )
    await registry.register_required_consumer(
        tenant, "ROUTE", b, lambda _: None, lambda _: None
    )
    with pytest.raises(RuntimeError):
        await registry.observe(InvalidationEvent(tenant, "ROUTE", a, 11))
    assert registry.reconciliation_required(tenant)
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", b, 12)) == "APPLIED"
    )
    assert registry.applied_version(tenant) == 12
    assert registry.reconciliation_required(tenant)


@pytest.mark.asyncio
async def test_equal_version_dirty_reconciliation_runs_every_required_consumer():
    registry = InvalidationRegistry()
    tenant, a, b = uuid4(), uuid4(), uuid4()
    calls = []

    def fail_a(event):
        raise RuntimeError("A failed")

    await registry.register_required_consumer(
        tenant, "ROUTE", a, fail_a, lambda _: calls.append("A")
    )
    await registry.register_required_consumer(
        tenant, "ROUTE", b, lambda _: None, lambda _: calls.append("B")
    )
    with pytest.raises(RuntimeError):
        await registry.observe(InvalidationEvent(tenant, "ROUTE", a, 11))
    await registry.observe(InvalidationEvent(tenant, "ROUTE", b, 12))
    assert await reconcile_at(registry, tenant, 12) == "initialized"
    assert calls == ["A", "B"]
    assert not registry.reconciliation_required(tenant)
    assert not registry.required_initialization_pending(tenant)
    assert await reconcile_at(registry, tenant, 12) == "up_to_date"


@pytest.mark.asyncio
async def test_equal_version_dirty_without_pending_reconciles_again():
    registry = InvalidationRegistry()
    tenant, a, b = uuid4(), uuid4(), uuid4()
    calls = []
    fail = False

    def event_a(event):
        if fail:
            raise RuntimeError("A failed")

    await registry.register_required_consumer(
        tenant, "ROUTE", a, event_a, lambda _: calls.append("A")
    )
    await registry.register_required_consumer(
        tenant, "ROUTE", b, lambda _: None, lambda _: calls.append("B")
    )
    assert await reconcile_at(registry, tenant, 10) == "initialized"
    fail = True
    with pytest.raises(RuntimeError):
        await registry.observe(InvalidationEvent(tenant, "ROUTE", a, 11))
    await registry.observe(InvalidationEvent(tenant, "ROUTE", b, 12))
    assert not registry.required_initialization_pending(tenant)
    assert registry.reconciliation_required(tenant)

    async def lookup(tenant_ids):
        assert tenant_ids == frozenset({tenant})
        return {tenant: 12}

    assert await ConfigReconciler(registry, lookup).reconcile_once() == "pass_success"
    assert calls == ["A", "B", "A", "B"]
    assert not registry.reconciliation_required(tenant)


@pytest.mark.asyncio
async def test_partial_current_state_failure_preserves_dirty_until_full_success():
    registry = InvalidationRegistry()
    tenant = uuid4()
    calls = []
    fail = True

    def current_a(_):
        calls.append("A")

    def current_b(_):
        calls.append("B")
        if fail:
            raise RuntimeError("B failed")

    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, current_a
    )
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, current_b
    )
    with pytest.raises(RuntimeError):
        await reconcile_at(registry, tenant, 4)
    assert registry.reconciliation_required(tenant)
    assert registry.required_initialization_pending(tenant)
    assert registry.applied_version(tenant) is None
    fail = False
    assert await reconcile_at(registry, tenant, 4) == "initialized"
    assert calls == ["A", "B", "A", "B"]
    assert not registry.reconciliation_required(tenant)
    assert not registry.required_initialization_pending(tenant)
    assert registry.applied_version(tenant) == 4


@pytest.mark.asyncio
async def test_late_required_consumer_initializes_at_equal_version():
    registry = InvalidationRegistry()
    tenant = uuid4()
    assert await reconcile_at(registry, tenant, 4) == "initialized"
    calls = []
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, lambda t: calls.append(t)
    )
    assert registry.required_initialization_pending(tenant)
    assert await reconcile_at(registry, tenant, 4) == "initialized"
    assert calls == [tenant]
    assert not registry.required_initialization_pending(tenant)


@pytest.mark.asyncio
async def test_registration_during_active_pass_waits_and_remains_pending():
    registry = InvalidationRegistry()
    tenant = uuid4()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def current_a(_):
        entered.set()
        await release.wait()
        calls.append("A")

    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, current_a
    )
    first = asyncio.create_task(reconcile_at(registry, tenant, 1))
    await entered.wait()
    registration = asyncio.create_task(
        registry.register_required_consumer(
            tenant, "ROUTE", uuid4(), lambda _: None, lambda _: calls.append("B")
        )
    )
    release.set()
    assert await first == "initialized"
    await registration
    assert calls == ["A"]
    assert registry.required_initialization_pending(tenant)
    assert await reconcile_at(registry, tenant, 1) == "initialized"
    assert calls == ["A", "A", "B"]


@pytest.mark.asyncio
async def test_db_failure_preserves_dirty_version_and_pending():
    registry = InvalidationRegistry()
    tenant, a, b = uuid4(), uuid4(), uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", a, lambda _: None, lambda _: None
    )
    assert await reconcile_at(registry, tenant, 2) == "initialized"

    def fail_event(event):
        raise RuntimeError("event failure")

    await registry.register_required_consumer(
        tenant, "ROUTE", b, fail_event, lambda _: None
    )
    with pytest.raises(RuntimeError):
        await registry.observe(InvalidationEvent(tenant, "ROUTE", b, 3))

    async def fail_lookup(tenant_ids):
        raise RuntimeError("database failure")

    assert (
        await ConfigReconciler(registry, fail_lookup).reconcile_once() == "pass_error"
    )
    assert registry.applied_version(tenant) == 2
    assert registry.reconciliation_required(tenant)
    assert registry.required_initialization_pending(tenant)


@pytest.mark.asyncio
async def test_empty_membership_certifies_no_tenant_and_never_queries_database():
    registry = InvalidationRegistry()
    calls = []

    async def lookup(tenant_ids):
        calls.append(tenant_ids)
        return {}

    assert (
        await ConfigReconciler(registry, lookup).reconcile_once() == "empty_membership"
    )
    assert calls == []


@pytest.mark.asyncio
async def test_legacy_registration_cannot_replace_required_consumer_capabilities():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", resource, lambda _: None, lambda _: None
    )
    with pytest.raises(ValueError):
        await registry.register(tenant, "ROUTE", resource, lambda _: None)
    assert registry.required_initialization_pending(tenant)


@pytest.mark.asyncio
async def test_cancelled_required_event_keeps_completeness_dirty():
    registry = InvalidationRegistry()
    tenant, resource = uuid4(), uuid4()

    async def cancelled(event):
        raise asyncio.CancelledError

    await registry.register_required_consumer(
        tenant, "ROUTE", resource, cancelled, lambda _: None
    )
    with pytest.raises(asyncio.CancelledError):
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 1))
    assert registry.reconciliation_required(tenant)
    assert registry.applied_version(tenant) is None
