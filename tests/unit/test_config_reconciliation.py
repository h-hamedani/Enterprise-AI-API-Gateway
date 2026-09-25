import asyncio
from uuid import uuid4

import pytest

from app.control_plane.config_reconciler import ConfigReconciler
from app.control_plane.config_subscriber import InvalidationEvent, InvalidationRegistry


@pytest.mark.asyncio
async def test_resource_callback_failure_does_not_consume_version_and_can_retry():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    calls = []

    def callback(event):
        calls.append(event.version)
        if len(calls) == 1:
            raise ValueError("failed")

    await registry.register(tenant, "ROUTE", resource, callback)
    event = InvalidationEvent(tenant, "ROUTE", resource, 12)
    with pytest.raises(ValueError):
        await registry.observe(event)
    assert registry.version(tenant) == 0
    assert await registry.observe(event) == "APPLIED"
    assert await registry.observe(event) == "DUPLICATE"
    assert calls == [12, 12]


@pytest.mark.asyncio
async def test_resource_inflight_duplicate_waits_and_different_tenant_proceeds():
    tenant, other, resource = uuid4(), uuid4(), uuid4()
    registry = InvalidationRegistry()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def callback(event):
        calls.append(event.version)
        entered.set()
        await release.wait()

    await registry.register(tenant, "ROUTE", resource, callback)
    event = InvalidationEvent(tenant, "ROUTE", resource, 12)
    first = asyncio.create_task(registry.observe(event))
    await entered.wait()
    second = asyncio.create_task(registry.observe(event))
    assert (
        await registry.observe(InvalidationEvent(other, "ROUTE", resource, 1))
        == "APPLIED"
    )
    release.set()
    assert await first == "APPLIED"
    assert await second == "DUPLICATE"
    assert calls == [12]


@pytest.mark.asyncio
async def test_late_tenant_callback_initializes_at_equal_or_lower_version():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    await registry.register(tenant, "ROUTE", resource, lambda event: None)
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 9))
    seen = []
    await registry.register_tenant_callback(
        tenant, lambda version: seen.append(version)
    )
    assert tenant in registry.tenant_ids()
    assert await registry.reconcile(tenant, 8) == "initialized"
    assert registry.version(tenant) == 9
    assert await registry.reconcile(tenant, 8) == "stale_db_ignored"
    assert seen == [8]


@pytest.mark.asyncio
async def test_failed_tenant_initialization_retries_and_replacement_reinitializes():
    tenant = uuid4()
    registry = InvalidationRegistry()
    calls = []

    def failing(version):
        calls.append(version)
        raise ValueError("failed")

    await registry.register_tenant_callback(tenant, failing)
    with pytest.raises(ValueError):
        await registry.reconcile(tenant, 0)
    assert registry.applied_version(tenant) is None
    await registry.register_tenant_callback(
        tenant, lambda version: calls.append(version)
    )
    assert await registry.reconcile(tenant, 0) == "initialized"
    assert registry.applied_version(tenant) == 0
    assert await registry.reconcile(tenant, 0) == "up_to_date"
    assert calls == [0, 0]


@pytest.mark.asyncio
async def test_tenant_callback_failure_preserves_version_then_newer_event_supersedes():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    await registry.register(tenant, "ROUTE", resource, lambda event: None)
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 10))
    await registry.register_tenant_callback(tenant, lambda version: None)
    await registry.reconcile(tenant, 10)

    def failing(version):
        raise ValueError("failed")

    await registry.register_tenant_callback(tenant, failing)
    with pytest.raises(ValueError):
        await registry.reconcile(tenant, 12)
    assert registry.version(tenant) == 10
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 13))
        == "APPLIED"
    )
    assert registry.version(tenant) == 13


@pytest.mark.asyncio
async def test_resource_registration_adds_process_lifetime_membership():
    tenant = uuid4()
    registry = InvalidationRegistry()
    assert registry.tenant_ids() == frozenset()
    await registry.register(tenant, "ROUTE", uuid4(), lambda event: None)
    assert registry.tenant_ids() == frozenset({tenant})
    assert InvalidationRegistry().tenant_ids() == frozenset()


@pytest.mark.asyncio
async def test_failed_inflight_resource_duplicate_retries_and_newer_supersedes():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def callback(event):
        calls.append(event.version)
        if len(calls) == 1:
            entered.set()
            await release.wait()
            raise ValueError("failed")

    await registry.register(tenant, "ROUTE", resource, callback)
    event = InvalidationEvent(tenant, "ROUTE", resource, 12)
    first = asyncio.create_task(registry.observe(event))
    await entered.wait()
    second = asyncio.create_task(registry.observe(event))
    release.set()
    with pytest.raises(ValueError):
        await first
    assert await second == "APPLIED"
    assert registry.version(tenant) == 12
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 13))
        == "APPLIED"
    )
    assert registry.version(tenant) == 13
    assert calls == [12, 12, 13]


@pytest.mark.asyncio
async def test_reconciliation_and_pubsub_race_remains_monotonic():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    await registry.register(tenant, "ROUTE", resource, lambda event: None)
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 10))
    entered, release = asyncio.Event(), asyncio.Event()

    async def tenant_callback(version):
        if version == 12:
            entered.set()
            await release.wait()

    await registry.register_tenant_callback(tenant, tenant_callback)
    await registry.reconcile(tenant, 10)
    reconcile = asyncio.create_task(registry.reconcile(tenant, 12))
    await entered.wait()
    event = asyncio.create_task(
        registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 13))
    )
    release.set()
    assert await reconcile == "reconciled"
    assert await event == "APPLIED"
    assert registry.version(tenant) == 13
    assert await registry.reconcile(tenant, 12) == "stale_db_ignored"


@pytest.mark.asyncio
async def test_callback_replacement_during_old_initialization_remains_pending():
    tenant = uuid4()
    registry = InvalidationRegistry()
    entered, release = asyncio.Event(), asyncio.Event()
    replacement_calls = []

    async def old_callback(version):
        entered.set()
        await release.wait()

    await registry.register_tenant_callback(tenant, old_callback)
    first = asyncio.create_task(registry.reconcile(tenant, 4))
    await entered.wait()
    replacement = asyncio.create_task(
        registry.register_tenant_callback(tenant, replacement_calls.append)
    )
    await asyncio.sleep(0)
    assert not replacement.done()
    release.set()
    assert await first == "initialized"
    await replacement
    assert await registry.reconcile(tenant, 4) == "initialized"
    assert replacement_calls == [4]
    assert await registry.reconcile(tenant, 4) == "up_to_date"


@pytest.mark.asyncio
async def test_resource_registration_waits_for_same_tenant_application():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def old_callback(event):
        calls.append("old")
        entered.set()
        await release.wait()

    await registry.register(tenant, "ROUTE", resource, old_callback)
    first = asyncio.create_task(
        registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 12))
    )
    await entered.wait()
    replacement = asyncio.create_task(
        registry.register(tenant, "ROUTE", resource, lambda event: calls.append("new"))
    )
    await asyncio.sleep(0)
    assert not replacement.done()
    release.set()
    assert await first == "APPLIED"
    await replacement
    assert registry.version(tenant) == 12
    assert (
        await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 13))
        == "APPLIED"
    )
    assert calls == ["old", "new"]


@pytest.mark.asyncio
async def test_missing_row_normal_mode_does_not_rollback_or_repeat_callback():
    tenant, resource = uuid4(), uuid4()
    registry = InvalidationRegistry()
    await registry.register(tenant, "ROUTE", resource, lambda event: None)
    assert await registry.reconcile(tenant, 0) == "initialized"
    assert await registry.reconcile(tenant, 0) == "up_to_date"
    await registry.observe(InvalidationEvent(tenant, "ROUTE", resource, 3))
    assert await registry.reconcile(tenant, 0) == "stale_db_ignored"
    assert registry.version(tenant) == 3


@pytest.mark.asyncio
async def test_empty_membership_pass_skips_database_and_later_registration_joins():
    registry = InvalidationRegistry()
    calls = []

    async def lookup(tenant_ids):
        calls.append(tenant_ids)
        return {tenant: 4 for tenant in tenant_ids}

    reconciler = ConfigReconciler(registry, lookup)
    assert await reconciler.reconcile_once() == "empty_membership"
    assert calls == []
    tenant = uuid4()
    observed = []
    await registry.register_tenant_callback(tenant, observed.append)
    assert await reconciler.reconcile_once() == "pass_success"
    assert calls == [frozenset({tenant})]
    assert observed == [4]
    assert registry.version(tenant) == 4


@pytest.mark.asyncio
async def test_lookup_failure_does_not_change_any_tenant_and_next_pass_recovers(
    caplog,
):
    tenant = uuid4()
    registry = InvalidationRegistry()
    observed = []
    await registry.register_tenant_callback(tenant, observed.append)
    attempts = 0

    async def lookup(tenant_ids):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("sensitive connection detail")
        return {tenant: 0}

    reconciler = ConfigReconciler(registry, lookup)
    assert await reconciler.reconcile_once() == "pass_error"
    assert registry.applied_version(tenant) is None
    assert observed == []
    assert "sensitive connection detail" not in caplog.text
    assert await reconciler.reconcile_once() == "pass_success"
    assert registry.applied_version(tenant) == 0
    assert observed == [0]


@pytest.mark.asyncio
async def test_managed_loop_starts_immediately_and_stops_without_orphan():
    registry = InvalidationRegistry()
    first_pass = asyncio.Event()
    wait_started = asyncio.Event()

    async def lookup(tenant_ids):
        first_pass.set()
        return {tenant: 1 for tenant in tenant_ids}

    async def sleep(delay):
        assert 27 <= delay <= 33
        wait_started.set()
        await asyncio.Event().wait()

    await registry.register_tenant_callback(uuid4(), lambda version: None)
    reconciler = ConfigReconciler(
        registry, lookup, delay_chooser=lambda: 27, sleep=sleep
    )
    await reconciler.start()
    await asyncio.wait_for(first_pass.wait(), 1)
    await asyncio.wait_for(wait_started.wait(), 1)
    assert reconciler.running
    await reconciler.stop()
    assert not reconciler.running


@pytest.mark.asyncio
async def test_empty_managed_first_pass_avoids_lookup_then_late_registration_joins():
    registry = InvalidationRegistry()
    sleeping, advance, observed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    lookup_calls = []

    async def lookup(tenant_ids):
        lookup_calls.append(tenant_ids)
        return dict.fromkeys(tenant_ids, 2)

    async def sleep(delay):
        sleeping.set()
        await advance.wait()
        advance.clear()

    reconciler = ConfigReconciler(registry, lookup, sleep=sleep)
    await reconciler.start()
    try:
        await asyncio.wait_for(sleeping.wait(), 1)
        assert lookup_calls == []
        tenant = uuid4()
        await registry.register_tenant_callback(tenant, lambda version: observed.set())
        advance.set()
        await asyncio.wait_for(observed.wait(), 1)
        assert lookup_calls == [frozenset({tenant})]
        assert registry.version(tenant) == 2
        assert reconciler.running
    finally:
        await reconciler.stop()
