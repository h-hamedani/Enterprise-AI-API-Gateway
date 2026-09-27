"""M3.9 recovery cutover and M3-R10–R12 provenance contracts."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.control_plane.config_subscriber import InvalidationRegistry
from app.control_plane.protection import ControlPlaneProtection
from app.core.config import Settings
from app.persistence.models.enums import RateScopeType
from app.redis.rate_limit import (
    RateLimitDependencyError,
    RateLimitResult,
    ResolvedRatePolicy,
)
from app.redis.recovery import (
    Backend,
    CircuitCompletionDisposition,
    CircuitCompletionResult,
    RecoveryCoordinator,
    TrafficMode,
)


class Availability:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    async def check(self):
        self.calls += 1
        return SimpleNamespace(available=self.results.pop(0))


class PausedFinalAvailability(Availability):
    def __init__(self):
        super().__init__(True, True)
        self.final_entered = asyncio.Event()
        self.final_release = asyncio.Event()

    async def check(self):
        if self.calls == 1:
            self.final_entered.set()
            await self.final_release.wait()
        return await super().check()


class TrackedAdmissionBarrier:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.entries = 0
        self.cutover_waiting = asyncio.Event()

    async def __aenter__(self):
        self.entries += 1
        if self.entries == 3:
            self.cutover_waiting.set()
        await self.lock.acquire()

    async def __aexit__(self, *_):
        self.lock.release()


class MutableAvailability:
    def __init__(self):
        self.calls = 0
        self.unavailable = False

    async def check(self):
        self.calls += 1
        return SimpleNamespace(available=not self.unavailable)


class Reconciler:
    def __init__(self, registry):
        self.registry = registry
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def reconcile_once(self):
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return "empty_membership"


@pytest.mark.asyncio
async def test_recovery_checks_twice_and_switches_all_new_admissions_atomically():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    availability = Availability(True, True)
    subscriber = SimpleNamespace(ready=True)
    coordinator = RecoveryCoordinator(availability, subscriber, reconciler, registry)
    coordinator.mark_unreachable()
    local = coordinator.active_generation
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS
    assert await coordinator.attempt_recovery() == "recovery_succeeded"
    assert availability.calls == 2
    assert reconciler.calls == 1
    assert coordinator.mode is TrafficMode.NORMAL
    assert coordinator.active_generation is None
    assert local in coordinator.retired_generations.values()


@pytest.mark.asyncio
async def test_recovering_keeps_local_admission_and_final_redis_failure_preserves_it():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    coordinator = RecoveryCoordinator(
        Availability(True, False), SimpleNamespace(ready=True), reconciler, registry
    )
    coordinator.mark_unreachable()
    local = coordinator.active_generation
    task = asyncio.create_task(coordinator.attempt_recovery())
    await reconciler.entered.wait()
    assert coordinator.mode is TrafficMode.RECOVERING_REDIS
    async with coordinator.admission() as selected:
        assert selected is local
    reconciler.release.set()
    assert await task == "final_redis_check_failed"
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS
    assert coordinator.active_generation is local


@pytest.mark.asyncio
async def test_unreconciled_registered_tenant_blocks_cutover():
    registry = InvalidationRegistry()
    tenant = uuid4()
    await registry.register_required_consumer(
        tenant, "ROUTE", uuid4(), lambda _: None, lambda _: None
    )
    reconciler = Reconciler(registry)
    reconciler.release.set()
    coordinator = RecoveryCoordinator(
        Availability(True), SimpleNamespace(ready=True), reconciler, registry
    )
    coordinator.mark_unreachable()
    assert await coordinator.attempt_recovery() == "config_not_reconciled"
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS


@pytest.mark.asyncio
async def test_stalled_reconciliation_times_out_without_cutover():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    coordinator = RecoveryCoordinator(
        Availability(True),
        SimpleNamespace(ready=True),
        reconciler,
        registry,
        reconcile_timeout_seconds=0.01,
    )
    coordinator.mark_unreachable()
    local = coordinator.active_generation
    assert await coordinator.attempt_recovery() == "config_reconciliation_timeout"
    assert reconciler.calls == 1
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS
    assert coordinator.active_generation is local


@pytest.mark.asyncio
async def test_registration_during_recovery_invalidates_cutover_certificate():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    coordinator = RecoveryCoordinator(
        Availability(True, True), SimpleNamespace(ready=True), reconciler, registry
    )
    coordinator.mark_unreachable()
    task = asyncio.create_task(coordinator.attempt_recovery())
    await reconciler.entered.wait()
    await registry.register_required_consumer(
        uuid4(), "ROUTE", uuid4(), lambda _: None, lambda _: None
    )
    reconciler.release.set()
    assert await task == "certificate_invalidated"
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS


@pytest.mark.asyncio
async def test_event_after_reconciliation_invalidates_cutover_certificate():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    availability = PausedFinalAvailability()
    coordinator = RecoveryCoordinator(
        availability, SimpleNamespace(ready=True), reconciler, registry
    )
    coordinator.mark_unreachable()
    task = asyncio.create_task(coordinator.attempt_recovery())
    await availability.final_entered.wait()
    registry._invalidate_certificate(uuid4())
    availability.final_release.set()
    assert await task == "certificate_invalidated"
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS


@pytest.mark.asyncio
async def test_subscriber_disconnect_at_final_boundary_preserves_local_generation():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    availability = PausedFinalAvailability()
    subscriber = SimpleNamespace(ready=True)
    coordinator = RecoveryCoordinator(availability, subscriber, reconciler, registry)
    coordinator.mark_unreachable()
    local = coordinator.active_generation
    task = asyncio.create_task(coordinator.attempt_recovery())
    await availability.final_entered.wait()
    subscriber.ready = False
    availability.final_release.set()
    assert await task == "subscriber_not_ready"
    assert coordinator.active_generation is local
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS


@pytest.mark.asyncio
async def test_subscriber_reconnect_after_reconciliation_requires_new_pass():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    availability = PausedFinalAvailability()
    subscriber = SimpleNamespace(ready=True, subscription_epoch=1)
    coordinator = RecoveryCoordinator(availability, subscriber, reconciler, registry)
    coordinator.mark_unreachable()
    task = asyncio.create_task(coordinator.attempt_recovery())
    await availability.final_entered.wait()
    subscriber.ready = False
    subscriber.subscription_epoch = 2
    subscriber.ready = True
    availability.final_release.set()
    assert await task == "subscriber_reconnected"
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS
    assert reconciler.calls == 1


@pytest.mark.asyncio
async def test_new_admission_waits_for_atomic_cutover_boundary():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    availability = PausedFinalAvailability()
    coordinator = RecoveryCoordinator(
        availability, SimpleNamespace(ready=True), reconciler, registry
    )
    coordinator.mark_unreachable()
    admission_waiting = asyncio.Event()
    selected = asyncio.Event()
    seen = []

    async def new_admission():
        admission_waiting.set()
        async with coordinator.admission() as local:
            seen.append(local)
            selected.set()

    recovery_task = asyncio.create_task(coordinator.attempt_recovery())
    await availability.final_entered.wait()
    admission_task = asyncio.create_task(new_admission())
    await admission_waiting.wait()
    assert not selected.is_set()
    availability.final_release.set()
    assert await recovery_task == "recovery_succeeded"
    await admission_task
    assert seen == [None]


@pytest.mark.asyncio
async def test_redis_goes_down_while_recovery_waits_for_cutover_barrier():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    availability = MutableAvailability()
    coordinator = RecoveryCoordinator(
        availability, SimpleNamespace(ready=True), reconciler, registry
    )
    barrier = TrackedAdmissionBarrier()
    coordinator._admission_lock = barrier
    coordinator.mark_unreachable()
    local = coordinator.active_generation
    recovery = asyncio.create_task(coordinator.attempt_recovery())
    await reconciler.entered.wait()
    assert coordinator.mode is TrafficMode.RECOVERING_REDIS

    selected = asyncio.Event()
    release = asyncio.Event()

    async def held_admission():
        async with coordinator.admission() as generation:
            assert generation is local
            selected.set()
            await release.wait()

    admission = asyncio.create_task(held_admission())
    await selected.wait()
    reconciler.release.set()
    try:
        await barrier.cutover_waiting.wait()
        calls_before_release = availability.calls
        availability.unavailable = True
    finally:
        release.set()
        await admission
    outcome = await recovery
    assert calls_before_release == 1
    assert outcome == "final_redis_check_failed"
    assert availability.calls == 2
    assert coordinator.mode is TrafficMode.DEGRADED_REDIS
    assert coordinator.active_generation is local


def test_circuit_orchestration_result_rejects_inconsistent_states():
    with pytest.raises(ValueError):
        CircuitCompletionResult(True, None, CircuitCompletionDisposition.APPLIED)
    with pytest.raises(ValueError):
        CircuitCompletionResult(True, None, CircuitCompletionDisposition.UNKNOWN_HANDLE)
    unknown = CircuitCompletionResult(
        False, None, CircuitCompletionDisposition.UNKNOWN_HANDLE
    )
    assert unknown.resulting_state is None
    assert Backend.LOCAL != Backend.REDIS


@pytest.mark.asyncio
async def test_admin_rate_protection_uses_shared_recovery_mode():
    registry = InvalidationRegistry()
    reconciler = Reconciler(registry)
    reconciler.release.set()
    owner = RecoveryCoordinator(
        Availability(True, True), SimpleNamespace(ready=True), reconciler, registry
    )
    guard = ControlPlaneProtection(SimpleNamespace(), Settings(), recovery=owner)

    class RedisBucket:
        def __init__(self):
            self.fail = True
            self.calls = 0

        async def evaluate(self, policies):
            self.calls += 1
            if self.fail:
                raise RateLimitDependencyError()
            return RateLimitResult(True, 0)

    bucket = RedisBucket()
    guard._bucket = bucket
    item = ResolvedRatePolicy(
        uuid4(), uuid4(), RateScopeType.ADMIN_TOKEN, uuid4(), 100, 60
    )
    await guard._evaluate([item])
    assert owner.mode is TrafficMode.DEGRADED_REDIS
    bucket.fail = False
    await guard._evaluate([item])
    assert bucket.calls == 1
    assert await owner.attempt_recovery() == "recovery_succeeded"
    await guard._evaluate([item])
    assert bucket.calls == 2
