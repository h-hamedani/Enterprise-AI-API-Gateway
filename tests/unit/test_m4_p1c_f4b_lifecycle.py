"""F4B lifecycle integration; controlled clocks and explicit scheduling gates."""

import asyncio
from dataclasses import FrozenInstanceError

import pytest

from app.normal_api.lifecycle import End
from app.redis.concurrency import RenewResult
from app.redis.guarded_renewal import (
    GuardedRenewalOperation,
)
from app.redis.guarded_renewal import (
    RenewalAttemptStatus as Status,
)
from app.redis.guarded_renewal import (
    RenewalDisposition as Disposition,
)
from tests.unit.test_m4_p1c_lifecycle import change, run, world


async def held_world(**kwargs):
    w = await world(**kwargs)
    w.entered, w.finish = asyncio.Event(), asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        w.entered.set()
        await w.finish.wait()

    w.runner = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await w.entered.wait()
    return w


@pytest.mark.asyncio
async def test_k04_k13_delayed_dispatch_guard_after_upstream_outcome():
    w = await world()
    prepared, resume = asyncio.Event(), asyncio.Event()
    writes = []

    async def guarded(policies, handle, *, guard):
        op = GuardedRenewalOperation()
        prepared.set()
        await resume.wait()
        stamp = op.authorize(guard)
        if stamp is None:
            return op.finish(Disposition.SUPPRESSED)
        writes.append(stamp)
        op.begin(stamp)
        op.resolve(Status.RESULT, result=RenewResult(True))
        return op.finish(Disposition.BACKEND_RESULT)

    w.concurrency.renew_guarded = guarded
    delivery = asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        delivery.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await delivery.wait()
    await w.clock.expect(2)
    w.clock.advance(2)
    await prepared.wait()
    assert w.lifecycle.upstream is not None and w.lifecycle.end is None
    w.clock.advance(4)
    resume.set()
    result = await task
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert writes == [] and w.lifecycle._confirmed_anchor == 0
    assert w.concurrency.released == [w.handle]
    assert w.circuit.completed == [w.lifecycle.upstream.classification]


@pytest.mark.asyncio
async def test_k05_producing_retry_timestamp_and_immutable_snapshot():
    w = await world()
    published = []

    async def guarded(policies, handle, *, guard):
        op = GuardedRenewalOperation()
        stamp = op.authorize(guard)
        op.begin(stamp)
        op.resolve(Status.NOSCRIPT)
        w.clock.advance(2.5)
        op.begin(op.authorize(guard))
        op.resolve(Status.RESULT, result=RenewResult(True))
        outcome = op.finish(Disposition.BACKEND_RESULT)
        published.append(outcome)
        return outcome

    w.concurrency.renew_guarded = guarded
    entered, finish = asyncio.Event(), asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        entered.set()
        await finish.wait()

    task = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await entered.wait()
    await w.clock.expect(2)
    w.clock.advance(2)
    await w.clock.expect(4.5)
    assert w.lifecycle._confirmed_anchor == 2.5
    assert w.lifecycle._confidence_deadline == 6.5
    with pytest.raises(FrozenInstanceError):
        published[0].dispatched_at = 100
    finish.set()
    assert (await task).end == End.DELIVERED


@pytest.mark.asyncio
async def test_k09_unexplained_suppression_is_internal_not_backend_loss():
    w = await world()

    async def guarded(*args, **kwargs):
        return GuardedRenewalOperation().finish(Disposition.SUPPRESSED)

    w.concurrency.renew_guarded = guarded
    entered = asyncio.Event()

    async def body(resolved, receiver):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, body=body)))
    await entered.wait()
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await task
    assert result.end == End.INTERNAL
    assert result.error.code == "gateway_internal_error"
    assert w.concurrency.released == [w.handle]
    assert "circuit" not in w.calls and "renew" not in w.calls


@pytest.mark.asyncio
async def test_k12_uncertain_attempt_survives_later_success_and_runner_exit():
    w = await world()
    outcomes = []

    async def guarded(policies, handle, *, guard):
        op = GuardedRenewalOperation()
        op.begin(op.authorize(guard))
        if not outcomes:
            result = op.finish(Disposition.UNKNOWN)
        else:
            op.resolve(Status.RESULT, result=RenewResult(True))
            result = op.finish(Disposition.BACKEND_RESULT)
        outcomes.append(result)
        return result

    w.concurrency.renew_guarded = guarded
    entered, finish = asyncio.Event(), asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        entered.set()
        await finish.wait()

    task = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await entered.wait()
    await w.clock.expect(2)
    w.clock.advance(2)
    await w.clock.expect(3)
    w.clock.advance(3)
    await w.clock.expect(5)
    assert w.lifecycle._confirmed_anchor == 3
    assert w.lifecycle.renewal_obligation == "unresolved"
    assert outcomes[0].attempts[0].status is Status.UNRESOLVED
    finish.set()
    result = await task
    assert not result.cleanup_complete
    assert w.deadline_scope in w.supervisor.residuals
    assert len(w.lifecycle.unresolved_renewals) == 1
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_k16_actual_guarded_transport_does_not_start_disconnect_after_cutoff():
    """Integration probe: existing F4A transport must obey the R1 cutoff."""
    from types import SimpleNamespace
    from uuid import uuid4

    from app.normal_api.lifecycle import Policies
    from app.normal_api.supervisor import LifecycleSupervisor
    from app.persistence.models.enums import RateScopeType
    from app.redis.concurrency import ResolvedConcurrencyPolicy
    from app.redis.protection_facade import RecoveryConcurrencySemaphore
    from tests.unit.test_guarded_renewal import setup

    w = await world()
    w.supervisor = LifecycleSupervisor(3, clock=w.clock)
    w.deadline_scope = w.supervisor.register()
    primitive, connection, _, _ = setup()
    facade = RecoveryConcurrencySemaphore(
        primitive, SimpleNamespace(owner_id=w.handle.owner_id)
    )
    w.concurrency.renew_guarded = facade.renew_guarded
    entered, drain = asyncio.Event(), asyncio.Event()
    cleanup_entries = []
    original_disconnect = connection.disconnect

    async def blocked_drain():
        entered.set()
        await drain.wait()

    async def disconnect(**kwargs):
        cleanup_entries.append((w.clock.now(), w.deadline_scope.expired))
        await original_disconnect(**kwargs)

    connection._writer.drain = blocked_drain
    connection.disconnect = disconnect

    async def policies(resolved):
        return Policies(
            (),
            (
                ResolvedConcurrencyPolicy(
                    uuid4(),
                    resolved.identity.tenant_id,
                    RateScopeType.SERVICE,
                    resolved.service["id"],
                    1,
                ),
            ),
        )

    async def body(*args):
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, policies=policies, body=body)))
    try:
        await w.clock.expect(2)
        w.supervisor.begin_shutdown()
        w.clock.advance(2)
        await asyncio.wait_for(entered.wait(), 3)
        # Shutdown started at t=0; its existing cutoff is t=3, before R8 t=4.
        w.clock.advance(3)
        result = await asyncio.wait_for(task, 3)
        await asyncio.wait_for(
            asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True), 3
        )
        assert result.end == End.SHUTDOWN
        assert cleanup_entries == [], cleanup_entries
    finally:
        drain.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)


async def start_blocked(w, *, stage="body", **overrides):
    """Start a real lifecycle, retaining deterministic stage acknowledgements."""
    w.entered, w.finish = asyncio.Event(), asyncio.Event()
    original = getattr(w.hooks, stage)

    async def block(*args):
        if stage == "deliver":
            args[0].committed_downstream()
        w.entered.set()
        await w.finish.wait()
        return await original(*args)

    w.runner = asyncio.create_task(
        run(w, change(w.hooks, **{stage: block}, **overrides))
    )
    await asyncio.wait_for(w.entered.wait(), 3)
    return w


async def finish_blocked(w):
    w.finish.set()
    return await asyncio.wait_for(w.runner, 3)


def attach_backend(w, primitive):
    from types import SimpleNamespace
    from uuid import uuid4

    from app.normal_api.lifecycle import Policies
    from app.persistence.models.enums import RateScopeType
    from app.redis.concurrency import ResolvedConcurrencyPolicy
    from app.redis.protection_facade import RecoveryConcurrencySemaphore

    coordinator = SimpleNamespace(
        owner_id=w.handle.owner_id,
        mark_unreachable=lambda: None,
        local_generation=lambda generation: None,
    )
    facade = RecoveryConcurrencySemaphore(primitive, coordinator)
    w.concurrency.renew_guarded = facade.renew_guarded
    policy = ResolvedConcurrencyPolicy(
        uuid4(),
        w.auth.identity.tenant_id,
        RateScopeType.SERVICE,
        w.rows["services"][0]["id"],
        1,
    )

    async def policies(resolved):
        return Policies((), (policy,))

    return facade, policies


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage", ["task", "script", "pool", "auth", "health", "reconnect"]
)
async def test_k04_integrated_preparation_delay_reaches_live_writer_guard(stage):
    from tests.unit.test_guarded_renewal import setup

    w = await world()
    primitive, connection, scripts, transport = setup()
    facade, policies = attach_backend(w, primitive)
    entered, resume = asyncio.Event(), asyncio.Event()
    decisions = []
    original_permit = w.lifecycle._renewal_guard.permit

    def permit(stamp):
        result = original_permit(stamp)
        decisions.append((stamp, result))
        return result

    w.lifecycle._renewal_guard.permit = permit

    class Gate:
        async def wait(self):
            entered.set()
            while not resume.is_set():
                try:
                    await resume.wait()
                except asyncio.CancelledError:
                    # Controlled non-cooperative preparation, to prove the final
                    # guard rather than merely cancellation of an outer wrapper.
                    continue

    gate = Gate()
    if stage == "script":
        scripts.gate = gate
    elif stage == "pool":
        transport.pool.checkout = gate
    elif stage == "health":
        connection.health = gate
    elif stage in {"auth", "reconnect"}:
        connection.preparation = gate
    else:

        async def delayed(*args, **kwargs):
            await gate.wait()
            return await facade.renew_guarded(*args, **kwargs)

        w.concurrency.renew_guarded = delayed
    await start_blocked(w, policies=policies)
    try:
        await w.clock.expect(2)
        w.clock.advance(2)
        await asyncio.wait_for(entered.wait(), 3)
        w.clock.advance(4)
        await asyncio.wait_for(w.lifecycle._terminal_event.wait(), 3)
        resume.set()
        result = await asyncio.wait_for(w.runner, 3)
        assert result.end == End.OWNERSHIP_UNCERTAIN
        assert decisions and all(
            not allowed and stamp >= 4 for stamp, allowed in decisions
        )
        assert not connection._writer.writes
        assert w.lifecycle._confirmed_anchor == 0
        assert w.concurrency.released == [w.handle]
        assert "circuit" not in w.calls
    finally:
        resume.set()
        w.finish.set()
        w.runner.cancel()
        await asyncio.gather(w.runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "disposition", [Disposition.GUARD_ERROR, Disposition.INTERNAL_ERROR]
)
async def test_k10_guarded_internal_dispositions_do_not_become_loss_or_unknown(
    disposition,
):
    w = await world()

    async def guarded(*args, **kwargs):
        return GuardedRenewalOperation().finish(disposition)

    w.concurrency.renew_guarded = guarded
    await start_blocked(w)
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await asyncio.wait_for(w.runner, 3)
    assert result.end == End.INTERNAL and result.error.code == "gateway_internal_error"
    assert w.lifecycle._confirmed_anchor == 0
    assert w.concurrency.released == [w.handle] and "circuit" not in w.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["acl", "protocol", "availability"])
async def test_k10_actual_backend_error_classification_and_evidence(kind):
    from redis.exceptions import BusyLoadingError, NoPermissionError

    from tests.unit.test_guarded_renewal import setup

    w = await world()
    primitive, connection, _, _ = setup()
    _, policies = attach_backend(w, primitive)
    error = (
        NoPermissionError("sensitive fixture")
        if kind == "acl"
        else BusyLoadingError("sensitive fixture")
    )
    connection.replies = [error if kind != "protocol" else b"invalid"]
    await start_blocked(w, policies=policies)
    await w.clock.expect(2)
    w.clock.advance(2)
    if kind == "availability":
        await w.clock.expect(3)
        assert w.lifecycle.end is None and w.lifecycle._confirmed_anchor == 0
        outcome = w.lifecycle.last_renewal_outcome
        assert outcome.disposition is Disposition.UNKNOWN
        assert outcome.attempts[-1].status is Status.RESOLVED_ERROR
        w.lifecycle.cancel(End.CANCELLED)
    result = await asyncio.wait_for(w.runner, 3)
    assert result.end == (End.CANCELLED if kind == "availability" else End.INTERNAL)
    outcome = w.lifecycle.last_renewal_outcome
    assert outcome.result is None
    assert outcome.attempts[-1].status is (
        Status.UNRESOLVED if kind == "protocol" else Status.RESOLVED_ERROR
    )
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["preparation", "drain", "response", "noscript"])
async def test_k11_lifecycle_cancellation_preserves_actual_operation_history(phase):
    import gc
    import weakref

    from redis.exceptions import NoScriptError

    from tests.unit.test_guarded_renewal import Gate, setup

    w = await world()
    primitive, connection, scripts, _ = setup()
    _, policies = attach_backend(w, primitive)
    gate = Gate()
    if phase == "preparation":
        scripts.gate = gate
    elif phase == "drain":
        connection._writer.gate = gate
    elif phase == "response":
        connection.reply_gate = gate
    else:
        connection.replies = [NoScriptError()]
        load = primitive._load

        async def reload(*args, **kwargs):
            if kwargs.get("force"):
                await gate.wait()
            return await load(*args, **kwargs)

        primitive._load = reload
    await start_blocked(w, policies=policies)
    try:
        await w.clock.expect(2)
        w.clock.advance(2)
        await asyncio.wait_for(gate.entered.wait(), 3)
        operation = w.lifecycle._owned_tasks[
            "renew_operation"
        ].guarded_renewal_operation
        before = operation.attempts
        record_ref = weakref.ref(operation)
        task_ref = weakref.ref(w.lifecycle._owned_tasks["renew_operation"])
        w.lifecycle.cancel(End.DISCONNECT)
        result = await asyncio.wait_for(w.runner, 3)
        assert result.end == End.DISCONNECT and result.error is None
        assert operation.cancelled and operation.attempts == before
        assert operation.outcome is None
        assert operation.phase == ("preparation" if phase == "noscript" else phase)
        if phase in {"drain", "response"}:
            assert before[-1].status is Status.UNRESOLVED
            assert not result.cleanup_complete
            assert w.deadline_scope in w.supervisor.residuals
        elif phase == "noscript":
            assert before[-1].status is Status.NOSCRIPT
        else:
            assert before == ()
        assert w.concurrency.released == [w.handle]
        del operation
        acknowledged = asyncio.Event()
        asyncio.get_running_loop().call_soon(acknowledged.set)
        await acknowledged.wait()
        gc.collect()
        assert record_ref() is None and task_ref() is None
        evidence = w.lifecycle.last_renewal_evidence
        assert evidence.phase == ("preparation" if phase == "noscript" else phase)
        assert evidence.cancelled and evidence.outcome is None
        assert evidence.attempts == before
        summary = w.lifecycle.renewal_evidence
        assert summary.live_invocations == summary.physical_obligations == 0
        assert dict(summary.cancellation_phase_counts)[evidence.phase] == 1
    finally:
        gate.resume.set()
        w.finish.set()
        w.runner.cancel()
        await asyncio.gather(w.runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["terminal", "process", "request", "horizon"])
async def test_k09_expected_suppression_preserves_owner_boundary(cause):
    w = await world()
    entered = asyncio.Event()

    async def guarded(*args, **kwargs):
        operation = GuardedRenewalOperation()
        if cause == "terminal":
            w.lifecycle.cancel(End.DISCONNECT)
        elif cause == "process":
            w.supervisor.grace_seconds = (
                1  # Explicit test configuration; cutoff precedes R8.
            )
            w.supervisor.begin_shutdown()
            w.clock.advance(w.supervisor.process_deadline)
        elif cause == "request":
            w.supervisor.grace_seconds = 1
            w.deadline_scope.begin_cleanup()
            w.clock.advance(w.deadline_scope.deadline)
        else:
            w.clock.advance(4)
        entered.set()
        return operation.finish(Disposition.SUPPRESSED)

    w.concurrency.renew_guarded = guarded
    await start_blocked(w)
    await w.clock.expect(2)
    w.clock.advance(2)
    await asyncio.wait_for(entered.wait(), 3)
    result = await asyncio.wait_for(w.runner, 3)
    assert (
        result.end
        == {
            "terminal": End.DISCONNECT,
            "process": End.SHUTDOWN,
            "request": End.SHUTDOWN,
            "horizon": End.OWNERSHIP_UNCERTAIN,
        }[cause]
    )
    assert w.lifecycle._confirmed_anchor == 0
    assert w.lifecycle.last_renewal_outcome.disposition is Disposition.SUPPRESSED
    assert w.lifecycle.last_renewal_outcome.result is None
    assert len(w.concurrency.released) <= 1


@pytest.mark.asyncio
async def test_context_isolation_concurrency_nesting_child_lifetime_and_cancellation():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import (
        _cleanup_allowed,
        _cleanup_scope,
        managed_renewal_cleanup,
    )
    from tests.unit.test_m4_p1c_lifecycle import Clock

    clock = Clock()
    a = LifecycleScope(LifecycleSupervisor(3, clock=clock))
    a.begin_cleanup()
    b = LifecycleScope(LifecycleSupervisor(10, clock=clock))
    b.begin_cleanup()
    assert _cleanup_scope.get() is None
    with managed_renewal_cleanup(a):
        with managed_renewal_cleanup(b):
            assert _cleanup_scope.get() is b
        assert _cleanup_scope.get() is a
    ready = asyncio.Queue()
    resume = asyncio.Event()

    async def worker(scope):
        try:
            with managed_renewal_cleanup(scope):
                await ready.put(scope)
                await resume.wait()
                assert _cleanup_scope.get() is scope
                return _cleanup_allowed()
        finally:
            assert _cleanup_scope.get() is None

    tasks = [asyncio.create_task(worker(a)), asyncio.create_task(worker(b))]
    await ready.get()
    await ready.get()
    clock.advance(3)
    resume.set()
    assert await asyncio.gather(*tasks) == [False, True]
    child_resume = asyncio.Event()

    async def child():
        await child_resume.wait()
        return _cleanup_scope.get()

    with managed_renewal_cleanup(b):
        task = asyncio.create_task(child())
    assert _cleanup_scope.get() is None
    child_resume.set()
    assert await task is b
    ready = asyncio.Queue()
    resume = asyncio.Event()
    task = asyncio.create_task(worker(b))
    await ready.get()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert _cleanup_scope.get() is None and _cleanup_allowed()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["unknown", "missing_generation", "false"])
async def test_k07_provenance_and_backend_false_through_real_facade(kind):
    from types import SimpleNamespace
    from uuid import uuid4

    from app.redis.protection_facade import (
        ConcurrencyHandle,
        ProtectedAcquireResult,
        RecoveryConcurrencySemaphore,
    )
    from app.redis.recovery import Backend

    w = await world()
    calls = []

    class BackendProbe:
        async def renew_guarded(self, policies, lease, *, guard):
            calls.append(lease)
            op = GuardedRenewalOperation()
            op.begin(op.authorize(guard))
            op.resolve(Status.RESULT, result=RenewResult(False))
            return op.finish(Disposition.BACKEND_RESULT)

    facade = RecoveryConcurrencySemaphore(
        BackendProbe(),
        SimpleNamespace(
            owner_id=w.handle.owner_id,
            local_generation=lambda generation: None,
        ),
    )
    handle = (
        w.handle
        if kind == "false"
        else (
            uuid4()
            if kind == "unknown"
            else ConcurrencyHandle(uuid4(), Backend.LOCAL, w.handle.owner_id, 7)
        )
    )

    async def acquire(policies):
        return ProtectedAcquireResult(True, handle)

    w.concurrency.acquire = acquire
    w.concurrency.renew_guarded = facade.renew_guarded
    if kind != "false":
        w.concurrency.release = facade.release
    await start_blocked(w)
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await asyncio.wait_for(w.runner, 3)
    assert (
        result.end == End.OWNERSHIP_LOST
        and result.error.code == "gateway_internal_error"
    )
    assert w.lifecycle._confirmed_anchor == 0
    assert calls == ([w.handle.raw_lease_id] if kind == "false" else [])
    if kind != "false":
        assert not w.lifecycle.release_result.released


@pytest.mark.asyncio
@pytest.mark.parametrize("locked", [False, True])
async def test_k18_lifecycle_uses_retired_local_generation_after_cutover(locked):
    from uuid import uuid4

    from app.normal_api.lifecycle import Policies
    from app.persistence.models.enums import RateScopeType
    from app.redis.concurrency import ResolvedConcurrencyPolicy
    from app.redis.local_degraded import LocalDegradedProtection
    from app.redis.protection_facade import RecoveryConcurrencySemaphore
    from app.redis.recovery import Backend, RecoveryCoordinator, TrafficMode

    w = await world()
    local = LocalDegradedProtection(lease_duration_ms=6000, clock=w.clock.now)
    coordinator = RecoveryCoordinator(
        None,
        None,
        None,
        None,
        initial_local=local,
        clock=w.clock.now,
        lease_duration_ms=6000,
    )

    class NoRedis:
        async def acquire(self, *args):
            raise AssertionError("Redis acquire")

        async def renew_guarded(self, *args, **kwargs):
            raise AssertionError("Redis renewal")

        async def release(self, *args):
            raise AssertionError("Redis release")

    facade = RecoveryConcurrencySemaphore(NoRedis(), coordinator)
    coordinator.mark_unreachable()
    generation = coordinator.active_generation_id
    w.concurrency = facade
    w.lifecycle.concurrency = facade
    policy = ResolvedConcurrencyPolicy(
        uuid4(),
        w.auth.identity.tenant_id,
        RateScopeType.SERVICE,
        w.rows["services"][0]["id"],
        1,
    )

    async def policies(resolved):
        return Policies((), (policy,))

    await start_blocked(w, policies=policies)
    issued = w.lifecycle.handle
    assert issued.backend is Backend.LOCAL and issued.local_generation_id == generation
    # Controlled cutover fixture changes only new-admission membership. Completion
    # still exercises the production facade and issuing local store.
    coordinator._retired[generation] = local
    coordinator._active = coordinator._active_id = None
    coordinator._set_mode(TrafficMode.NORMAL)
    before = tuple(
        (tuple(item.owners.items()), item.touched_ms)
        for item in local.concurrency._entries.values()
    )
    if locked:
        await local.concurrency._lock.acquire()
    try:
        await w.clock.expect(2)
        w.clock.advance(2)
        if locked:
            await w.clock.expect(4)
            w.clock.advance(4)
            await asyncio.wait_for(w.lifecycle._terminal_event.wait(), 3)
            local.concurrency._lock.release()
            result = await asyncio.wait_for(w.runner, 3)
            assert result.end == End.OWNERSHIP_UNCERTAIN
            assert w.lifecycle._confirmed_anchor == 0
        else:
            await w.clock.expect(4)
            await w.clock.expect(
                4
            )  # Distinguish old timer from newly scheduled due time.
            assert w.lifecycle._confirmed_anchor == 2
            result = await finish_blocked(w)
            assert result.end == End.DELIVERED
        assert w.lifecycle.release_result.released
        assert w.lifecycle.handle is issued
        assert all(not item.owners for item in local.concurrency._entries.values())
        if locked:
            operation = w.lifecycle.last_renewal_evidence
            assert operation.attempts == ()
            assert before[0][1] == 0
    finally:
        if local.concurrency._lock.locked():
            local.concurrency._lock.release()
        w.finish.set()
        w.runner.cancel()
        await asyncio.gather(w.runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_k21_fault_redaction_and_repeated_terminal_callbacks(caplog):
    import logging

    w = await world()
    secret = "fixture-private-token-and-exception"

    async def guarded(*args, **kwargs):
        raise RuntimeError(secret)

    w.concurrency.renew_guarded = guarded
    with caplog.at_level(logging.DEBUG):
        await start_blocked(w)
        await w.clock.expect(2)
        w.clock.advance(2)
        result = await asyncio.wait_for(w.runner, 3)
        assert not w.lifecycle.cancel(End.OWNERSHIP_LOST)
        assert not w.lifecycle.cancel(End.DISCONNECT)
    assert result.end == End.INTERNAL
    assert result.error.code == "gateway_internal_error"
    assert secret not in caplog.text and secret not in str(result.error)
    assert str(w.handle.raw_lease_id) not in caplog.text
    assert w.concurrency.released == [w.handle]
    assert "circuit" not in w.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "later",
    [Disposition.SUPPRESSED, Disposition.GUARD_ERROR, Disposition.INTERNAL_ERROR],
)
async def test_k12_prior_uncertainty_survives_later_denial_or_fault(later):
    w = await world()
    snapshots = []

    async def guarded(*args, guard, **kwargs):
        operation = GuardedRenewalOperation()
        if not snapshots:
            operation.begin(operation.authorize(guard))
            outcome = operation.finish(Disposition.UNKNOWN)
        else:
            outcome = operation.finish(later)
        snapshots.append(outcome)
        return outcome

    w.concurrency.renew_guarded = guarded
    await start_blocked(w)
    await w.clock.expect(2)
    w.clock.advance(2)
    await w.clock.expect(3)
    w.clock.advance(3)
    result = await asyncio.wait_for(w.runner, 3)
    assert result.end is End.INTERNAL and not result.cleanup_complete
    assert snapshots[0].attempts[-1].status is Status.UNRESOLVED
    assert snapshots[1].disposition is later and snapshots[1].attempts == ()
    assert w.lifecycle.unresolved_renewals[0].attempts == snapshots[0].attempts
    assert w.deadline_scope in w.supervisor.residuals
    assert w.lifecycle._confirmed_anchor == 0 and w.concurrency.released == [w.handle]


@pytest.mark.asyncio
@pytest.mark.parametrize("ownership", ["real", "null", "denied"])
async def test_k01_worker_is_supervised_before_body_only_for_real_acquisition(
    ownership,
):
    w = await world(null=ownership == "null")
    if ownership == "denied":
        w.concurrency.acquired = False
    seen = []

    async def body(resolved, receiver):
        seen.append(True)
        if ownership == "real":
            assert w.lifecycle._renewal is w.deadline_scope.tasks["renewal"]
            assert w.lifecycle.handle is w.handle
        else:
            assert w.lifecycle._renewal is None
        return await w.hooks.body(resolved, receiver)

    result = await run(w, change(w.hooks, body=body))
    assert not w.concurrency.renewed
    assert len(w.concurrency.released) == (ownership == "real")
    assert bool(seen) == (ownership != "denied")
    assert result.end is (End.REJECTED if ownership == "denied" else End.DELIVERED)


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [5000, 120000])
async def test_k02_native_fractional_unknown_schedule_at_both_ttl_bounds(ttl):
    from app.redis.concurrency import ConcurrencyDependencyError

    w = await world(ttl=ttl)
    w.concurrency.renew_results = [
        ConcurrencyDependencyError(),
        ConcurrencyDependencyError(),
    ]
    await start_blocked(w)
    t = ttl / 1000
    await w.clock.expect(t / 3)
    w.clock.advance(t / 3)
    await w.clock.expect(t / 2)
    assert w.lifecycle._confirmed_anchor == 0 and len(w.concurrency.renewed) == 1
    w.concurrency.renew_entered.clear()
    w.clock.advance(t / 2)
    await asyncio.wait_for(w.concurrency.renew_entered.wait(), 3)
    assert len(w.concurrency.renewed) == 2 and w.lifecycle._confirmed_anchor == 0
    await w.clock.expect(2 * t / 3)
    w.clock.advance(2 * t / 3)
    result = await asyncio.wait_for(w.runner, 3)
    assert result.end is End.OWNERSHIP_UNCERTAIN
    assert len(w.concurrency.renewed) == 2 and w.concurrency.released == [w.handle]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["loss", "recovered", "expired", "internal"])
async def test_k21_r8_diagnostics_are_bounded_correlated_and_not_duplicate(
    caplog, mode
):
    import logging

    from app.redis.concurrency import ConcurrencyDependencyError

    w = await world()
    if mode == "loss":
        w.concurrency.renew_results = [RenewResult(False)]
    elif mode == "recovered":
        w.concurrency.renew_results = [ConcurrencyDependencyError(), RenewResult(True)]
    elif mode == "expired":
        w.concurrency.renew_results = [
            ConcurrencyDependencyError(),
            ConcurrencyDependencyError(),
        ]
    else:
        w.concurrency.renew_results = [RuntimeError("fixture-private-error")]
    with caplog.at_level(logging.INFO, logger="app.normal_api.lifecycle"):
        await start_blocked(w)
        await w.clock.expect(2)
        w.clock.advance(2)
        if mode in {"recovered", "expired"}:
            await w.clock.expect(3)
            w.clock.advance(3)
            await w.clock.expect(5 if mode == "recovered" else 4)
            if mode == "recovered":
                w.finish.set()
            else:
                w.clock.advance(4)
        result = await asyncio.wait_for(w.runner, 3)
        w.lifecycle.cancel(End.OWNERSHIP_LOST)
    records = [r for r in caplog.records if r.name == "app.normal_api.lifecycle"]
    expected = {
        "loss": ["ownership_lost"],
        "recovered": ["unknown", "recovered"],
        "expired": ["unknown", "confidence_expired"],
        "internal": ["internal_fault"],
    }[mode]
    assert [r.event for r in records] == [
        "normal_api_renewal_" + event for event in expected
    ]
    for record in records:
        assert record.request_id == str(w.lifecycle.resolved.request_id)
        assert record.plane == "DATA_NORMAL" and record.exc_info is None
    assert str(w.handle.raw_lease_id) not in caplog.text
    assert "fixture-private-error" not in caplog.text
    assert w.concurrency.released == [w.handle]
    assert result.end is (
        End.DELIVERED
        if mode == "recovered"
        else End.OWNERSHIP_LOST
        if mode == "loss"
        else End.OWNERSHIP_UNCERTAIN
        if mode == "expired"
        else End.INTERNAL
    )


@pytest.mark.asyncio
async def test_k21_diagnostic_failure_does_not_change_cleanup_or_selected_result(
    monkeypatch,
):
    import app.normal_api.lifecycle as lifecycle_module

    w = await world()
    w.concurrency.renew_results = [RenewResult(False)]

    def fail(*args, **kwargs):
        raise RuntimeError("fixture-log-failure")

    monkeypatch.setattr(lifecycle_module.logger, "info", fail)
    await start_blocked(w)
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await asyncio.wait_for(w.runner, 3)
    assert result.end is End.OWNERSHIP_LOST
    assert w.concurrency.released == [w.handle] and result.cleanup_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency_first", [False, True])
async def test_k18_redis_issued_lifecycle_never_substitutes_active_local_backend(
    dependency_first,
):
    from redis.exceptions import ConnectionError

    from app.redis.concurrency import ReleaseResult
    from app.redis.local_degraded import LocalDegradedProtection
    from app.redis.protection_facade import (
        ConcurrencyHandle,
        ProtectedAcquireResult,
        RecoveryConcurrencySemaphore,
    )
    from app.redis.recovery import Backend, RecoveryCoordinator
    from tests.unit.test_guarded_renewal import setup

    w = await world()
    primitive, connection, _, _ = setup()
    local = LocalDegradedProtection(lease_duration_ms=6000, clock=w.clock.now)
    coordinator = RecoveryCoordinator(
        None,
        None,
        None,
        None,
        initial_local=local,
        clock=w.clock.now,
        lease_duration_ms=6000,
    )
    handle = ConcurrencyHandle(
        w.handle.raw_lease_id, Backend.REDIS, coordinator.owner_id
    )
    facade = RecoveryConcurrencySemaphore(primitive, coordinator)
    coordinator.mark_unreachable()
    calls = []

    async def acquire(policies):
        return ProtectedAcquireResult(True, handle)

    async def release(policies, lease):
        calls.append(lease)
        return ReleaseResult(True)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Local substitution")

    primitive.release = release
    local.concurrency.renew_guarded = forbidden
    local.concurrency.release = forbidden
    w.concurrency.acquire = acquire
    w.concurrency.renew_guarded = facade.renew_guarded
    w.concurrency.release = facade.release
    _, policies = attach_backend(w, primitive)
    # attach_backend supplies policies only; restore the actual degraded facade.
    w.concurrency.renew_guarded = facade.renew_guarded
    connection.replies = (
        [ConnectionError("fixture private dependency"), 1] if dependency_first else [1]
    )
    await start_blocked(w, policies=policies)
    await w.clock.expect(2)
    w.clock.advance(2)
    if dependency_first:
        await w.clock.expect(3)
        w.clock.advance(3)
        await w.clock.expect(5)
    else:
        await w.clock.expect(4)
        await w.clock.expect(4)
    assert w.lifecycle._confirmed_anchor == (3 if dependency_first else 2)
    result = await finish_blocked(w)
    assert result.end is End.DELIVERED
    assert w.lifecycle.handle is handle and calls == [handle.raw_lease_id]
    assert local.concurrency.entry_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cycles", [1, 8, 64, 256])
async def test_f1_repeated_cycles_retire_completed_tasks_and_bound_history(cycles):
    import gc
    import weakref

    from redis.exceptions import ConnectionError

    from tests.unit.test_guarded_renewal import setup

    w = await world()
    primitive, connection, _, _ = setup()
    facade, policies = attach_backend(w, primitive)
    connection.replies = [
        reply
        for _ in range(cycles)
        for reply in (ConnectionError("synthetic dependency"), 1)
    ]
    references = []
    original = facade.renew_guarded

    async def observe(*args, **kwargs):
        references.append(weakref.ref(asyncio.current_task()))
        return await original(*args, **kwargs)

    w.concurrency.renew_guarded = observe
    await start_blocked(w, stage="deliver", policies=policies)
    retained = []
    first_snapshot = None
    try:
        await w.clock.expect(2)
        anchor = 0
        for cycle in range(cycles):
            w.clock.advance(anchor + 2)
            await w.clock.expect(anchor + 3)
            w.clock.advance(anchor + 3)
            await w.clock.expect(anchor + 5)
            anchor += 3
            retained.append(
                sum(task.done() for task in w.lifecycle._renewal_obligations)
            )
            snapshot = w.lifecycle.renewal_evidence
            if first_snapshot is None:
                first_snapshot = snapshot
            assert snapshot.uncertain_invocations == cycle + 1
            assert snapshot.live_invocations == snapshot.physical_obligations == 0
            assert len(snapshot.recent) <= 2
            assert first_snapshot.uncertain_invocations == 1
            assert not w.lifecycle.cleanup_complete
            assert w.lifecycle._confirmed_anchor == anchor
            assert w.lifecycle.end is None and w.lifecycle.committed
        assert max(retained) == 0, retained
        summary = w.lifecycle.renewal_evidence
        assert summary.completed_invocations == 2 * cycles
        assert (
            summary.uncertain_invocations == cycles
            and summary.unknown_invocations == cycles
        )
        assert len(summary.cancellation_phase_counts) == 6
        assert all(count == 0 for _, count in summary.cancellation_phase_counts)
        assert summary.unresolved and len(summary.recent) <= 2
        assert summary.live_invocations == summary.physical_obligations == 0
        assert summary.issuing_backend == "REDIS" and summary.issuing_generation is None
        assert dict(summary.attempt_counts)[Status.UNRESOLVED] == cycles
        assert dict(summary.attempt_counts)[Status.RESULT] == cycles
        # Drain already-scheduled observation callbacks without a timing sleep.
        acknowledged = asyncio.Event()
        asyncio.get_running_loop().call_soon(acknowledged.set)
        await acknowledged.wait()
        gc.collect()
        assert all(ref() is None for ref in references)
        result = await finish_blocked(w)
        assert result.end is End.DELIVERED and not result.cleanup_complete
        assert w.lifecycle.outstanding_tasks == {}
        assert w.deadline_scope in w.supervisor.residuals
        assert w.concurrency.released == [w.handle]
    finally:
        w.finish.set()
        w.runner.cancel()
        await asyncio.gather(w.runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)


@pytest.mark.asyncio
async def test_f1_live_cancelled_work_and_physical_obligations_are_independent():
    w = await world()
    entered, cancellation, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Resource:
        confirmed = False

        def closure_confirmed(self, connection):
            return self.confirmed

    class Connection:
        pass

    resource = Resource()
    connection = Connection()
    connection._managed_pool_owner = resource

    async def invocation():
        record = GuardedRenewalOperation()
        record.begin(2)
        record.deferred_connections.add(connection)
        entered.set()
        try:
            await finish.wait()
        except asyncio.CancelledError:
            cancellation.set()
            await finish.wait()
        return record.finish(Disposition.UNKNOWN)

    task = w.lifecycle._own("renew_operation", invocation())
    w.lifecycle._retain_renewal(task)
    await entered.wait()
    task.cancel()
    await cancellation.wait()
    pending = w.lifecycle.renewal_evidence
    assert pending.live_invocations == 1 and pending.completed_invocations == 0
    assert task in w.lifecycle._renewal_obligations and not task.done()
    finish.set()
    await task
    w.lifecycle._retain_renewal(task)
    first = w.lifecycle.renewal_evidence
    assert first.live_invocations == 0 and first.physical_obligations == 1
    assert first.uncertain_invocations == first.unknown_invocations == 1
    assert w.lifecycle.unresolved_renewals[0].cleanup_pending
    for _ in range(3):
        w.lifecycle._retain_renewal(task)
    assert w.lifecycle.renewal_evidence == first
    resource.confirmed = True
    current = w.lifecycle.renewal_evidence
    assert current.physical_obligations == 0 and current.unresolved
    assert first.physical_obligations == 1 and pending.live_invocations == 1
    assert w.lifecycle.renewal_obligation == "unresolved"
    assert not w.lifecycle.cleanup_complete
    with pytest.raises(FrozenInstanceError):
        first.uncertain_invocations = 0


@pytest.mark.asyncio
async def test_f1_exception_is_observed_once_without_archiving_task():
    w = await world()

    async def invocation():
        record = GuardedRenewalOperation()
        record.begin(2)
        raise RuntimeError("synthetic private failure")

    task = w.lifecycle._own("renew_operation", invocation())
    await asyncio.gather(task, return_exceptions=True)
    w.lifecycle._retain_renewal(task)
    first = w.lifecycle.renewal_evidence
    w.lifecycle._retain_renewal(task)
    assert first == w.lifecycle.renewal_evidence
    assert first.completed_invocations == first.uncertain_invocations == 1
    assert first.live_invocations == 0 and first.unresolved
    assert not w.lifecycle._renewal_obligations
    assert "renew_operation" not in w.lifecycle._owned_tasks
    assert "synthetic private failure" not in repr(first)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_f1_completed_record_truth_survives_exception_or_cancellation(cancelled):
    w = await world()

    async def invocation():
        record = GuardedRenewalOperation()
        record.begin(2)
        record.finish(Disposition.UNKNOWN)
        if cancelled:
            raise asyncio.CancelledError
        raise RuntimeError("synthetic private failure")

    task = w.lifecycle._own("renew_operation", invocation())
    await asyncio.gather(task, return_exceptions=True)
    w.lifecycle._retain_renewal(task)
    summary = w.lifecycle.renewal_evidence
    assert summary.completed_invocations == summary.unknown_invocations == 1
    assert summary.uncertain_invocations == 1 and summary.unresolved
    assert summary.cancelled_invocations == cancelled
    assert summary.failed_invocations == (not cancelled)
    assert dict(summary.disposition_counts)[Disposition.UNKNOWN] == 1
    assert summary.first_uncertain_dispatch == summary.last_uncertain_dispatch == 2
    assert w.lifecycle.last_renewal_evidence.outcome.disposition is Disposition.UNKNOWN
    assert not w.lifecycle._renewal_obligations


@pytest.mark.asyncio
async def test_f2_phase_counters_survive_eviction_success_suppression_and_gc():
    """Accounting-only mixed invocations; K11 proves actual backend phases."""
    import gc
    import weakref

    w = await world()
    references = []

    async def complete(phase, kind, stamp):
        async def invocation():
            operation = GuardedRenewalOperation()
            references.append(weakref.ref(operation))
            if kind != "suppressed" and phase != "preparation":
                operation.begin(stamp)
            operation.phase = phase
            if kind == "cancelled":
                raise asyncio.CancelledError
            if kind == "success":
                operation.resolve(Status.RESULT, result=RenewResult(True))
                return operation.finish(Disposition.BACKEND_RESULT)
            return operation.finish(
                Disposition.UNKNOWN if kind == "unknown" else Disposition.SUPPRESSED
            )

        task = w.lifecycle._own("renew_operation", invocation())
        references.append(weakref.ref(task))
        await asyncio.gather(task, return_exceptions=True)
        w.lifecycle._retain_renewal(task)
        snapshot = w.lifecycle.renewal_evidence
        for _ in range(3):
            w.lifecycle._retain_renewal(task)
            assert w.lifecycle.renewal_evidence == snapshot
        return snapshot

    first = await complete("preparation", "cancelled", 0)
    assert dict(first.cancellation_phase_counts)["preparation"] == 1
    for stamp, phase in enumerate(("drain", "response", "drain", "response"), 1):
        snapshot = await complete(phase, "cancelled", stamp)
        assert len(snapshot.recent) <= 2
    for stamp in range(5, 8):
        snapshot = await complete("response", "unknown", stamp)
    assert len(snapshot.recent) == 2
    assert all(not detail.cancelled for detail in snapshot.recent)
    assert dict(snapshot.cancellation_phase_counts) == {
        "preparation": 1,
        "write": 0,
        "drain": 2,
        "response": 2,
        "local": 0,
        None: 0,
    }
    before = snapshot
    await complete("response", "success", 8)
    after = await complete("preparation", "suppressed", 9)
    assert after.cancellation_phase_counts == before.cancellation_phase_counts
    assert after.cancelled_invocations == 5
    assert after.uncertain_invocations == 7 and after.unknown_invocations == 3
    assert after.completed_invocations == 10 and after.unresolved
    assert dict(first.cancellation_phase_counts)["drain"] == 0
    assert len(after.cancellation_phase_counts) == 6 and len(after.recent) == 2
    assert after.live_invocations == after.physical_obligations == 0
    assert w.lifecycle.renewal_obligation == "unresolved"
    acknowledged = asyncio.Event()
    asyncio.get_running_loop().call_soon(acknowledged.set)
    await acknowledged.wait()
    gc.collect()
    assert all(reference() is None for reference in references)
    with pytest.raises(FrozenInstanceError):
        before.recent[0].phase = "drain"


@pytest.mark.asyncio
async def test_f2_actual_drain_and_response_snapshots_are_distinguishable(monkeypatch):
    from app.normal_api.lifecycle import RequestLifecycle

    original = RequestLifecycle._retain_renewal
    snapshots = {}

    def observe(owner, task):
        original(owner, task)
        evidence = owner.last_renewal_evidence
        if task.done() and evidence is not None and evidence.cancelled:
            snapshots[evidence.phase] = evidence

    monkeypatch.setattr(RequestLifecycle, "_retain_renewal", observe)
    await test_k11_lifecycle_cancellation_preserves_actual_operation_history("drain")
    await test_k11_lifecycle_cancellation_preserves_actual_operation_history("response")
    assert snapshots["drain"] != snapshots["response"]
    assert snapshots["drain"].attempts == snapshots["response"].attempts


@pytest.mark.parametrize("phase", [None, "synthetic private field", 1])
def test_f2_missing_or_unsupported_phase_never_fabricates_a_production_phase(phase):
    from types import SimpleNamespace

    from app.normal_api.lifecycle import RequestLifecycle

    assert RequestLifecycle._renewal_snapshot(None).phase is None
    assert (
        RequestLifecycle._renewal_snapshot(SimpleNamespace(phase=phase)).phase is None
    )


@pytest.mark.asyncio
async def test_f2_cancellation_without_an_operation_counts_absence_once():
    w = await world()

    async def invocation():
        raise asyncio.CancelledError

    task = w.lifecycle._own("renew_operation", invocation())
    await asyncio.gather(task, return_exceptions=True)
    w.lifecycle._retain_renewal(task)
    snapshot = w.lifecycle.renewal_evidence
    assert w.lifecycle.last_renewal_evidence.phase is None
    assert dict(snapshot.cancellation_phase_counts)[None] == 1
    assert snapshot.cancelled_invocations == 1 and snapshot.uncertain_invocations == 0
    w.lifecycle._retain_renewal(task)
    assert w.lifecycle.renewal_evidence == snapshot
