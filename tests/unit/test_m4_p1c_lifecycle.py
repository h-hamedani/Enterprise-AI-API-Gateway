import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.core.errors import GatewayHttpError
from app.core.request_context import new_request_id
from app.normal_api.lifecycle import (
    CircuitClass,
    End,
    Hooks,
    Policies,
    RequestLifecycle,
    TimeoutKind,
    UpstreamOutcome,
)
from app.normal_api.supervisor import LifecycleSupervisor
from app.redis.circuit import CircuitState
from app.redis.concurrency import ConcurrencyDependencyError, ReleaseResult, RenewResult
from app.redis.protection_facade import (
    ConcurrencyHandle,
    ProtectedAcquireResult,
    ProtectedEligibilityResult,
)
from app.redis.rate_limit import RateLimitResult
from app.redis.recovery import Backend
from tests.unit.test_m4_p1b_resolution import foundation, scope


class Clock:
    def __init__(self):
        self.value = 0.0
        self.waiters = []
        self.scheduled = asyncio.Queue()

    def now(self):
        return self.value

    async def until(self, deadline):
        if deadline <= self.value:
            return
        event = asyncio.Event()
        self.waiters.append((deadline, event))
        await self.scheduled.put(deadline)
        await event.wait()

    def advance(self, value):
        self.value = value
        for deadline, event in self.waiters:
            if deadline <= value:
                event.set()

    async def expect(self, value):
        while True:
            if await asyncio.wait_for(self.scheduled.get(), 3) == pytest.approx(value):
                return


async def world(*, ttl=6000, null=False):
    resolver, registry, current, rows, auth, reconcile = await foundation()
    calls = []
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    deadline_scope = supervisor.register()
    handle = ConcurrencyHandle(uuid4(), Backend.REDIS, uuid4())

    class Rate:
        allowed = True

        async def evaluate(self, policies):
            calls.append("rate")
            return RateLimitResult(self.allowed, 1501)

    class Concurrency:
        acquired = True
        release_error = False

        def __init__(self):
            self.renew_results = []
            self.released = []
            self.renewed = []
            self.renew_entered = asyncio.Event()

        async def acquire(self, policies):
            calls.append("acquire")
            return ProtectedAcquireResult(
                self.acquired, handle if self.acquired and not null else None
            )

        async def renew(self, policies, issued):
            calls.append("renew")
            self.renewed.append(issued)
            self.renew_entered.set()
            result = (
                self.renew_results.pop(0) if self.renew_results else RenewResult(True)
            )
            if isinstance(result, Exception):
                raise result
            if callable(result):
                return await result()
            return result

        async def renew_guarded(self, policies, issued, *, guard):
            from app.redis.guarded_renewal import (
                GuardedRenewalOperation,
                RenewalAttemptStatus,
                RenewalDisposition,
            )

            operation = GuardedRenewalOperation()
            stamp = operation.authorize(guard)
            if stamp is None:
                return operation.finish(RenewalDisposition.SUPPRESSED)
            operation.begin(stamp)
            try:
                result = await self.renew(policies, issued)
            except ConcurrencyDependencyError:
                return operation.finish(RenewalDisposition.UNKNOWN)
            operation.resolve(RenewalAttemptStatus.RESULT, result=result)
            return operation.finish(RenewalDisposition.BACKEND_RESULT)

        async def release(self, policies, issued):
            calls.append("release")
            self.released.append(issued)
            if self.release_error:
                raise ConcurrencyDependencyError()
            return ReleaseResult(True)

    class Circuit:
        allowed = True
        state = CircuitState.HALF_OPEN
        token = object()

        def __init__(self):
            self.completed = []

        async def check_or_claim_eligibility(self, identity):
            calls.append("circuit")
            return ProtectedEligibilityResult(self.allowed, self.state, self.token)

        async def record_success(self, identity, token):
            calls.append("success")
            assert token is self.token
            self.completed.append(CircuitClass.SUCCESS)
            return "stale-result-preserved"

        async def record_failure(self, identity, token):
            calls.append("failure")
            assert token is self.token
            self.completed.append(CircuitClass.FAILURE)
            return "failure-result-preserved"

    rate, concurrency, circuit = Rate(), Concurrency(), Circuit()
    lifecycle = RequestLifecycle(
        resolver,
        rate,
        concurrency,
        circuit,
        lease_duration_ms=ttl,
        dependency_timeout_seconds=1,
        clock=clock,
    )
    receive_calls = []
    never = asyncio.Event()

    async def receive():
        receive_calls.append("receive")
        if len(receive_calls) == 1:
            return {"type": "http.request", "body": b"", "more_body": False}
        await never.wait()
        return {"type": "http.disconnect"}

    async def policies(resolved):
        calls.append("policies")
        return Policies((), ())

    async def body(resolved, owner):
        calls.append("body")
        await owner.next_body_message()
        return b""

    async def prepare(resolved, data):
        calls.append("prepare")
        return data

    async def execute(owner, prepared):
        calls.append("execute")
        assert owner.observe_upstream(UpstreamOutcome.complete_http(200))

    async def deliver(owner):
        calls.append("deliver")
        owner.committed_downstream()

    hooks = Hooks(policies, body, prepare, execute, deliver)
    return SimpleNamespace(**locals())


async def run(w, hooks=None):
    return await w.lifecycle.run(
        scope(),
        request_id=new_request_id(),
        receive=w.receive,
        hooks=hooks or w.hooks,
        lifecycle_scope=w.deadline_scope,
    )


def change(hooks, **values):
    from dataclasses import replace

    return replace(hooks, **values)


@pytest.mark.asyncio
async def test_stage_order_snapshot_identity_and_single_use():
    w = await world()
    result = await run(w)
    assert result.end == End.DELIVERED and result.committed
    assert w.calls == [
        "policies",
        "rate",
        "acquire",
        "body",
        "prepare",
        "circuit",
        "execute",
        "deliver",
        "success",
        "release",
    ]
    assert (
        w.lifecycle.resolved.snapshot.projection
        is (await w.current.capture(w.auth.identity.tenant_id)).projection
    )
    assert w.concurrency.released == [w.handle]
    assert w.lifecycle.completion_result == "stale-result-preserved"
    assert all(
        task.done()
        for task in (w.lifecycle._work, w.lifecycle._renewal, w.lifecycle.receiver.task)
    )
    with pytest.raises(RuntimeError):
        await run(w)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "denial", ["auth", "rate", "concurrency", "body", "prepare", "circuit"]
)
async def test_rejection_isolates_all_later_stages(denial):
    w = await world()
    hooks = w.hooks
    expected = 429
    if denial == "auth":
        w.auth.fail = True
        expected = 401
    elif denial == "rate":
        w.rate.allowed = False
    elif denial == "concurrency":
        w.concurrency.acquired = False
    elif denial in {"body", "prepare"}:
        expected = 413 if denial == "body" else 500

        async def reject(*args):
            raise GatewayHttpError(
                expected,
                "request_too_large" if expected == 413 else "gateway_internal_error",
                "Safe failure.",
            )

        hooks = change(hooks, **{denial: reject})
    else:
        w.circuit.allowed = False
        expected = 503
    result = await run(w, hooks)
    assert result.error.status_code == expected
    assert "execute" not in w.calls and not w.circuit.completed
    if denial in {"auth", "rate"}:
        assert "acquire" not in w.calls
    if denial != "circuit":
        assert "circuit" not in w.calls
    assert len(w.concurrency.released) == (denial in {"body", "prepare", "circuit"})
    if denial in {"auth", "rate", "concurrency"}:
        assert not w.receive_calls
    if denial == "rate":
        assert result.error.headers == {"Retry-After": "2"}
    if denial == "concurrency":
        assert result.error.headers is None


@pytest.mark.asyncio
async def test_null_handle_creates_no_renewal_or_release():
    w = await world(null=True)
    await run(w)
    assert w.lifecycle._renewal is None
    assert not w.concurrency.released and not w.concurrency.renewed


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["body", "prepare", "execute", "deliver"])
@pytest.mark.parametrize("reason", [End.DISCONNECT, End.SHUTDOWN, End.CANCELLED])
async def test_controlled_cancellation_in_each_stage(stage, reason):
    w = await world()
    entered, closed = asyncio.Event(), asyncio.Event()

    async def suspend(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    task = asyncio.create_task(run(w, change(w.hooks, **{stage: suspend})))
    await asyncio.wait_for(entered.wait(), 3)
    assert w.lifecycle.cancel(reason)
    assert not w.lifecycle.cancel(End.OWNERSHIP_LOST)
    result = await asyncio.wait_for(task, 3)
    assert result.end == reason and result.error is None
    assert closed.is_set() and w.concurrency.released == [w.handle]
    assert w.circuit.completed == ([CircuitClass.SUCCESS] if stage == "deliver" else [])
    assert not w.lifecycle.observe_upstream(UpstreamOutcome.complete_http(500))
    assert w.lifecycle._renewal.done()


@pytest.mark.asyncio
async def test_external_task_cancel_still_joins_and_releases():
    w = await world()
    entered = asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert w.concurrency.released == [w.handle]
    assert w.lifecycle._renewal.done() and w.lifecycle._work.done()


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [5000, 120000])
async def test_exact_renewal_schedule_and_loss_cancels_work(ttl):
    w = await world(ttl=ttl)
    w.concurrency.renew_results = [RenewResult(False)]
    entered = asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    await w.clock.expect(ttl / 3000)
    w.clock.advance(ttl / 3000)
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.OWNERSHIP_LOST and result.error.status_code == 500
    assert w.concurrency.renewed == [w.handle]
    assert w.concurrency.released == [w.handle]
    assert not w.circuit.completed


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_true", [True, False])
async def test_unknown_one_retry_and_original_safety_horizon(retry_true):
    w = await world()
    w.concurrency.renew_results = [
        ConcurrencyDependencyError(),
        RenewResult(True) if retry_true else ConcurrencyDependencyError(),
    ]
    entered, finish = asyncio.Event(), asyncio.Event()

    async def execute(owner, prepared):
        entered.set()
        await finish.wait()
        owner.observe_upstream(UpstreamOutcome.complete_http(200))

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    await w.clock.expect(2)
    w.clock.advance(2)
    await w.clock.expect(3)
    assert w.lifecycle._confirmed_anchor == 0
    assert w.lifecycle._confidence_deadline == 4
    assert not task.done()
    w.clock.advance(3)
    await w.clock.expect(5 if retry_true else 4)
    if retry_true:
        assert w.lifecycle._confirmed_anchor == 3
        assert w.lifecycle._confidence_deadline == 7
        w.clock.advance(
            4.1
        )  # Only the confirmed retry authorizes this old-horizon crossing.
        finish.set()
    else:
        w.clock.advance(4)
    result = await asyncio.wait_for(task, 3)
    assert result.end == (End.DELIVERED if retry_true else End.OWNERSHIP_UNCERTAIN)
    assert len(w.concurrency.renewed) == 2
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_hanging_renewal_cancelled_and_awaited_before_release():
    w = await world()
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def renew():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    w.concurrency.renew_results = [renew]

    async def execute(*args):
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await w.clock.expect(2)
    w.clock.advance(2)
    await asyncio.wait_for(entered.wait(), 3)
    w.clock.advance(4)
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert stopped.is_set() and w.concurrency.released == [w.handle]
    assert w.calls.index("renew") < w.calls.index("release")


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["outcome", "timeout", "disconnect"])
async def test_first_gate_wins_without_rewriting_circuit(first):
    w = await world()

    async def execute(owner, prepared):
        if first == "outcome":
            assert owner.observe_upstream(UpstreamOutcome.complete_http(500))
            assert not owner.observe_timeouts(TimeoutKind.READ)
            owner.cancel(End.DISCONNECT)
        elif first == "timeout":
            assert owner.observe_timeouts(TimeoutKind.PRE_RESPONSE, TimeoutKind.READ)
            assert not owner.observe_upstream(UpstreamOutcome.complete_http(200))
        else:
            owner.cancel(End.DISCONNECT)
            assert not owner.observe_timeouts(TimeoutKind.READ)

    result = await run(w, change(w.hooks, execute=execute))
    assert w.circuit.completed == (
        [] if first == "disconnect" else [CircuitClass.FAILURE]
    )
    if first == "timeout":
        assert result.error.status_code == 504
    else:
        assert result.error is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", list(TimeoutKind))
async def test_timeout_classification_boundary(kind):
    w = await world()

    async def execute(owner, prepared):
        owner.observe_timeouts(kind)

    result = await run(w, change(w.hooks, execute=execute))
    assert result.end == End.TIMEOUT and result.error.status_code == 504
    assert bool(w.circuit.completed) == (
        kind in {TimeoutKind.CONNECT, TimeoutKind.WRITE, TimeoutKind.READ}
    )


@pytest.mark.asyncio
async def test_post_commit_loss_aborts_delivery_without_second_envelope():
    w = await world()
    entered = asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        assert not owner.observe_timeouts(TimeoutKind.PRE_RESPONSE)
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await asyncio.wait_for(entered.wait(), 3)
    w.lifecycle.cancel(End.OWNERSHIP_LOST)
    result = await task
    assert result.committed and result.error is None
    assert w.circuit.completed == [CircuitClass.SUCCESS]
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_receive_has_single_owner_and_disconnect_cancels_before_circuit():
    w = await world()
    receive_tasks = set()

    async def receive():
        receive_tasks.add(asyncio.current_task())
        return {"type": "http.disconnect"}

    w.receive = receive
    result = await run(w)
    assert result.end == End.DISCONNECT
    assert len(receive_tasks) == 1 and "circuit" not in w.calls
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_pre_admission_disconnect_does_not_read_or_admit():
    w = await world()
    w.lifecycle.cancel(End.DISCONNECT)
    result = await run(w)
    assert result.end == End.DISCONNECT
    assert not w.calls and not w.receive_calls


@pytest.mark.asyncio
async def test_cleanup_error_preserves_result_no_second_attempt():
    w = await world()
    w.concurrency.release_error = True
    result = await run(w)
    assert result.end == End.DELIVERED
    assert w.lifecycle.cleanup_failures == {"release"}
    assert w.lifecycle.release_obligation == "attempted_unconfirmed"
    assert w.concurrency.released == [w.handle]
    assert w.circuit.completed == [CircuitClass.SUCCESS]


@pytest.mark.parametrize(
    "status", [200, 301, 404, 408, 429, 500, 501, 502, 503, 504, 505, 599]
)
def test_frozen_status_classifier(status):
    assert UpstreamOutcome.complete_http(status).classification == (
        CircuitClass.FAILURE if status in {500, 502, 503, 504} else CircuitClass.SUCCESS
    )


@pytest.mark.asyncio
async def test_multiple_renewals_continue_during_downstream_delivery():
    w = await world()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def deliver(owner):
        owner.committed_downstream()
        entered.set()
        await finish.wait()

    task = asyncio.create_task(run(w, change(w.hooks, deliver=deliver)))
    await asyncio.wait_for(entered.wait(), 3)
    await w.clock.expect(2)
    w.clock.advance(2)
    # The first 4 is the in-flight operation's safety timer; the second is
    # the next normal due time, scheduled only after confirmation is observed.
    await w.clock.expect(4)
    await w.clock.expect(4)
    assert w.circuit.completed == [CircuitClass.SUCCESS]
    assert not w.concurrency.released
    w.clock.advance(4)
    await w.clock.expect(6)
    finish.set()
    assert (await task).end == End.DELIVERED
    assert w.concurrency.renewed == [w.handle, w.handle]
    assert w.concurrency.released == [w.handle]
    assert w.lifecycle._renewal.done()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cancel", "error"])
async def test_dead_renewal_worker_is_terminal(fault):
    w = await world()
    entered = asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    if fault == "cancel":
        w.lifecycle._renewal.cancel()
    else:
        w.concurrency.renew_results = [ValueError("not a dependency error")]
        await w.clock.expect(2)
        w.clock.advance(2)
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.INTERNAL and result.error.status_code == 500
    assert not w.circuit.completed and w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_late_acquire_keeps_handle_only_for_cleanup():
    w = await world()
    original = w.concurrency.acquire

    async def late(policies):
        result = await original(policies)
        w.clock.advance(4)
        return result

    w.concurrency.acquire = late
    result = await run(w)
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert "body" not in w.calls and "circuit" not in w.calls
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_cancel_after_half_open_claim_before_dispatch_no_completion():
    w = await world()
    original = w.circuit.check_or_claim_eligibility

    async def admit(identity):
        result = await original(identity)
        w.lifecycle.cancel(End.DISCONNECT)
        return result

    w.circuit.check_or_claim_eligibility = admit
    result = await run(w)
    assert result.end == End.DISCONNECT
    assert not w.lifecycle.dispatched and "execute" not in w.calls
    assert not w.circuit.completed
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_receive_failure_is_internal_not_empty_body():
    w = await world()

    async def broken():
        raise OSError("sensitive receive error")

    w.receive = broken
    result = await run(w)
    assert result.end == End.INTERNAL and result.error.status_code == 500
    assert "sensitive" not in result.error.message
    assert "circuit" not in w.calls and w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_duplicate_outcome_and_delivery_callbacks_do_not_duplicate_cleanup():
    w = await world()

    async def execute(owner, prepared):
        assert owner.observe_upstream(UpstreamOutcome.complete_http(429))
        assert not owner.observe_upstream(UpstreamOutcome.complete_http(500))

    async def deliver(owner):
        assert owner.committed_downstream()
        assert not owner.committed_downstream()

    result = await run(w, change(w.hooks, execute=execute, deliver=deliver))
    assert result.end == End.DELIVERED and result.upstream.status == 429
    assert w.circuit.completed == [CircuitClass.SUCCESS]
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_cancellation_during_release_does_not_repeat_release():
    w = await world()
    entered, finish = asyncio.Event(), asyncio.Event()
    original = w.concurrency.release

    async def release(*args):
        entered.set()
        await finish.wait()
        await original(*args)

    w.concurrency.release = release
    task = asyncio.create_task(run(w))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert w.concurrency.released == [w.handle]
    assert w.lifecycle._renewal.done()


@pytest.mark.asyncio
async def test_completion_fault_does_not_rewrite_http_result():
    w = await world()

    async def fail(*args):
        raise RuntimeError("private circuit failure")

    w.circuit.record_success = fail
    result = await run(w)
    assert result.end == End.DELIVERED
    assert w.lifecycle.cleanup_failures == {"circuit_completion"}
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["body", "prepare"])
async def test_lease_loss_in_local_stage_prevents_half_open_claim(stage):
    w = await world()
    w.concurrency.renew_results = [RenewResult(False)]
    entered = asyncio.Event()

    async def blocked(*args):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, **{stage: blocked})))
    await asyncio.wait_for(entered.wait(), 3)
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.OWNERSHIP_LOST
    assert "circuit" not in w.calls and not w.lifecycle.dispatched
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_delayed_true_renewal_cannot_extend_old_safety_horizon():
    w = await world()

    async def late():
        w.clock.advance(4)
        return RenewResult(True)

    w.concurrency.renew_results = [late]

    async def blocked(*args):
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=blocked)))
    await w.clock.expect(2)
    w.clock.advance(2)
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert len(w.concurrency.renewed) == 1 and w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_clock_regression_is_internal_not_backend_loss():
    w = await world()
    original = w.concurrency.acquire

    async def acquire(policies):
        result = await original(policies)
        # Corrupt R8's anchor tracking, not the supervisor's time source.
        w.lifecycle._last_time = 1
        return result

    w.concurrency.acquire = acquire
    result = await run(w)
    assert result.end == End.INTERNAL and result.error.status_code == 500
    assert w.concurrency.released == [w.handle] and not w.circuit.completed


@pytest.mark.asyncio
async def test_cancelled_stage_callback_is_not_an_upstream_failure():
    w = await world()

    async def cancelled(*args):
        raise asyncio.CancelledError

    result = await run(w, change(w.hooks, execute=cancelled))
    assert result.end == End.CANCELLED and result.error is None
    assert w.concurrency.released == [w.handle] and not w.circuit.completed


@pytest.mark.asyncio
async def test_false_release_retained_without_retry():
    from app.redis.concurrency import ReleaseResult

    w = await world()
    calls = []

    async def release(policies, handle):
        calls.append(handle)
        return ReleaseResult(False)

    w.concurrency.release = release
    result = await run(w)
    assert result.end == End.DELIVERED
    assert w.lifecycle.release_result.released is False and calls == [w.handle]


@pytest.mark.asyncio
async def test_unexpected_receive_owner_cancellation_fails_closed():
    w = await world()
    entered = asyncio.Event()

    async def receive():
        entered.set()
        await asyncio.Event().wait()

    w.receive = receive
    task = asyncio.create_task(run(w))
    await asyncio.wait_for(entered.wait(), 3)
    w.lifecycle.receiver.task.cancel()
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.INTERNAL and result.error.status_code == 500
    assert w.concurrency.released == [w.handle]
    assert "circuit" not in w.calls


@pytest.mark.asyncio
async def test_policy_scope_order_and_no_foreign_resource_input():
    from app.persistence.models.enums import RateScopeType
    from app.redis.concurrency import ResolvedConcurrencyPolicy

    w = await world()
    resolved = await w.resolver.resolve(scope(), request_id=new_request_id())
    policies = [
        ResolvedConcurrencyPolicy(
            uuid4(), resolved.identity.tenant_id, kind, resource, 1
        )
        for kind, resource in (
            (RateScopeType.SERVICE, resolved.service["id"]),
            (RateScopeType.ROUTE, resolved.route["id"]),
            (RateScopeType.API_KEY, resolved.identity.api_key_id),
        )
    ]
    ordered = Policies((), tuple(policies)).for_resolution(resolved)
    assert [p.scope_type for p in ordered.concurrency] == [
        RateScopeType.API_KEY,
        RateScopeType.ROUTE,
        RateScopeType.SERVICE,
    ]
    with pytest.raises(ValueError):
        Policies(
            (),
            (
                ResolvedConcurrencyPolicy(
                    uuid4(),
                    resolved.identity.tenant_id,
                    RateScopeType.ROUTE,
                    uuid4(),
                    1,
                ),
            ),
        ).for_resolution(resolved)
    with pytest.raises(ValueError):
        Policies((), (policies[0], policies[0])).for_resolution(resolved)


@pytest.mark.asyncio
async def test_f1_preparation_stall_cannot_outlive_confidence_window():
    w = await world()

    async def prepare(resolved, body):
        w.clock.advance(4.1)
        return body

    result = await run(w, change(w.hooks, prepare=prepare))
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert result.error.status_code == 500
    assert "circuit" not in w.calls and "execute" not in w.calls
    assert "deliver" not in w.calls and not w.circuit.completed
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
@pytest.mark.parametrize("definitive", [False, True])
async def test_f1_stalled_executor_finally_does_not_block_release_or_server_cutoff(
    definitive,
):
    w = await world()
    entered, closing, finish, released = (asyncio.Event() for _ in range(4))
    w.lifecycle.shutdown_expired = w.deadline_scope.notification
    original_release = w.concurrency.release

    async def execute(owner, prepared):
        if definitive:
            owner.observe_upstream(UpstreamOutcome.complete_http(200))
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            # Model a request finalizer that cannot finish until its owned
            # resource acknowledges closure; cancellation cannot fake that ack.
            while not finish.is_set():
                try:
                    await finish.wait()
                except asyncio.CancelledError:
                    continue

    async def release(*args):
        await original_release(*args)
        released.set()

    w.concurrency.release = release
    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.wait_for(closing.wait(), 3)
        task.cancel()
        # A deadlock guard, not a timing-based correctness assumption: release
        # must precede opening the executor-finalization gate.
        await asyncio.wait_for(released.wait(), 3)
        assert not w.lifecycle._work.done()
        w.lifecycle.shutdown_expired.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), 3)
        assert not w.lifecycle.cleanup_complete
        assert "execution" in w.lifecycle.outstanding_tasks
        assert w.lifecycle._renewal.done()
        assert w.concurrency.released == [w.handle]
        assert w.circuit.completed == ([CircuitClass.SUCCESS] if definitive else [])
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(w.lifecycle._work, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gate", ["body", "circuit_return", "outcome", "commit", "delivery_end"]
)
async def test_f1_confidence_enforced_at_every_protected_gate(gate):
    w = await world()
    hooks = w.hooks
    if gate == "body":
        original = hooks.body

        async def body(*args):
            result = await original(*args)
            w.clock.advance(4.1)
            return result

        hooks = change(hooks, body=body)
    elif gate == "circuit_return":
        original = w.circuit.check_or_claim_eligibility

        async def admit(*args):
            result = await original(*args)
            w.clock.advance(4.1)
            return result

        w.circuit.check_or_claim_eligibility = admit
    elif gate == "outcome":

        async def execute(owner, prepared):
            w.clock.advance(4.1)
            assert not owner.observe_upstream(UpstreamOutcome.complete_http(200))

        hooks = change(hooks, execute=execute)
    else:

        async def deliver(owner):
            if gate == "delivery_end":
                owner.committed_downstream()
            w.clock.advance(4.1)
            if gate == "commit":
                assert not owner.before_downstream_commit()

        hooks = change(hooks, deliver=deliver)
    result = await run(w, hooks)
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert w.concurrency.released == [w.handle]
    assert w.circuit.completed == (
        [CircuitClass.SUCCESS] if gate in {"commit", "delivery_end"} else []
    )
    if gate in {"body", "circuit_return"}:
        assert not w.lifecycle.dispatched
    if gate == "body":
        assert "circuit" not in w.calls


@pytest.mark.asyncio
async def test_f1_unknown_does_not_extend_pipeline_confidence():
    w = await world()
    w.concurrency.renew_results = [ConcurrencyDependencyError()]
    entered, continue_preparation = asyncio.Event(), asyncio.Event()

    async def prepare(resolved, body):
        entered.set()
        await continue_preparation.wait()
        w.clock.advance(4.1)
        return body

    task = asyncio.create_task(run(w, change(w.hooks, prepare=prepare)))
    await asyncio.wait_for(entered.wait(), 3)
    await w.clock.expect(2)
    w.clock.advance(2)
    await w.clock.expect(3)
    assert w.lifecycle._confidence_deadline == 4
    continue_preparation.set()
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert "circuit" not in w.calls and "execute" not in w.calls
    assert w.concurrency.released == [w.handle]


@pytest.mark.asyncio
async def test_f1_server_cutoff_reports_unstopped_renewal_without_racing_release():
    w = await world()
    entered, closing, finish = (asyncio.Event() for _ in range(3))
    w.lifecycle.shutdown_expired = w.deadline_scope.notification

    async def renew():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            while not finish.is_set():
                try:
                    await finish.wait()
                except asyncio.CancelledError:
                    continue

    w.concurrency.renew_results = [renew]

    async def execute(*args):
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    try:
        await w.clock.expect(2)
        w.clock.advance(2)
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.wait_for(closing.wait(), 3)
        task.cancel()
        w.lifecycle.shutdown_expired.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), 3)
        assert not w.lifecycle.cleanup_complete
        assert "renewal" in w.lifecycle.outstanding_tasks
        assert "renew_operation" in w.lifecycle.outstanding_tasks
        assert "release_not_attempted_shutdown" in w.lifecycle.cleanup_failures
        assert not w.lifecycle.release_attempted
        assert not w.concurrency.released and not w.circuit.completed
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(
            *w.lifecycle.outstanding_tasks.values(), return_exceptions=True
        )
    assert not w.lifecycle.outstanding_tasks
    assert w.lifecycle._confirmed_anchor == 0  # Late result cannot revive execution.


@pytest.mark.asyncio
async def test_f1_expiry_during_release_is_explicitly_incomplete_no_second_attempt():
    w = await world()
    entered, finish = asyncio.Event(), asyncio.Event()
    w.lifecycle.shutdown_expired = w.deadline_scope.notification
    calls = []

    async def release(*args):
        calls.append(args[1])
        entered.set()
        while not finish.is_set():
            try:
                await finish.wait()
            except asyncio.CancelledError:
                continue

    w.concurrency.release = release
    task = asyncio.create_task(run(w))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        w.lifecycle.shutdown_expired.set()
        result = await asyncio.wait_for(asyncio.shield(task), 3)
        assert result.end == End.DELIVERED  # Delivery genuinely won before cutoff.
        assert not result.cleanup_complete and "release" in result.outstanding_roles
        assert calls == [w.handle] and w.circuit.completed == [CircuitClass.SUCCESS]
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(
            *w.lifecycle.outstanding_tasks.values(), return_exceptions=True
        )


@pytest.mark.asyncio
async def test_f1_requires_explicit_server_boundary_no_default_grace():
    w = await world()
    with pytest.raises(TypeError):
        await w.lifecycle.run(
            scope(), request_id=new_request_id(), receive=w.receive, hooks=w.hooks
        )
    assert not w.lifecycle._started


@pytest.mark.asyncio
async def test_f1_cancelled_boundary_waiter_cannot_spin_cleanup_forever():
    w = await world()
    entered = asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    w.lifecycle._shutdown_wait.cancel()
    result = await asyncio.wait_for(task, 3)
    assert result.end == End.CANCELLED
    assert not result.cleanup_complete
    assert "shutdown_signal_unavailable" in w.lifecycle.cleanup_failures
    await asyncio.gather(
        *w.lifecycle.outstanding_tasks.values(), return_exceptions=True
    )


@pytest.mark.asyncio
async def test_f1_commit_ack_after_horizon_preserves_actual_commit_fact():
    w = await world()

    async def deliver(owner):
        assert owner.before_downstream_commit()
        # Model expiry while awaiting the actual successful response-start.
        w.clock.advance(4.1)
        assert not owner.committed_downstream()

    result = await run(w, change(w.hooks, deliver=deliver))
    assert result.end == End.OWNERSHIP_UNCERTAIN
    assert result.committed and result.error is None
    assert w.circuit.completed == [CircuitClass.SUCCESS]
    assert w.concurrency.released == [w.handle]
