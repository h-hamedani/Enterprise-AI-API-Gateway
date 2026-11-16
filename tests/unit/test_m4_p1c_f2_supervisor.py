import asyncio
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.core.request_context import new_request_id
from app.normal_api.lifecycle import CircuitClass, End, UpstreamOutcome
from app.normal_api.supervisor import AdmissionClosed, LifecycleSupervisor
from tests.unit.test_m4_p1b_resolution import scope
from tests.unit.test_m4_p1c_lifecycle import Clock, change, run, world


async def turn():
    acknowledged = asyncio.Event()
    asyncio.get_running_loop().call_soon(acknowledged.set)
    await acknowledged.wait()


@pytest.mark.parametrize("value", [None, True, False, "", "text", 0, -1, "nan", "inf"])
def test_f2_settings_invalid(value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, lifecycle_grace_timeout_seconds=value)


def test_f2_missing_configuration_is_not_a_default(monkeypatch):
    monkeypatch.delenv("LIFECYCLE_GRACE_TIMEOUT_SECONDS", raising=False)
    assert Settings(_env_file=None).lifecycle_grace_timeout_seconds is None
    monkeypatch.setenv("LIFECYCLE_GRACE_TIMEOUT_SECONDS", "30")
    assert Settings(_env_file=None).lifecycle_grace_timeout_seconds == 30


@pytest.mark.asyncio
async def test_f2_missing_grace_cannot_enable_supervisor():
    with pytest.raises(ValueError):
        LifecycleSupervisor(None)


@pytest.mark.asyncio
async def test_f2_notification_set_observed_clear_is_irreversible():
    supervisor = LifecycleSupervisor(30, clock=Clock())
    boundary = supervisor.register()
    boundary.begin_cleanup()
    waiter = asyncio.create_task(boundary.wait_expired())
    boundary.notification.set()
    await asyncio.wait_for(waiter, 3)
    boundary.notification.clear()
    assert boundary.expired and boundary.deadline == 30
    boundary.begin_cleanup()
    assert boundary.expired and boundary.deadline == 30
    child = asyncio.create_task(asyncio.Event().wait())
    assert not await boundary.join(child)
    await asyncio.gather(child, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown_first", [False, True])
async def test_f2_earliest_deadline_and_notifications_do_not_extend(shutdown_first):
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    boundary = supervisor.register()
    if shutdown_first:
        supervisor.begin_shutdown()
    else:
        boundary.begin_cleanup()
    clock.advance(10)
    if shutdown_first:
        boundary.begin_cleanup()
    else:
        supervisor.begin_shutdown()
    assert boundary.deadline == 30
    clock.advance(20)
    boundary.begin_cleanup()
    supervisor.begin_shutdown()
    assert boundary.deadline == 30
    clock.advance(30)
    assert boundary.expired


@pytest.mark.asyncio
@pytest.mark.parametrize("registration_first", [False, True])
async def test_f2_registration_shutdown_linearization(registration_first):
    supervisor = LifecycleSupervisor(30, clock=Clock())
    if registration_first:
        boundary = supervisor.register()
        supervisor.begin_shutdown()
        assert boundary in supervisor.scopes and boundary.deadline == 30
    else:
        supervisor.begin_shutdown()
        with pytest.raises(AdmissionClosed):
            supervisor.register()
        assert not supervisor.scopes
    with pytest.raises(AdmissionClosed):
        supervisor.register()


@pytest.mark.asyncio
async def test_f2_cancel_before_runner_first_step_has_registered_owner():
    w = await world()
    runner = w.lifecycle.run(
        scope(),
        request_id=new_request_id(),
        receive=w.receive,
        hooks=w.hooks,
        lifecycle_scope=w.deadline_scope,
    )
    assert w.deadline_scope in w.supervisor.scopes
    runner.cancel()
    assert w.deadline_scope.request_deadline == 30
    with pytest.raises(asyncio.CancelledError):
        await runner
    await turn()
    assert not w.lifecycle._started and not w.calls
    assert not w.supervisor.scopes and not w.supervisor.residuals


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["success", "cancel", "disconnect", "failure", "committed"]
)
async def test_f2_unset_event_finalizer_cutoff_and_eventual_observation(mode):
    w = await world()
    entered, closing, finish, released = (asyncio.Event() for _ in range(4))
    original_release = w.concurrency.release

    async def release(*args):
        result = await original_release(*args)
        released.set()
        return result

    async def execute(owner, prepared):
        if mode in {"success", "failure", "committed"}:
            owner.observe_upstream(
                UpstreamOutcome.complete_http(500 if mode == "failure" else 200)
            )
        if mode == "committed":
            owner.committed_downstream()
        entered.set()
        if mode not in {"success", "failure"}:
            await asyncio.Event().wait()

    async def deliver(owner):
        owner.committed_downstream()

    async def work_finalizer(*args):
        try:
            await execute(*args)
            if mode in {"success", "failure"}:
                return
        finally:
            # For success the delivery finalizer below supplies the stall.
            if mode not in {"success", "failure"}:
                closing.set()
                while not finish.is_set():
                    try:
                        await finish.wait()
                    except asyncio.CancelledError:
                        pass

    async def delivery_finalizer(owner):
        await deliver(owner)
        owner._finish(End.DELIVERED)
        closing.set()
        while not finish.is_set():
            try:
                await finish.wait()
            except asyncio.CancelledError:
                pass

    w.concurrency.release = release
    hooks = change(w.hooks, execute=work_finalizer, deliver=delivery_finalizer)
    runner = asyncio.create_task(run(w, hooks))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        if mode in {"cancel", "committed"}:
            runner.cancel()
        elif mode == "disconnect":
            w.lifecycle.cancel(End.DISCONNECT)
        await asyncio.wait_for(closing.wait(), 3)
        await asyncio.wait_for(released.wait(), 3)
        original_deadline = w.deadline_scope.deadline
        if mode == "cancel":
            runner.cancel()
        await w.clock.expect(original_deadline)
        assert not w.deadline_scope.notification.is_set()
        w.clock.advance(original_deadline)
        try:
            result = await asyncio.wait_for(asyncio.shield(runner), 3)
        except asyncio.CancelledError:
            assert mode in {"cancel", "committed"}
        else:
            assert not result.cleanup_complete
            if mode in {"success", "failure"}:
                assert result.end == End.DELIVERED
        await turn()
        assert w.deadline_scope in w.supervisor.residuals
        assert w.lifecycle._work in w.deadline_scope.tasks.values()
        assert w.concurrency.released == [w.handle]
        assert w.lifecycle.release_obligation == "confirmed_success"
        assert w.circuit.completed == (
            [CircuitClass.FAILURE]
            if mode == "failure"
            else [CircuitClass.SUCCESS]
            if mode in {"success", "committed"}
            else []
        )
        assert not w.lifecycle.observe_upstream(UpstreamOutcome.complete_http(200))
    finally:
        finish.set()
        await asyncio.gather(runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)
        await turn()
    assert not w.supervisor.scopes and not w.supervisor.residuals


@pytest.mark.asyncio
async def test_f2_shutdown_does_not_cancel_early_or_allow_second_window():
    w = await world()
    entered = asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    runner = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    await asyncio.wait_for(entered.wait(), 3)
    w.supervisor.begin_shutdown()
    await w.clock.expect(30)
    assert not runner.done() and w.lifecycle.end is None
    w.clock.advance(30)
    result = await asyncio.wait_for(runner, 3)
    assert result.end == End.SHUTDOWN
    assert w.deadline_scope.deadline == 30
    assert not w.lifecycle.release_attempted
    assert w.lifecycle.release_obligation == "not_attempted"


@pytest.mark.asyncio
async def test_f2_retained_task_exception_is_observed_after_runner_exit():
    supervisor = LifecycleSupervisor(30, clock=Clock())
    boundary = supervisor.register()
    gate = asyncio.Event()

    async def child():
        await gate.wait()
        raise ValueError("fixture")

    task = boundary.track("child", asyncio.create_task(child()))
    owner = SimpleNamespace(_started=True, release_obligation="none")

    async def finish():
        return None

    await boundary.launch(owner, finish)
    assert boundary in supervisor.residuals
    gate.set()
    await asyncio.gather(task, return_exceptions=True)
    await turn()
    assert not supervisor.residuals
    assert not task._log_traceback


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["renew", "release"])
async def test_f2_absolute_cutoff_retains_dependency_without_overlapping_release(role):
    w = await world()
    entered, closing, finish = (asyncio.Event() for _ in range(3))

    async def blocked(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            while not finish.is_set():
                try:
                    await finish.wait()
                except asyncio.CancelledError:
                    pass

    if role == "renew":
        w.concurrency.renew_results = [blocked]

        async def execute(*args):
            await asyncio.Event().wait()

        hooks = change(w.hooks, execute=execute)
    else:
        w.concurrency.release = blocked
        hooks = w.hooks
    runner = asyncio.create_task(run(w, hooks))
    try:
        if role == "renew":
            await w.clock.expect(2)
            w.clock.advance(2)
        await asyncio.wait_for(entered.wait(), 3)
        if role == "renew":
            runner.cancel()
            await asyncio.wait_for(closing.wait(), 3)
        deadline = w.deadline_scope.deadline
        await w.clock.expect(deadline)
        w.clock.advance(deadline)
        try:
            result = await asyncio.wait_for(asyncio.shield(runner), 3)
        except asyncio.CancelledError:
            assert role == "renew"
        else:
            assert not result.cleanup_complete and result.committed
        assert w.lifecycle.release_obligation == (
            "not_attempted" if role == "renew" else "attempted_unconfirmed"
        )
        if role == "renew":
            assert not w.concurrency.released and not w.lifecycle.release_attempted
        else:
            assert w.lifecycle.release_attempted
        assert w.deadline_scope in w.supervisor.residuals
    finally:
        finish.set()
        await asyncio.gather(runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)
        await turn()


@pytest.mark.asyncio
async def test_f2_server_adapter_activates_before_uvicorn_drain(monkeypatch):
    import signal

    import uvicorn

    from app.server import LifecycleServer

    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    server = LifecycleServer(uvicorn.Config("app.main:app"), supervisor)
    calls = []

    async def drain(self, sockets=None):
        assert supervisor.process_deadline == 30
        with pytest.raises(AdmissionClosed):
            supervisor.register()
        calls.append("drain")

    monkeypatch.setattr(uvicorn.Server, "shutdown", drain)
    server.handle_exit(signal.SIGTERM, None)
    await turn()
    clock.advance(4)
    await server.shutdown()
    assert calls == ["drain"] and supervisor.process_deadline == 30


@pytest.mark.asyncio
async def test_f2_lifespan_resource_wait_has_no_second_grace_window():
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    supervisor.begin_shutdown()
    clock.advance(20)
    entered = asyncio.Event()
    calls = []

    async def close():
        entered.set()
        await asyncio.Event().wait()

    async def later():
        calls.append("late")

    runner = asyncio.create_task(
        supervisor.close_resources((("first", close), ("second", later)))
    )
    await entered.wait()
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(runner, 3)
    assert supervisor.process_deadline == 30 and not calls
    assert supervisor.unclosed_resources == {"first", "second"}
    await asyncio.gather(*supervisor.resources.values(), return_exceptions=True)


@pytest.mark.asyncio
async def test_f2_registered_scope_cannot_escape_drain_before_first_launch():
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    boundary = supervisor.register()
    draining = asyncio.create_task(supervisor.drain())
    await clock.expect(30)
    assert not draining.done() and boundary in supervisor.scopes
    clock.advance(30)
    await asyncio.wait_for(draining, 3)
    owner = SimpleNamespace(_started=False, release_obligation="none")
    with pytest.raises(AdmissionClosed):
        boundary.launch(owner, lambda: asyncio.Event().wait())
    assert not supervisor.scopes and boundary.runner is None


@pytest.mark.asyncio
async def test_f2_signal_activation_cannot_interrupt_registration(monkeypatch):
    import signal

    import uvicorn

    from app.server import LifecycleServer

    supervisor = LifecycleSupervisor(30, clock=Clock())
    server = LifecycleServer(uvicorn.Config("app.main:app"), supervisor)
    original = supervisor.now

    def interrupted():
        value = original()
        server.handle_exit(signal.SIGTERM, None)
        return value

    monkeypatch.setattr(supervisor, "now", interrupted)
    boundary = supervisor.register()
    monkeypatch.setattr(supervisor, "now", original)
    await turn()
    assert boundary in supervisor.scopes and boundary.deadline == 30
    with pytest.raises(AdmissionClosed):
        supervisor.register()


@pytest.mark.asyncio
async def test_f2_server_cutoff_cancels_and_retains_server_owned_requests(monkeypatch):
    import uvicorn

    from app.server import LifecycleServer

    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    server = LifecycleServer(uvicorn.Config("app.main:app"), supervisor)
    entered = asyncio.Event()

    async def drain(self, sockets=None):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(uvicorn.Server, "shutdown", drain)
    request = asyncio.create_task(asyncio.Event().wait())
    server.server_state.tasks.add(request)
    runner = asyncio.create_task(server.shutdown())
    await entered.wait()
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(runner, 3)
    await asyncio.gather(request, return_exceptions=True)
    assert request.cancelled()
    assert request in supervisor.resources.values()
    await asyncio.gather(*supervisor.resources.values(), return_exceptions=True)
