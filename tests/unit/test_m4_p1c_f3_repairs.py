import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
import uvicorn

from app.normal_api.lifecycle import End, Policies
from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import RedisConcurrencySemaphore, ResolvedConcurrencyPolicy
from app.redis.protection_facade import RecoveryConcurrencySemaphore
from app.server import LifecycleServer
from tests.unit.test_m4_p1c_f2_supervisor import turn
from tests.unit.test_m4_p1c_lifecycle import change, run, world


@pytest.mark.asyncio
async def test_f3_expired_shutdown_requests_close_cancel_and_retains_ownership():
    w = await world()
    entered, closed = asyncio.Event(), asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    class Listener:
        close_requested = False

        def close(self):
            self.close_requested = True

        async def wait_closed(self):
            await closed.wait()

    listener = Listener()
    server = LifecycleServer(uvicorn.Config("app.main:app"), w.supervisor)
    server.servers = [listener]
    runner = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    server.server_state.tasks.add(runner)
    try:
        await entered.wait()
        w.supervisor.begin_shutdown()
        w.clock.advance(31)
        await server.shutdown()
        assert listener.close_requested
        assert runner.cancelling()
        assert runner in w.supervisor.resources.values()
        assert w.supervisor.process_deadline == 30
        assert w.deadline_scope.deadline == 30
        assert "listener_0" in w.supervisor.unclosed_resources
        assert "application_lifespan" in w.supervisor.unclosed_resources
        assert w.lifecycle.handle is w.handle
        assert not w.lifecycle.release_attempted
    finally:
        closed.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)
        await asyncio.gather(*w.supervisor.resources.values(), return_exceptions=True)
        await turn()
    assert "listener_0" not in w.supervisor.unclosed_resources


@pytest.mark.asyncio
async def test_f3_actual_m3_release_protocol_uncertainty_stays_owned():
    w = await world()
    dispatched = []

    class Client:
        async def script_load(self, script):
            return "fixture-sha"

        async def evalsha(self, sha, count, *args):
            dispatched.append(args)
            # Actual M3 decoder rejects this reply after the operation dispatch.
            return b"untrusted-result"

    primitive = RedisConcurrencySemaphore(SimpleNamespace(client=Client()), 6000)
    coordinator = SimpleNamespace(owner_id=w.handle.owner_id)
    facade = RecoveryConcurrencySemaphore(primitive, coordinator)
    w.concurrency.release = facade.release

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

    result = await run(w, change(w.hooks, policies=policies))
    await turn()
    assert result.end == End.DELIVERED and result.committed
    assert len(dispatched) == 1
    assert w.lifecycle.handle is w.handle
    assert w.lifecycle.release_result is None
    assert w.lifecycle.release_obligation == "attempted_unconfirmed"
    assert not result.cleanup_complete
    assert w.deadline_scope in w.supervisor.residuals
    await turn()
    assert len(dispatched) == 1


@pytest.mark.asyncio
async def test_f3_cleanup_fault_after_renewal_stop_cannot_report_complete():
    w = await world()
    original = w.lifecycle._join_before_shutdown
    calls = []

    async def fault(*tasks):
        await original(*tasks)
        calls.append("stopped")
        raise RuntimeError("controlled post-stop failure")

    w.lifecycle._join_before_shutdown = fault
    result = await run(w)
    await turn()
    assert "cleanup_failed" in w.lifecycle.cleanup_failures
    assert not w.lifecycle.outstanding_tasks
    assert not result.cleanup_complete
    assert w.lifecycle.release_obligation == "not_attempted"
    assert w.deadline_scope in w.supervisor.residuals
    assert not w.lifecycle.release_attempted and not w.concurrency.released
    assert calls == ["stopped"]


@pytest.mark.asyncio
async def test_f3_confirmed_success_still_reports_complete():
    w = await world()
    result = await run(w)
    await turn()
    assert result.cleanup_complete
    assert w.lifecycle.release_obligation == "confirmed_success"
    assert not w.supervisor.residuals


@pytest.mark.asyncio
async def test_f3_unresolved_circuit_attempt_is_not_complete_or_forgotten():
    w = await world()
    calls = []

    async def uncertain(*args):
        calls.append("attempted")
        raise RuntimeError("controlled circuit completion fault")

    w.circuit.record_success = uncertain
    result = await run(w)
    await turn()
    assert result.end == End.DELIVERED and result.committed
    assert not result.cleanup_complete
    assert w.lifecycle.release_obligation == "confirmed_success"
    assert w.lifecycle.circuit_obligation == "unresolved"
    assert w.deadline_scope in w.supervisor.residuals
    assert calls == ["attempted"]
