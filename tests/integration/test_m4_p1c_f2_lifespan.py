"""Actual application lifespan/PG/Redis; synthetic request work, not wire shutdown."""

import asyncio

import pytest

from app.main import create_app
from tests.unit.test_m4_p1c_lifecycle import change, run, world


@pytest.mark.asyncio
async def test_f3_lifespan_supplies_original_supervisor_scope(monkeypatch):
    from app.normal_api.supervisor import LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    supervisor = LifecycleSupervisor(30, clock=Clock())
    app = create_app()
    app.state.lifecycle_supervisor = supervisor
    seen = []
    async with app.router.lifespan_context(app):
        runtime = app.state.redis_runtime
        original = runtime.close

        async def close(*, cleanup_scope):
            seen.append(cleanup_scope)
            assert cleanup_scope.supervisor is supervisor
            assert cleanup_scope.deadline == supervisor.process_deadline == 30
            await original(cleanup_scope=cleanup_scope)

        monkeypatch.setattr(runtime, "close", close)
    assert len(seen) == 1
    assert runtime._cleanup_scope is seen[0]
    assert runtime.disposal_states == {"legacy": "completed"}
    assert not supervisor.unclosed_resources


@pytest.mark.asyncio
async def test_f2_lifespan_waits_for_supervised_request_before_dependencies_close():
    w = await world()
    app = create_app()
    app.state.lifecycle_supervisor = w.supervisor
    context = app.router.lifespan_context(app)
    await context.__aenter__()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def execute(owner, prepared):
        entered.set()
        await finish.wait()
        await w.hooks.execute(owner, prepared)

    runner = asyncio.create_task(run(w, change(w.hooks, execute=execute)))
    exit_task = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        exit_task = asyncio.create_task(context.__aexit__(None, None, None))
        await w.clock.expect(30)
        assert w.supervisor.process_deadline == 30
        assert not app.state.normal_api_upstream_client.is_closed
        assert not app.state.invalidation_subscriber._task.done()
        assert not exit_task.done()
        finish.set()
        await asyncio.wait_for(runner, 5)
        await asyncio.wait_for(exit_task, 5)
        assert app.state.normal_api_upstream_client.is_closed
        assert not w.supervisor.unclosed_resources
    finally:
        finish.set()
        await asyncio.gather(runner, return_exceptions=True)
        if exit_task is None:
            await context.__aexit__(None, None, None)
        else:
            await asyncio.gather(exit_task, return_exceptions=True)
