"""F4B lifecycle acceptance on disposable Redis; no production upstream I/O."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
import redis

from app.normal_api.lifecycle import End, Policies
from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import (
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
    semaphore_key,
)
from app.redis.guarded_renewal import GuardedRenewalConnection
from app.redis.guarded_renewal import RenewalAttemptStatus as Status
from app.redis.guarded_renewal import RenewalDisposition as Disposition
from app.redis.protection_facade import RecoveryConcurrencySemaphore
from app.redis.recovery import Backend, RecoveryCoordinator
from app.redis.runtime import RedisDisposalError, RedisRuntime
from tests.integration import test_guarded_renewal_runtime as guarded_fixtures
from tests.integration.test_guarded_renewal_runtime import Gate
from tests.unit.test_m4_p1c_f4b_lifecycle import finish_blocked, start_blocked
from tests.unit.test_m4_p1c_lifecycle import world

disposable = guarded_fixtures.disposable

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_M4_F4B_REDIS") != "1",
    reason="Explicit disposable Redis F4B lifecycle acceptance opt-in required.",
)


@pytest_asyncio.fixture
async def engine(disposable):
    w = await world(ttl=5000)
    runtime = RedisRuntime(
        f"redis://:{disposable.password}@127.0.0.1:{disposable.port}/4", 3
    )
    await runtime.start()
    assert redis.__version__ == "8.1.0"
    assert (await runtime.client.info("server"))["redis_version"] == "7.4.11"
    primitive = RedisConcurrencySemaphore(runtime, 5000)
    coordinator = RecoveryCoordinator(
        runtime, None, None, None, clock=w.clock.now, lease_duration_ms=5000
    )
    facade = RecoveryConcurrencySemaphore(primitive, coordinator)
    w.concurrency = w.lifecycle.concurrency = facade
    policy = ResolvedConcurrencyPolicy(
        uuid4(),
        w.auth.identity.tenant_id,
        RateScopeType.SERVICE,
        w.rows["services"][0]["id"],
        1,
    )
    e = SimpleNamespace(
        w=w,
        runtime=runtime,
        primitive=primitive,
        facade=facade,
        coordinator=coordinator,
        policy=policy,
        writes=[],
        disconnects=[],
        gate=Gate(),
        stage=None,
        abort=False,
        outcomes=asyncio.Queue(),
        releases=[],
        expected_close_failure=False,
    )

    class Writer:
        def __init__(self, writer):
            self.writer = writer

        def __getattr__(self, name):
            return getattr(self.writer, name)

        def writelines(self, command):
            packed = list(command)
            if b"EVALSHA\r\n" in b"".join(packed):
                e.writes.append(w.clock.now())
                self.writer.writelines(packed)
                if e.abort:
                    self.writer.transport.abort()
            else:
                self.writer.writelines(packed)

        async def drain(self):
            if e.stage == "drain" and e.writes:
                await e.gate.wait()
            await self.writer.drain()

    class Connection(GuardedRenewalConnection):
        async def _connect(self):
            if e.stage == "reconnect":
                await e.gate.wait()
            await super()._connect()
            self._writer = Writer(self._writer)

        async def on_connect_check_health(self, check_health=True):
            if e.stage == "initialize":
                await e.gate.wait()
            await super().on_connect_check_health(check_health=check_health)
            self.initialized = True

        async def send_command(self, *args, **kwargs):
            self.last_command = str(args[0]).upper()
            await super().send_command(*args, **kwargs)

        async def send_renewal(self, *args, **kwargs):
            sent = await super().send_renewal(*args, **kwargs)
            if sent:
                self.last_command = "EVALSHA"
            return sent

        async def check_health(self):
            if getattr(self, "initialized", False):
                self.next_health_check = -1
            await super().check_health()

        async def read_response(self, *args, **kwargs):
            if e.stage == "auth" and getattr(self, "last_command", None) in {
                "AUTH",
                "HELLO",
            }:
                await e.gate.wait()
            if e.stage == "health" and getattr(self, "last_command", None) == "PING":
                await e.gate.wait()
            if (
                e.stage == "response"
                and getattr(self, "last_command", None) == "EVALSHA"
            ):
                await e.gate.wait()
            return await super().read_response(*args, **kwargs)

        async def disconnect(self, *args, **kwargs):
            e.disconnects.append(w.clock.now())
            await super().disconnect(*args, **kwargs)

    runtime.guarded_renewal_transport.pool.connection_class = Connection
    renew = facade.renew_guarded

    async def observe(*args, **kwargs):
        result = await renew(*args, **kwargs)
        await e.outcomes.put(result)
        return result

    facade.renew_guarded = observe
    release = primitive.release

    async def released(policies, lease):
        result = await release(policies, lease)
        e.releases.append((lease, result.released))
        return result

    primitive.release = released

    async def policies(resolved):
        return Policies((), (policy,))

    e.policies = policies
    pools = (runtime.guarded_renewal_transport.pool, runtime.client.connection_pool)
    e.test_pool_lock_held = False
    try:
        yield e
    finally:
        e.gate.resume.set()
        if hasattr(w, "finish"):
            w.finish.set()
        if hasattr(w, "runner"):
            w.runner.cancel()
            await asyncio.wait_for(asyncio.gather(w.runner, return_exceptions=True), 5)
            await asyncio.wait_for(
                asyncio.gather(
                    *w.deadline_scope.tasks.values(), return_exceptions=True
                ),
                5,
            )
        if e.test_pool_lock_held:
            pools[0]._lock.release()
            e.test_pool_lock_held = False
        # Test-owned physical cleanup, not a product retry or a release claim.
        for pool in pools:
            owner = pool._gateway_managed_owner
            seen = set()
            for connection in owner.connections:
                writers = [
                    record.writer
                    for record in owner.writer_obligations.get(connection, {}).values()
                ]
                if connection._writer is not None:
                    writers.append(connection._writer)
                for writer in writers:
                    if id(writer) in seen:
                        continue
                    seen.add(id(writer))
                    writer.close()
                    await writer.wait_closed()
                owner.closure_confirmed(connection)
                pool._in_use_connections.discard(connection)
        try:
            await runtime.close()
        except RedisDisposalError:
            if not e.expected_close_failure:
                raise


async def start(e, stage="body"):
    await start_blocked(e.w, stage=stage, policies=e.policies)
    assert e.w.lifecycle.handle.backend is Backend.REDIS
    return e.w


async def next_outcome(e):
    return await asyncio.wait_for(e.outcomes.get(), 5)


@pytest.mark.asyncio
async def test_l01_real_lifecycle_multiple_renewals_and_exact_original_release(engine):
    from app.normal_api.lifecycle import MonotonicClock

    e = engine

    class ObservedClock(MonotonicClock):
        def __init__(self):
            self.scheduled = asyncio.Queue()

        async def until(self, deadline):
            await self.scheduled.put(deadline)
            await super().until(deadline)

        async def expect(self, deadline):
            while await asyncio.wait_for(self.scheduled.get(), 10) != deadline:
                pass

    clock = ObservedClock()
    e.w.clock = clock
    e.w.supervisor = LifecycleSupervisor(30, clock=clock)
    e.w.deadline_scope = e.w.supervisor.register()
    e.coordinator._clock = clock.now
    w = await start(e, stage="deliver")
    issued = w.lifecycle.handle
    original_expiry = await e.runtime.client.zscore(
        semaphore_key(e.policy), str(issued.raw_lease_id)
    )
    for _ in range(3):
        outcome = await next_outcome(e)
        assert outcome.result.renewed
        await clock.expect(outcome.dispatched_at + 5 / 3)
        assert w.lifecycle._confirmed_anchor == outcome.dispatched_at
        assert not (await e.facade.acquire([e.policy])).acquired
    # Wait to the measured original Redis expiry, not an arbitrary timing sleep.
    # Redis TIME is re-read to establish the actual server-side boundary.
    while True:
        seconds, micros = await e.runtime.client.time()
        now_ms = seconds * 1000 + micros // 1000
        if now_ms >= original_expiry:
            break
        reached = asyncio.Event()
        timer = asyncio.get_running_loop().call_later(
            (original_expiry - now_ms) / 1000, reached.set
        )
        try:
            await reached.wait()
        finally:
            timer.cancel()
    assert not (await e.facade.acquire([e.policy])).acquired
    assert (
        await e.runtime.client.zscore(semaphore_key(e.policy), str(issued.raw_lease_id))
        > original_expiry
    )
    result = await finish_blocked(w)
    assert result.end is End.DELIVERED and result.cleanup_complete
    assert e.releases == [(issued.raw_lease_id, True)]
    subsequent = await e.facade.acquire([e.policy])
    assert subsequent.acquired
    assert (await e.facade.release([e.policy], subsequent.handle)).released
    assert len(e.writes) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "preparation",
    ["initialize", "auth", "health", "reconnect", "pool", "script", "task"],
)
async def test_l02_real_initialization_delay_denies_late_lifecycle_dispatch(
    engine, preparation
):
    e = engine
    e.stage = preparation
    transport = e.runtime.guarded_renewal_transport
    pool = transport.pool
    if preparation == "health":
        pool.connection_kwargs["health_check_interval"] = 1
    if preparation == "reconnect":
        e.stage = None  # Warm setup must not enter the later reconnect gate.
        await e.primitive._load("renew", transport=transport)
        for connection in pool._available_connections:
            await connection.disconnect()
        e.stage = "reconnect"
    if preparation == "pool":
        await pool._lock.acquire()
        e.test_pool_lock_held = True
        original = pool.get_connection

        async def checkout(*args, **kwargs):
            e.gate.entered.set()
            return await original(*args, **kwargs)

        pool.get_connection = checkout
    if preparation == "script":
        original = e.primitive._load

        async def load(*args, **kwargs):
            if args and args[0] == "renew":
                await e.gate.wait()
            return await original(*args, **kwargs)

        e.primitive._load = load
    if preparation == "task":
        original = e.facade.renew_guarded

        async def invoke(*args, **kwargs):
            await e.gate.wait()
            return await original(*args, **kwargs)

        e.facade.renew_guarded = invoke
    w = await start(e)
    await w.clock.expect(5 / 3)
    w.clock.advance(5 / 3)
    await asyncio.wait_for(e.gate.entered.wait(), 5)
    w.clock.advance(10 / 3)
    result = await asyncio.wait_for(w.runner, 5)
    assert result.end is End.OWNERSHIP_UNCERTAIN
    assert e.writes == [] and w.lifecycle._confirmed_anchor == 0
    record = w.lifecycle.last_renewal_evidence
    assert record is None or not record.attempts
    assert e.releases == [(w.lifecycle.handle.raw_lease_id, True)]
    assert not e.runtime.guarded_renewal_transport.pool._in_use_connections


@pytest.mark.asyncio
@pytest.mark.parametrize("late", [False, True])
async def test_l03_real_noscript_retry_uses_producing_timestamp_or_is_denied(
    engine, late
):
    e = engine
    w = await start(e)
    # Prime only the script cache on this disposable server; the primary action
    # remains the actual lifecycle's renewal, not a direct publisher/backend call.
    await e.primitive._load("renew", transport=e.runtime.guarded_renewal_transport)
    await e.runtime.client.script_flush()
    reload = e.primitive._load
    entered, resume = asyncio.Event(), asyncio.Event()

    async def gated(*args, **kwargs):
        if kwargs.get("force"):
            entered.set()
            while not resume.is_set():
                try:
                    await resume.wait()
                except asyncio.CancelledError:
                    if not late:
                        raise
                    # Deliberately finish reload after cancellation, proving
                    # denial at the real retry writer instead of only task abort.
                    continue
        return await reload(*args, **kwargs)

    e.primitive._load = gated
    try:
        await w.clock.expect(5 / 3)
        w.clock.advance(5 / 3)
        await asyncio.wait_for(entered.wait(), 5)
        record = w.lifecycle._owned_tasks["renew_operation"].guarded_renewal_operation
        snapshot = record.attempts
        assert snapshot[-1].status is Status.NOSCRIPT
        if late:
            w.clock.advance(10 / 3)
            await asyncio.wait_for(w.lifecycle._terminal_event.wait(), 5)
            resume.set()
            result = await asyncio.wait_for(w.runner, 5)
            assert result.end is End.OWNERSHIP_UNCERTAIN
            assert len(e.writes) == 1 and w.lifecycle._confirmed_anchor == 0
            assert record.attempts == snapshot
            if record.outcome is None:
                assert record.cancelled
            else:
                assert record.outcome.disposition is Disposition.SUPPRESSED
        else:
            w.clock.advance(2)
            resume.set()
            outcome = await next_outcome(e)
            await w.clock.expect(2 + 5 / 3)
            assert [a.status for a in outcome.attempts] == [
                Status.NOSCRIPT,
                Status.RESULT,
            ]
            assert outcome.dispatched_at == 2 and w.lifecycle._confirmed_anchor == 2
            assert snapshot[0].status is Status.NOSCRIPT
            assert (await finish_blocked(w)).end is End.DELIVERED
        assert e.releases == [(w.lifecycle.handle.raw_lease_id, True)]
    finally:
        resume.set()


@pytest.mark.asyncio
async def test_l04_real_uncertain_send_retained_across_one_bounded_retry(engine):
    e = engine
    e.abort = True
    w = await start(e)
    await w.clock.expect(5 / 3)
    w.clock.advance(5 / 3)
    first = await next_outcome(e)
    assert first.disposition is Disposition.UNKNOWN
    assert first.attempts[-1].status is Status.UNRESOLVED
    await w.clock.expect(2.5)
    e.abort = False
    w.clock.advance(2.5)
    second = await next_outcome(e)
    # The first command may or may not have reached Redis. Neither result is
    # invented; its already-acquired lease remains Redis's authoritative truth.
    assert second.disposition is Disposition.BACKEND_RESULT and second.result.renewed
    await w.clock.expect(2.5 + 5 / 3)
    assert len(e.writes) == 2 and w.lifecycle._confirmed_anchor == 2.5
    assert w.lifecycle.unresolved_renewals[0].attempts == first.attempts
    result = await finish_blocked(w)
    assert not result.cleanup_complete and w.deadline_scope in w.supervisor.residuals
    assert e.releases == [(w.lifecycle.handle.raw_lease_id, True)]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["request", "process"])
async def test_l05_real_pending_response_at_effective_cutoff_stays_owned(
    engine, boundary
):
    e = engine
    e.stage = "response"
    w = await start(e)
    w.supervisor.grace_seconds = 3  # Explicit fixture configuration, not a new default.
    if boundary == "process":
        w.supervisor.begin_shutdown()
    else:
        w.deadline_scope.begin_cleanup()
    await w.clock.expect(5 / 3)
    w.clock.advance(5 / 3)
    await asyncio.wait_for(e.gate.entered.wait(), 5)
    owner = e.runtime.guarded_renewal_transport.pool._gateway_managed_owner
    connection = next(iter(owner.connections))
    writer = connection._writer
    future = writer._protocol._get_close_waiter(writer)
    w.clock.advance(3)
    result = await asyncio.wait_for(w.runner, 5)
    await asyncio.wait_for(
        asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True), 5
    )
    assert result.end is End.SHUTDOWN and not result.cleanup_complete
    assert len(e.writes) == 1 and e.disconnects == []
    assert connection._gateway_quarantined and not future.done()
    assert writer.get_extra_info("socket").fileno() >= 0
    assert not owner.closure_confirmed(connection)
    assert w.deadline_scope in w.supervisor.residuals
    assert not w.lifecycle.release_attempted and e.releases == []
    assert w.lifecycle.unresolved_renewals[0].cleanup_pending
    # Actual response-wait cancellation, separate from L04 transport uncertainty.
    assert w.lifecycle.last_renewal_evidence.cancelled
    assert w.lifecycle.last_renewal_evidence.phase == "response"
    assert dict(w.lifecycle.renewal_evidence.cancellation_phase_counts)["response"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["guarded", "legacy"])
async def test_l06_lifecycle_and_both_real_pool_confirmation_failures_remain_unclosed(
    engine, kind
):
    e = engine
    w = await start(e)
    await w.clock.expect(5 / 3)
    w.clock.advance(5 / 3)
    assert (await next_outcome(e)).result.renewed
    await w.clock.expect(10 / 3)
    await w.clock.expect(10 / 3)
    pool = (
        e.runtime.guarded_renewal_transport.pool
        if kind == "guarded"
        else e.runtime.client.connection_pool
    )
    connection = pool._available_connections[0]
    writer = connection._writer
    original = writer.close
    future = writer._protocol._get_close_waiter(writer)
    attempts = []

    def fail():
        attempts.append(True)
        raise OSError("synthetic close failure")

    writer.close = fail
    try:
        await connection.disconnect()
        await connection.disconnect()
        assert attempts == [True] and not future.done()
        assert not pool._gateway_managed_owner.closure_confirmed(connection)
        result = await finish_blocked(w)
        assert result.end is End.DELIVERED
        assert e.releases == [(w.lifecycle.handle.raw_lease_id, True)]
        supervisor = LifecycleSupervisor(30, clock=w.clock)

        async def close():
            await e.runtime.close(cleanup_scope=LifecycleScope(supervisor))

        e.expected_close_failure = True
        await supervisor.close_resources([("redis", close)])
        assert supervisor.unclosed_resources == {"redis"}
        assert not future.done() and attempts == [True]
        with pytest.raises(RedisDisposalError):
            await e.runtime.close()
    finally:
        writer.close = original


@pytest.mark.asyncio
async def test_l04_repeated_uncertain_sends_compact_completed_tasks(engine):
    e = engine
    w = await start(e, stage="deliver")
    await w.clock.expect(5 / 3)
    anchor = 0
    first_snapshot = None
    for cycle in range(8):
        e.abort = True
        w.clock.advance(anchor + 5 / 3)
        unknown = await next_outcome(e)
        assert unknown.disposition is Disposition.UNKNOWN
        assert unknown.attempts[-1].status is Status.UNRESOLVED
        await w.clock.expect(anchor + 2.5)
        e.abort = False
        w.clock.advance(anchor + 2.5)
        confirmed = await next_outcome(e)
        assert confirmed.disposition is Disposition.BACKEND_RESULT
        assert confirmed.result.renewed
        anchor += 2.5
        await w.clock.expect(anchor + 5 / 3)
        snapshot = w.lifecycle.renewal_evidence
        if first_snapshot is None:
            first_snapshot = snapshot
        assert snapshot.completed_invocations == 2 * (cycle + 1)
        assert snapshot.uncertain_invocations == cycle + 1
        assert snapshot.live_invocations == snapshot.physical_obligations == 0
        assert len(snapshot.recent) <= 2
        assert not w.lifecycle._renewal_obligations
        assert first_snapshot.uncertain_invocations == 1
        assert w.lifecycle._confirmed_anchor == anchor
        assert w.lifecycle.committed and w.lifecycle.end is None
    assert len(e.writes) == 16
    result = await finish_blocked(w)
    assert result.end is End.DELIVERED and not result.cleanup_complete
    assert w.deadline_scope in w.supervisor.residuals
    assert e.releases == [(w.lifecycle.handle.raw_lease_id, True)]
