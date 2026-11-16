"""Opt-in, disposable Redis acceptance for the production R2 backend."""

import asyncio
import json
import os
import socket
import subprocess
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import AuthenticationError

from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import (
    RedisConcurrencySemaphore,
    ResolvedConcurrencyPolicy,
    semaphore_key,
)
from app.redis.guarded_renewal import GuardedRenewalConnection
from app.redis.guarded_renewal import RenewalAttemptStatus as Status
from app.redis.guarded_renewal import RenewalDisposition as Disposition
from app.redis.runtime import RedisRuntime

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_M4_F4A_REDIS") != "1",
    reason="Requires explicit disposable Docker Redis opt-in.",
)


class Guard:
    def __init__(self):
        self.now = 2.0
        self.horizon = 4.0
        self.calls = []

    def monotonic(self):
        return self.now

    def permit(self, sampled_at):
        self.calls.append(sampled_at)
        return sampled_at < self.horizon


class Gate:
    def __init__(self):
        self.entered = asyncio.Event()
        self.resume = asyncio.Event()

    async def wait(self):
        self.entered.set()
        await self.resume.wait()


def docker_cleanup_command(arguments, stage):
    try:
        result = subprocess.run(
            ["docker", *arguments],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        raise RuntimeError(
            f"Disposable Redis cleanup could not complete {stage}."
        ) from None
    if result.returncode:
        raise RuntimeError(f"Disposable Redis cleanup failed during {stage}.")
    return result.stdout


def cleanup_owned_container(name):
    suffix = name.removeprefix("m4-f4a-")
    if (
        not name.startswith("m4-f4a-")
        or len(suffix) != 32
        or any(c not in "0123456789abcdef" for c in suffix)
    ):
        raise RuntimeError("Disposable Redis cleanup identity is not test-owned.")
    names = docker_cleanup_command(
        ["ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"],
        "ownership lookup for " + name,
    ).splitlines()
    if not names:
        return
    if names != [name]:
        raise RuntimeError("Disposable Redis cleanup ownership is ambiguous.")
    raw = docker_cleanup_command(
        ["inspect", "--format", "{{json .}}", name], "inspection of " + name
    )
    try:
        container = json.loads(raw)
        labels = container["Config"]["Labels"]
        owned = (
            container["Name"] == "/" + name
            and isinstance(labels, dict)
            and labels.get("codex.m4-f4a.owner") == name
        )
        container_id = container["Id"]
        valid_id = len(container_id) == 64 and all(
            c in "0123456789abcdef" for c in container_id
        )
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("Disposable Redis cleanup identity is invalid.") from None
    if not owned or not valid_id:
        raise RuntimeError("Disposable Redis cleanup ownership was not confirmed.")
    # Use the inspected immutable ID so a later name change cannot target another container.
    docker_cleanup_command(
        ["rm", "-f", "-v", container_id], "removal of " + container_id
    )
    remaining = docker_cleanup_command(
        ["ps", "-a", "--filter", "id=" + container_id, "--format", "{{.ID}}"],
        "removal verification for " + container_id,
    )
    if remaining.strip():
        raise RuntimeError("Disposable Redis cleanup could not confirm removal.")


@pytest.fixture(scope="module")
def disposable():
    name = "m4-f4a-" + uuid4().hex
    password = os.urandom(24).hex()
    command = [
        "docker",
        "run",
        "-d",
        "--name",
        name,
        "--label",
        "codex.m4-f4a.owner=" + name,
        "--tmpfs",
        "/data",
        "-p",
        "127.0.0.1::6379",
        "redis:7-alpine",
        "redis-server",
        "--save",
        "",
        "--appendonly",
        "no",
        "--requirepass",
        password,
    ]
    try:
        try:
            result = subprocess.run(
                command, capture_output=True, timeout=30, check=False
            )
        except (subprocess.TimeoutExpired, OSError):
            raise RuntimeError(
                "Disposable Redis creation result is uncertain for " + name
            ) from None
        if result.returncode:
            pytest.fail("Disposable Redis could not start.")
        port_text = subprocess.check_output(
            ["docker", "port", name, "6379/tcp"], text=True, timeout=10
        )
        port = int(port_text.strip().rsplit(":", 1)[1])
        # Startup readiness polling only; concurrency assertions use event gates.
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    pytest.fail("Disposable Redis did not become ready.")
                threading.Event().wait(0.05)
        yield SimpleNamespace(port=port, password=password)
    finally:
        cleanup_owned_container(name)


@pytest_asyncio.fixture
async def backend(disposable):
    runtime = RedisRuntime(
        f"redis://:{disposable.password}@127.0.0.1:{disposable.port}/3", 3
    )
    await runtime.start()
    policy = ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.SERVICE, uuid4(), 1
    )
    semaphore = RedisConcurrencySemaphore(runtime, 5000)
    issued = await semaphore.acquire([policy])
    writes = []
    state = SimpleNamespace(gate=None, stage=None, abort=False)
    transport = runtime.guarded_renewal_transport

    class WriterProbe:
        def __init__(self, writer):
            self.writer = writer

        def __getattr__(self, name):
            return getattr(self.writer, name)

        def writelines(self, command):
            packed = list(command)
            renewal = b"EVALSHA\r\n" in b"".join(packed)
            if renewal:
                writes.append(tuple(packed))
            self.writer.writelines(packed)
            if renewal and state.abort:
                self.writer.transport.abort()

        async def drain(self):
            if state.stage == "drain" and writes:
                await state.gate.wait()
            await self.writer.drain()

    class ObservedConnection(GuardedRenewalConnection):
        async def _connect(self):
            self.initialized = False
            if state.stage == "connect":
                await state.gate.wait()
            await super()._connect()
            self._writer = WriterProbe(self._writer)

        async def on_connect_check_health(self, check_health=True):
            if state.stage == "initialize":
                await state.gate.wait()
            await super().on_connect_check_health(check_health)
            self.initialized = True

        async def send_command(self, *args, **kwargs):
            self.last_command = str(args[0]).upper()
            await super().send_command(*args, **kwargs)

        async def read_response(self, *args, **kwargs):
            if (
                state.stage == "auth"
                and self.last_command in ("AUTH", "HELLO")
                or state.stage == "health"
                and self.last_command == "PING"
            ):
                await state.gate.wait()
            response = await super().read_response(*args, **kwargs)
            if state.stage == "confirmed_response" and self.last_command == "EVALSHA":
                await state.gate.wait()
            return response

        async def send_renewal(self, *args, **kwargs):
            permitted = await super().send_renewal(*args, **kwargs)
            if permitted:
                self.last_command = "EVALSHA"
            return permitted

        async def check_health(self):
            if self.initialized:
                self.next_health_check = -1
            await super().check_health()

    transport.pool.connection_class = ObservedConnection
    transport.pool.connection_kwargs["health_check_interval"] = 1
    data = SimpleNamespace(
        runtime=runtime,
        semaphore=semaphore,
        policy=policy,
        lease=issued.lease_id,
        writes=writes,
        state=state,
        transport=transport,
    )
    try:
        yield data
    finally:
        await runtime.close()
        assert not transport.pool._in_use_connections
        assert all(not c.is_connected for c in transport.pool._available_connections)


@pytest.mark.asyncio
async def test_real_lua_cold_warm_server_time_and_stale_ownership(backend):
    b = backend
    guard = Guard()
    version = (await b.runtime.client.info("server"))["redis_version"]
    assert version.startswith("7.")
    print("Disposable Redis version", version)
    for _ in range(2):
        outcome = await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
        assert outcome.result.renewed and outcome.dispatched_at == 2
    assert len(b.writes) == 2
    score = await b.runtime.client.zscore(semaphore_key(b.policy), str(b.lease))
    sec, usec = await b.runtime.client.time()
    now = sec * 1000 + usec // 1000
    assert now < score <= now + 5000
    await b.runtime.client.zadd(semaphore_key(b.policy), {str(b.lease): 1})
    expired = await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    assert not expired.result.renewed
    assert expired.attempts[0].status is Status.RESULT


@pytest.mark.parametrize(
    "stage", ["connect", "initialize", "auth", "health", "pool", "pool_lock", "script"]
)
@pytest.mark.asyncio
async def test_real_preparation_expiry_queues_zero_renewal_bytes(
    backend, monkeypatch, stage
):
    b, guard, gate = backend, Guard(), Gate()
    b.state.stage, b.state.gate = stage, gate
    if stage == "script":
        original = b.transport.script_load

        async def load(script):
            sha = await original(script)
            await gate.wait()
            return sha

        monkeypatch.setattr(b.transport, "script_load", load)
    elif stage in ("pool", "pool_lock"):
        original = b.transport.pool.get_connection
        if stage == "pool_lock":
            await b.transport.pool._lock.acquire()

        async def checkout(*args, **kwargs):
            if stage == "pool_lock":
                gate.entered.set()
            else:
                await gate.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(b.transport.pool, "get_connection", checkout)
    task = asyncio.create_task(
        b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    )
    await asyncio.wait_for(gate.entered.wait(), 5)
    guard.now = 4
    gate.resume.set()
    if stage == "pool_lock":
        b.transport.pool._lock.release()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert not outcome.attempts and outcome.result is None and b.writes == []


@pytest.mark.parametrize("retry_time", [3.0, 4.0, 4.1])
@pytest.mark.asyncio
async def test_real_noscript_timestamp_and_denied_retry(
    backend, monkeypatch, retry_time
):
    b, guard, gate = backend, Guard(), Gate()
    await b.semaphore._load("renew")
    await (
        b.runtime.client.script_flush()
    )  # This fixture owns the entire Redis instance.
    original = b.semaphore._load

    async def load(action, *, force=False, transport=None):
        sha = await original(action, force=force, transport=transport)
        if force:
            await gate.wait()
        return sha

    monkeypatch.setattr(b.semaphore, "_load", load)
    task = asyncio.create_task(
        b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    )
    await asyncio.wait_for(gate.entered.wait(), 5)
    guard.now = retry_time
    gate.resume.set()
    outcome = await task
    assert outcome.attempts[0].status is Status.NOSCRIPT
    assert outcome.attempts[0].dispatched_at == 2
    assert guard.calls == [2, retry_time]
    if retry_time < 4:
        assert outcome.result.renewed and outcome.dispatched_at == retry_time
        assert len(b.writes) == 2
    else:
        assert outcome.disposition is Disposition.SUPPRESSED
        assert len(b.writes) == len(outcome.attempts) == 1
        assert outcome.result is None


@pytest.mark.asyncio
async def test_real_authentication_database_and_guarded_identity(disposable, backend):
    wrong = Redis(
        host="127.0.0.1",
        port=disposable.port,
        password="synthetic-wrong",
        socket_timeout=1,
    )
    other = Redis(
        host="127.0.0.1", port=disposable.port, password=disposable.password, db=4
    )
    try:
        with pytest.raises(AuthenticationError):
            await wrong.ping()
        assert await backend.runtime.client.ping()
        assert not await other.exists(semaphore_key(backend.policy))
        options = backend.transport.pool.connection_kwargs
        legacy = backend.runtime.client.connection_pool.connection_kwargs
        for field in (
            "host",
            "port",
            "db",
            "username",
            "password",
            "credential_provider",
        ):
            assert options.get(field) == legacy.get(field)
    finally:
        await wrong.aclose()
        await other.aclose()


@pytest.mark.asyncio
async def test_real_acl_error_reply_and_safe_reuse(backend):
    b = backend
    # Deny EVALSHA through Redis ACL after script preparation has completed.
    await b.semaphore._load("renew")
    await b.runtime.client.execute_command("ACL", "SETUSER", "default", "-evalsha")
    try:
        outcome = await b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
        assert outcome.disposition is Disposition.INTERNAL_ERROR
        assert outcome.attempts[-1].status is Status.RESOLVED_ERROR
        assert outcome.result is None and len(b.writes) == 1
        writer = b.transport.pool._available_connections[-1]._writer
    finally:
        await b.runtime.client.execute_command("ACL", "SETUSER", "default", "+evalsha")
    assert (
        await b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
    ).result.renewed
    assert len(b.writes) == 2
    assert b.transport.pool._available_connections[-1]._writer is writer


@pytest.mark.asyncio
async def test_real_uncertain_connection_disposal_and_recovery(backend):
    b, guard = backend, Guard()
    b.state.abort = True
    outcome = await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    assert outcome.disposition is Disposition.UNKNOWN
    assert outcome.attempts[-1].status is Status.UNRESOLVED
    assert len(b.writes) == 1
    b.state.abort = False
    guard.now = 4
    denied = await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    assert denied.disposition is Disposition.SUPPRESSED and len(b.writes) == 1
    guard.now = 2
    recovered = await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    assert recovered.result.renewed and len(b.writes) == 2


@pytest.mark.asyncio
async def test_real_reconnect_preparation_rechecks_live_guard(backend):
    b, guard, gate = backend, Guard(), Gate()
    assert (
        await b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    ).result.renewed
    await b.transport.pool.disconnect()
    b.state.stage, b.state.gate = "connect", gate
    task = asyncio.create_task(
        b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    )
    await asyncio.wait_for(gate.entered.wait(), 5)
    guard.now = 4
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert not outcome.attempts and len(b.writes) == 1


@pytest.mark.parametrize(
    "stage", ["script_lock", "script", "checkout", "auth", "health", "noscript"]
)
@pytest.mark.asyncio
async def test_real_runtime_closure_fences_pending_preparation(
    backend, monkeypatch, stage
):
    b, guard, gate = backend, Guard(), Gate()
    b.state.stage, b.state.gate = stage, gate
    if stage == "script_lock":
        await b.semaphore._script_lock.acquire()
        original = b.semaphore._load

        async def load(action, **kwargs):
            gate.entered.set()
            return await original(action, **kwargs)

        monkeypatch.setattr(b.semaphore, "_load", load)
    elif stage == "script":
        original = b.transport.script_load

        async def load(script):
            result = await original(script)
            await gate.wait()
            return result

        monkeypatch.setattr(b.transport, "script_load", load)
    elif stage == "checkout":
        original = b.transport.pool.get_connection

        async def checkout(*args, **kwargs):
            await gate.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(b.transport.pool, "get_connection", checkout)
    elif stage == "noscript":
        await b.semaphore._load("renew")
        await b.runtime.client.script_flush()
        original = b.semaphore._load

        async def load(action, *, force=False, **kwargs):
            if force:
                await gate.wait()
            return await original(action, force=force, **kwargs)

        monkeypatch.setattr(b.semaphore, "_load", load)
    task = asyncio.create_task(
        b.semaphore.renew_guarded([b.policy], b.lease, guard=guard)
    )
    await asyncio.wait_for(gate.entered.wait(), 5)
    count_at_close = len(b.transport.pool._available_connections) + len(
        b.transport.pool._in_use_connections
    )
    await b.runtime.close()
    if stage == "script_lock":
        b.semaphore._script_lock.release()
    gate.resume.set()
    outcome = await task
    assert b.transport.closed
    assert outcome.disposition is Disposition.SUPPRESSED
    assert outcome.result is None
    assert [a.status for a in outcome.attempts] == (
        [Status.NOSCRIPT] if stage == "noscript" else []
    )
    assert len(b.writes) == (1 if stage == "noscript" else 0)
    assert not b.transport.pool._in_use_connections
    assert all(not c.is_connected for c in b.transport.pool._available_connections)
    assert len(b.transport.pool._available_connections) <= count_at_close


@pytest.mark.asyncio
async def test_real_retained_transport_is_inert_after_runtime_close(backend):
    from app.redis.guarded_renewal import GuardedRenewalOperation

    b = backend
    await b.runtime.close()
    operation = GuardedRenewalOperation()
    assert (
        await b.transport.execute(
            "unused-sha",
            [semaphore_key(b.policy)],
            [5000, str(b.lease)],
            guard=Guard(),
            operation=operation,
        )
        is None
    )
    assert not operation.attempts and not b.writes
    assert not b.transport.pool._in_use_connections


@pytest.mark.parametrize("stage", ["drain", "confirmed_response"])
@pytest.mark.asyncio
async def test_real_closure_after_writer_preserves_execution_evidence(backend, stage):
    b, gate = backend, Gate()
    b.state.stage, b.state.gate = stage, gate
    task = asyncio.create_task(
        b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
    )
    await asyncio.wait_for(gate.entered.wait(), 5)
    assert len(b.writes) == 1
    await b.runtime.close()
    gate.resume.set()
    outcome = await task
    assert len(outcome.attempts) == 1 and outcome.attempts[0].dispatched_at == 2
    if stage == "drain":
        assert outcome.disposition is Disposition.UNKNOWN
        assert outcome.attempts[0].status is Status.UNRESOLVED
        assert outcome.result is None
    else:
        assert outcome.disposition is Disposition.BACKEND_RESULT
        assert outcome.result.renewed and outcome.dispatched_at == 2


@pytest.mark.asyncio
async def test_real_managed_disposal_confirms_both_pool_socket_closures(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor

    b = backend
    assert (await b.runtime.client.info("server"))["redis_version"] == "7.4.11"
    assert (
        await b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
    ).result.renewed
    guarded_connections = tuple(b.transport.pool._available_connections)
    legacy_connections = tuple(b.runtime.client.connection_pool._available_connections)
    assert guarded_connections and legacy_connections
    supervisor = LifecycleSupervisor(30)
    scope = LifecycleScope(supervisor)

    async def close():
        await b.runtime.close(cleanup_scope=scope)

    await supervisor.close_resources([("redis", close)])
    assert not supervisor.unclosed_resources
    assert b.runtime.disposal_states == {"guarded": "completed", "legacy": "completed"}
    for adapter in (b.transport._pool_disposal, b.runtime._legacy_pool_disposal):
        assert adapter.states and set(adapter.states.values()) == {"completed"}
        assert all(task.done() for task in adapter.tasks.values())
    assert all(
        not connection.is_connected
        for connection in (*guarded_connections, *legacy_connections)
    )
    assert (
        await b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
    ).disposition is Disposition.SUPPRESSED
    from app.redis.guarded_renewal import GuardedTransportClosed

    for pool in (b.transport.pool, b.runtime.client.connection_pool):
        with pytest.raises(GuardedTransportClosed):
            await pool.get_connection()
        with pytest.raises(GuardedTransportClosed):
            pool.make_connection()
        for connection in pool._gateway_managed_owner.connections:
            with pytest.raises(GuardedTransportClosed):
                await connection.connect()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["connect", "initialize"])
async def test_real_f5_guarded_establishment_owned_until_terminal(backend, stage):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import GuardedTransportClosed

    b, gate = backend, Gate()
    assert (await b.runtime.client.info("server"))["redis_version"] == "7.4.11"
    b.state.stage, b.state.gate = stage, gate
    checking = asyncio.create_task(b.transport.pool.get_connection())
    await asyncio.wait_for(gate.entered.wait(), 5)
    owner = b.transport.ownership
    assert owner.inflight and len(owner.connections) == 1
    connection = next(iter(owner.connections))
    assert connection.is_connected is (stage == "initialize")
    supervisor = LifecycleSupervisor(30)
    scope = LifecycleScope(supervisor)

    async def close():
        await b.runtime.close(cleanup_scope=scope)

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await asyncio.wait_for(owner.closed_admission.wait(), 5)
    assert not b.runtime._close_task.done()
    assert not closing.done()
    gate.resume.set()
    with pytest.raises(GuardedTransportClosed):
        await checking
    await asyncio.wait_for(closing, 5)
    assert not owner.inflight and not supervisor.unclosed_resources
    assert not connection.is_connected
    assert b.writes == []
    assert b.runtime.disposal_states == {"guarded": "completed", "legacy": "completed"}


@pytest.mark.asyncio
async def test_real_f5_legacy_socket_initialization_remains_owned(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import GuardedTransportClosed

    b, gate = backend, Gate()
    assert (await b.runtime.client.info("server"))["redis_version"] == "7.4.11"
    pool = b.runtime.client.connection_pool
    await pool.disconnect()
    pool._available_connections.clear()  # all are closed; owner still retains them

    async def initialize(connection):
        await gate.wait()
        await connection.on_connect_check_health()

    pool.connection_kwargs["redis_connect_func"] = initialize
    checking = asyncio.create_task(pool.get_connection())
    await asyncio.wait_for(gate.entered.wait(), 5)
    connection = next(iter(pool._in_use_connections))
    assert connection.is_connected
    owner = pool._gateway_managed_owner
    supervisor = LifecycleSupervisor(30)

    async def close():
        await b.runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await asyncio.wait_for(owner.closed_admission.wait(), 5)
    assert owner.inflight and not b.runtime._close_task.done()
    gate.resume.set()
    with pytest.raises(GuardedTransportClosed):
        await checking
    await asyncio.wait_for(closing, 5)
    assert not connection.is_connected
    assert not owner.inflight and not supervisor.unclosed_resources


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["guarded", "legacy"])
async def test_real_f6_writerless_noop_retains_real_socket_confirmation(backend, kind):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor

    b = backend
    assert (await b.runtime.client.info("server"))["redis_version"] == "7.4.11"
    if kind == "guarded":
        assert (
            await b.semaphore.renew_guarded([b.policy], b.lease, guard=Guard())
        ).result.renewed
        pool = b.transport.pool
    else:
        pool = b.runtime.client.connection_pool
    connection = pool._available_connections[-1]
    writer = connection._writer
    original_close = writer.close
    attempts = []

    def fail_close():
        attempts.append(True)
        raise OSError("synthetic writer close failure")

    writer.close = fail_close
    owner = pool._gateway_managed_owner
    try:
        await connection.disconnect()
        assert owner.last_cleanup[connection] == "unresolved"
        record = next(iter(owner.writer_obligations[connection].values()))
        assert not record.confirmation.done()
        await connection.disconnect()
        assert owner.last_cleanup[connection] == "unresolved"
        assert not owner.closure_confirmed(connection)
        assert attempts == [True]
    finally:
        # Test-owned producer completes the genuine existing socket-close waiter.
        # This is not a runtime retry or post-cutoff compensating cleanup.
        original_close()
        await writer.wait_closed()
    assert owner.closure_confirmed(connection)
    supervisor = LifecycleSupervisor(30)

    async def close():
        await b.runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    assert not supervisor.unclosed_resources
    assert attempts == [True]
    assert not connection.is_connected


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["drain", "confirmed_response", "initialize"])
@pytest.mark.parametrize("cancel_at", [2.0, 3.0])
async def test_real_f7_lifecycle_cutoff_quarantines_without_false_socket_closure(
    backend, stage, cancel_at
):
    from app.normal_api.lifecycle import End, Policies
    from app.normal_api.supervisor import LifecycleSupervisor
    from app.redis.protection_facade import RecoveryConcurrencySemaphore
    from app.redis.recovery import RecoveryCoordinator
    from tests.unit.test_m4_p1c_lifecycle import change, run, world

    b = backend
    assert (await b.runtime.client.info("server"))["redis_version"] == "7.4.11"
    w = await world(ttl=5000)
    w.supervisor = LifecycleSupervisor(3, clock=w.clock)
    w.deadline_scope = w.supervisor.register()
    coordinator = RecoveryCoordinator(
        b.runtime, None, None, None, clock=w.clock.now, lease_duration_ms=5000
    )
    facade = RecoveryConcurrencySemaphore(b.semaphore, coordinator)
    w.concurrency.acquire = facade.acquire
    w.concurrency.renew_guarded = facade.renew_guarded
    w.concurrency.release = facade.release
    cleanup_entries = []
    base = b.transport.pool.connection_class

    class ConnectionEntryProbe(base):
        async def disconnect(self, **kwargs):
            cleanup_entries.append((w.clock.now(), w.deadline_scope.expired))
            return await super().disconnect(**kwargs)

    b.transport.pool.connection_class = ConnectionEntryProbe
    b.state.stage, b.state.gate = stage, Gate()
    policy = ResolvedConcurrencyPolicy(
        uuid4(),
        w.auth.identity.tenant_id,
        RateScopeType.SERVICE,
        w.rows["services"][0]["id"],
        1,
    )

    async def policies(resolved):
        return Policies((), (policy,))

    async def body(*args):
        await asyncio.Event().wait()

    runner = asyncio.create_task(run(w, change(w.hooks, policies=policies, body=body)))
    connections = []
    try:
        await w.clock.expect(5000 / 3000)
        w.supervisor.begin_shutdown()
        w.clock.advance(5000 / 3000)
        await asyncio.wait_for(b.state.gate.entered.wait(), 3)
        connections = list(b.transport.pool._gateway_managed_owner.connections)
        assert len(connections) == 1
        connection = connections[0]
        raw_socket = connection._writer.get_extra_info("socket")
        assert raw_socket is not None and raw_socket.fileno() >= 0
        w.clock.advance(cancel_at)
        if cancel_at < 3:
            w.lifecycle.cancel(End.CANCELLED)
        result = await asyncio.wait_for(runner, 3)
        await asyncio.wait_for(
            asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True), 3
        )
        assert all(t < 3 and not expired for t, expired in cleanup_entries)
        if stage != "initialize":
            assert len(b.writes) == 1
            assert (
                w.lifecycle.unresolved_renewals[0].attempts[-1].status
                is Status.UNRESOLVED
            )
        else:
            assert not b.writes
        if stage == "initialize" and cancel_at < 3:
            assert result.cleanup_complete
            assert w.deadline_scope not in w.supervisor.residuals
        else:
            assert not result.cleanup_complete
            assert w.deadline_scope in w.supervisor.residuals
        if cancel_at < 3:
            assert w.lifecycle.release_result.released
        else:
            assert not w.lifecycle.release_attempted
        if cancel_at == 3:
            assert cleanup_entries == []
            assert connection._gateway_quarantined
            assert connection in b.transport.pool._in_use_connections
            assert connection not in b.transport.pool._available_connections
            owner = b.transport.pool._gateway_managed_owner
            assert not owner.closure_confirmed(connection)
            assert raw_socket.fileno() >= 0  # Logical invalidation did not close it.
            # No quarantine reuse even if another checkout is attempted.
            other = b.transport.pool.get_available_connection()
            assert other is not connection
            await b.transport.pool.release(other)
            assert w.deadline_scope in w.supervisor.residuals
    finally:
        b.state.gate.resume.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await asyncio.gather(*w.deadline_scope.tasks.values(), return_exceptions=True)
        # Explicit test-owned socket disposal only. This is neither a product
        # retry nor evidence that runtime cleanup ran after the cutoff.
        for connection in connections:
            writer = connection._writer
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            b.transport.pool._gateway_managed_owner.closure_confirmed(connection)
            b.transport.pool._in_use_connections.discard(connection)
