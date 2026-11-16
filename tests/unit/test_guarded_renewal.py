import asyncio
import copy
import importlib.util
import inspect
import json
import subprocess
from contextlib import nullcontext
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.exceptions import (
    AuthenticationError,
    BusyLoadingError,
    ConnectionError,
    NoPermissionError,
    NoScriptError,
    ResponseError,
)

from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import (
    RedisConcurrencySemaphore,
    RenewResult,
    ResolvedConcurrencyPolicy,
)
from app.redis.guarded_renewal import (
    GuardedRenewalConnection,
    GuardedRenewalOperation,
    GuardedRenewalOutcome,
    GuardedRenewalTransport,
    GuardedTransportClosed,
    RenewalAttempt,
    RenewalFailure,
)
from app.redis.guarded_renewal import (
    RenewalAttemptStatus as Status,
)
from app.redis.guarded_renewal import (
    RenewalDisposition as Disposition,
)
from app.redis.local_degraded import LocalConcurrencyStore
from app.redis.protection_facade import ConcurrencyHandle, RecoveryConcurrencySemaphore
from app.redis.recovery import Backend
from app.redis.runtime import RedisRuntime


class Gate:
    def __init__(self):
        self.entered = asyncio.Event()
        self.resume = asyncio.Event()

    async def wait(self):
        self.entered.set()
        await self.resume.wait()


class Guard:
    def __init__(self):
        self.now = 2.0
        self.horizon = 4.0
        self.cutoff = 10.0
        self.terminal = False
        self.decision = True
        self.error = None
        self.calls = []

    def monotonic(self):
        return self.now

    def permit(self, sampled_at):
        self.calls.append(sampled_at)
        if self.error:
            raise self.error
        if self.decision is not True:
            return self.decision
        return not self.terminal and sampled_at < min(self.horizon, self.cutoff)


def policy():
    return ResolvedConcurrencyPolicy(
        uuid4(), uuid4(), RateScopeType.SERVICE, uuid4(), 1
    )


class Writer:
    def __init__(self):
        self.writes = []
        self.gate = None
        self.error = None

    def writelines(self, command):
        self.writes.append(b"".join(command))
        if self.error:
            raise self.error

    async def drain(self):
        if self.gate:
            await self.gate.wait()

    def close(self):
        pass


class Connection(GuardedRenewalConnection):
    def __init__(self):
        super().__init__(socket_timeout=2)
        self._writer = Writer()
        self.connected = True
        self.preparation = None
        self.health = None
        self.reply_gate = None
        self.replies = [1]
        self.disconnects = 0

    @property
    def is_connected(self):
        return self.connected

    async def connect_check_health(self, **kwargs):
        if self.preparation:
            await self.preparation.wait()

    async def check_health(self):
        if self.health:
            await self.health.wait()

    async def read_response(self):
        if self.reply_gate:
            await self.reply_gate.wait()
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply

    async def disconnect(self, **kwargs):
        self.disconnects += 1
        self.connected = False


class Pool:
    def __init__(self, connection):
        self.connection = connection
        self.checkout = None
        self.releases = 0

    async def get_connection(self):
        if self.checkout:
            await self.checkout.wait()
        self.connection.connected = True
        return self.connection

    async def release(self, connection):
        assert connection is self.connection
        self.releases += 1

    async def disconnect(self):
        await self.connection.disconnect()


class Scripts:
    def __init__(self):
        self.calls = 0
        self.gate = None
        self.failure = None

    async def script_load(self, script):
        self.calls += 1
        if self.gate:
            await self.gate.wait()
        if self.failure:
            raise self.failure
        return "sha"


def setup():
    connection = Connection()
    transport = object.__new__(GuardedRenewalTransport)
    transport._loop = asyncio.get_running_loop()
    transport._closed = False
    transport._close_task = None
    transport.pool = Pool(connection)
    transport.timeout_seconds = 2
    scripts = Scripts()

    async def load(script):
        transport.require_open()
        return await scripts.script_load(script)

    transport.script_load = load
    runtime = SimpleNamespace(client=scripts, guarded_renewal_transport=transport)
    return RedisConcurrencySemaphore(runtime, 5000), connection, scripts, transport


async def bounded(event):
    await asyncio.wait_for(event.wait(), 3)


@pytest.mark.asyncio
async def test_confirmed_result_and_immutable_producing_evidence():
    semaphore, connection, _, _ = setup()
    guard = Guard()
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    assert outcome.disposition is Disposition.BACKEND_RESULT
    assert outcome.result == RenewResult(True)
    assert outcome.dispatched_at == outcome.attempts[-1].dispatched_at == 2
    assert outcome.attempts[-1].backend == "REDIS"
    assert len(connection._writer.writes) == 1
    with pytest.raises(FrozenInstanceError):
        outcome.result = None


@pytest.mark.parametrize("reply", [0, 1])
@pytest.mark.asyncio
async def test_legacy_renewal_result_values(reply):
    semaphore, connection, _, _ = setup()
    connection.replies = [reply]
    result = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert result.result == RenewResult(bool(reply))


@pytest.mark.parametrize("stage", ["script", "checkout", "connection", "health"])
@pytest.mark.asyncio
async def test_expiry_during_preparation_sends_nothing(stage):
    semaphore, connection, scripts, transport = setup()
    guard, gate = Guard(), Gate()
    if stage == "script":
        scripts.gate = gate
    elif stage == "checkout":
        transport.pool.checkout = gate
    elif stage == "connection":
        connection.preparation = gate
    else:
        connection.health = gate
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    )
    await bounded(gate.entered)
    guard.now = guard.horizon
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert outcome.attempts == () and outcome.result is None
    assert connection._writer.writes == []


@pytest.mark.asyncio
async def test_timed_send_task_handoff_precedes_guard(monkeypatch):
    from app.redis import guarded_renewal

    connection, guard = Connection(), Guard()
    operation = GuardedRenewalOperation()
    gate = Gate()

    class SchedulingBoundary:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def wait_for(self, coroutine, timeout):
            await gate.wait()
            return await asyncio.wait_for(coroutine, timeout)

    # Scope instrumentation to this module, without changing global asyncio.
    monkeypatch.setattr(guarded_renewal, "asyncio", SchedulingBoundary())
    task = asyncio.create_task(
        connection.send_renewal(("EVALSHA", "sha", 0), guard, operation)
    )
    await bounded(gate.entered)
    guard.now = 4.0
    gate.resume.set()
    assert not await task
    assert guard.calls == [4.0]
    assert not connection._writer.writes


@pytest.mark.parametrize("decision", [False, 1, "yes", None])
@pytest.mark.asyncio
async def test_strict_guard_decisions(decision):
    semaphore, connection, _, _ = setup()
    guard = Guard()
    guard.decision = decision
    result = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    expected = Disposition.SUPPRESSED if decision is False else Disposition.GUARD_ERROR
    assert result.disposition is expected
    assert not result.attempts and not connection._writer.writes


@pytest.mark.parametrize("stamp", [True, float("nan"), float("inf"), "2"])
@pytest.mark.asyncio
async def test_invalid_clock_fails_closed(stamp):
    semaphore, connection, _, _ = setup()
    guard = Guard()
    guard.now = stamp
    result = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    assert result.disposition is Disposition.GUARD_ERROR
    assert not connection._writer.writes


@pytest.mark.parametrize("after", [3.0, 4.0, 4.1])
@pytest.mark.asyncio
async def test_noscript_retry_has_fresh_guard_and_timestamp(after):
    semaphore, connection, scripts, _ = setup()
    guard, gate = Guard(), Gate()
    connection.replies = [NoScriptError("synthetic"), 1]
    original = scripts.script_load

    async def load(script):
        if scripts.calls:
            await gate.wait()
        return await original(script)

    scripts.script_load = load
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    )
    await bounded(gate.entered)
    guard.now = after
    gate.resume.set()
    result = await task
    assert result.attempts[0].status is Status.NOSCRIPT
    assert result.attempts[0].dispatched_at == 2
    assert guard.calls == [2, after]
    if after < 4:
        assert result.dispatched_at == after and result.result == RenewResult(True)
        assert len(connection._writer.writes) == 2
    else:
        assert result.disposition is Disposition.SUPPRESSED
        assert len(result.attempts) == len(connection._writer.writes) == 1
        assert result.result is None


@pytest.mark.parametrize(
    "failure", [ConnectionError("secret"), ResponseError("secret")]
)
@pytest.mark.asyncio
async def test_noscript_reload_failure_retains_history(failure):
    semaphore, connection, scripts, _ = setup()
    connection.replies = [NoScriptError("missing")]
    original = scripts.script_load

    async def load(script):
        if scripts.calls:
            raise failure
        return await original(script)

    scripts.script_load = load
    result = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert result.disposition is (
        Disposition.UNKNOWN
        if type(failure) is ConnectionError
        else Disposition.INTERNAL_ERROR
    )
    assert [a.status for a in result.attempts] == [Status.NOSCRIPT]
    assert len(connection._writer.writes) == 1
    assert result.result is None and "secret" not in repr(result)


@pytest.mark.parametrize(
    "failure,disposition",
    [
        (NoPermissionError("secret"), Disposition.INTERNAL_ERROR),
        (ResponseError("secret"), Disposition.INTERNAL_ERROR),
        (AuthenticationError("secret"), Disposition.INTERNAL_ERROR),
        (BusyLoadingError("secret"), Disposition.UNKNOWN),
    ],
)
@pytest.mark.asyncio
async def test_definitive_errors_are_resolved_and_classified(failure, disposition):
    semaphore, connection, _, _ = setup()
    connection.replies = [failure]
    result = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert result.disposition is disposition
    assert result.attempts[0].status is Status.RESOLVED_ERROR
    assert result.attempts[0].dispatched_at == 2
    assert result.result is None and len(connection._writer.writes) == 1
    assert "secret" not in repr(result)


@pytest.mark.asyncio
async def test_uncertain_send_disconnected_without_retransmit_or_erasing_old_outcome():
    semaphore, connection, _, _ = setup()
    connection.replies = [ConnectionError("synthetic")]
    guard = Guard()
    first = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    assert first.disposition is Disposition.UNKNOWN
    assert first.attempts[0].status is Status.UNRESOLVED
    assert connection.disconnects == 1
    guard.now = 4
    second = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    assert second.disposition is Disposition.SUPPRESSED and not second.attempts
    assert first.attempts[0].status is Status.UNRESOLVED
    assert len(connection._writer.writes) == 1


@pytest.mark.parametrize("stage", ["preparation", "drain", "response", "reload"])
@pytest.mark.asyncio
async def test_cancellation_propagates_and_task_retains_evidence(stage):
    semaphore, connection, scripts, _ = setup()
    gate = Gate()
    if stage == "preparation":
        scripts.gate = gate
    elif stage == "drain":
        connection._writer.gate = gate
    elif stage == "response":
        connection.reply_gate = gate
    else:
        connection.replies = [NoScriptError("missing")]
        original = scripts.script_load

        async def load(script):
            if scripts.calls:
                await gate.wait()
            return await original(script)

        scripts.script_load = load
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    evidence = task.guarded_renewal_operation
    assert evidence.cancelled and evidence.outcome is None
    expected = (
        []
        if stage == "preparation"
        else [Status.NOSCRIPT if stage == "reload" else Status.UNRESOLVED]
    )
    assert [a.status for a in evidence.attempts] == expected


@pytest.mark.asyncio
async def test_local_lock_denial_precedes_cleanup_and_legacy_is_unchanged():
    lease_clock = [10.0]
    store = LocalConcurrencyStore(
        8, lambda: lease_clock[0], 5000, SimpleNamespace(record=lambda *a: None)
    )
    policies = [policy()]
    issued = await store.acquire(policies)
    guard = Guard()
    await store._lock.acquire()
    before = copy.deepcopy(store._entries)
    task = asyncio.create_task(
        store.renew_guarded(policies, issued.lease_id, guard=guard)
    )
    # Queue task entry deterministically ahead of the acknowledgement callback.
    entered = asyncio.Event()
    asyncio.get_running_loop().call_soon(entered.set)
    await entered.wait()
    guard.now = 4
    lease_clock[0] = 20
    store._lock.release()
    result = await task
    assert result.disposition is Disposition.SUPPRESSED and store._entries == before
    assert not (await store.renew(policies, issued.lease_id)).renewed
    assert store._entries != before


@pytest.mark.asyncio
async def test_local_success_and_expiry_use_local_clock():
    clock = [10.0]
    store = LocalConcurrencyStore(
        8, lambda: clock[0], 5000, SimpleNamespace(record=lambda *a: None)
    )
    policies = [policy()]
    issued = await store.acquire(policies)
    result = await store.renew_guarded(policies, issued.lease_id, guard=Guard())
    assert result.result == RenewResult(True)
    assert result.attempts[0].backend == "LOCAL"
    clock[0] = 20
    result = await store.renew_guarded(policies, issued.lease_id, guard=Guard())
    assert result.result == RenewResult(False)


@pytest.mark.asyncio
async def test_runtime_is_lazy_and_tls_fails_closed():
    runtime = RedisRuntime("rediss://localhost:6379/3", 1)
    await runtime.start()
    assert runtime._guarded_renewal_transport is None
    with pytest.raises(RuntimeError, match="security"):
        _ = runtime.guarded_renewal_transport
    await runtime.close()


@pytest.mark.parametrize("kind", ["redis", "local", "unknown", "missing"])
@pytest.mark.asyncio
async def test_facade_preserves_backend_and_generation(kind):
    calls = []
    outcome = GuardedRenewalOutcome(
        Disposition.UNKNOWN, failure_category=RenewalFailure.PREPARATION_DEPENDENCY
    )

    class Store:
        async def renew_guarded(self, policies, lease, *, guard):
            calls.append((self, lease))
            return outcome

    normal, local = Store(), Store()
    owner = uuid4()
    coordinator = SimpleNamespace(
        owner_id=owner,
        local_generation=lambda generation: (
            SimpleNamespace(concurrency=local) if generation == 7 else None
        ),
        mark_unreachable=lambda: calls.append("unreachable"),
    )
    facade = RecoveryConcurrencySemaphore(normal, coordinator)
    handle = ConcurrencyHandle(
        uuid4(),
        Backend.REDIS if kind == "redis" else Backend.LOCAL,
        owner,
        None if kind == "redis" else 7 if kind != "missing" else 8,
    )
    if kind == "unknown":
        handle = uuid4()
    result = await facade.renew_guarded([], handle, guard=Guard())
    if kind in ("unknown", "missing"):
        assert result.disposition is Disposition.NOT_OWNED and calls == []
    else:
        assert calls[0] == (normal if kind == "redis" else local, handle.raw_lease_id)
        assert ("unreachable" in calls) == (kind == "redis")


def test_invalid_outcome_cannot_fabricate_a_backend_result():
    with pytest.raises(ValueError):
        GuardedRenewalOutcome(Disposition.SUPPRESSED, RenewResult(False))
    with pytest.raises(ValueError):
        GuardedRenewalOutcome(Disposition.BACKEND_RESULT, RenewResult(True), 2)
    with pytest.raises(ValueError):
        RenewalAttempt(2, "REDIS", Status.RESOLVED_ERROR)


@pytest.mark.asyncio
async def test_second_noscript_is_bounded_and_preserves_both_attempts():
    semaphore, connection, scripts, _ = setup()
    connection.replies = [NoScriptError("first"), NoScriptError("second")]
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.INTERNAL_ERROR
    assert [a.status for a in outcome.attempts] == [Status.NOSCRIPT, Status.NOSCRIPT]
    assert scripts.calls == len(connection._writer.writes) == 2
    assert outcome.result is None


@pytest.mark.parametrize(
    "failure", [ConnectionError("secret"), AuthenticationError("secret")]
)
@pytest.mark.asyncio
async def test_preparation_failure_before_first_write(failure):
    semaphore, connection, scripts, _ = setup()
    scripts.failure = failure
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is (
        Disposition.UNKNOWN
        if type(failure) is ConnectionError
        else Disposition.INTERNAL_ERROR
    )
    assert not outcome.attempts and not connection._writer.writes
    assert "secret" not in repr(outcome)


@pytest.mark.parametrize("reply", [True, "1", None, [1], 2])
@pytest.mark.asyncio
async def test_malformed_reply_is_internal_with_unresolved_execution(reply):
    semaphore, connection, _, _ = setup()
    connection.replies = [reply]
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.INTERNAL_ERROR
    assert outcome.failure_category is RenewalFailure.PROTOCOL_ERROR
    assert outcome.attempts[0].status is Status.UNRESOLVED
    assert connection.disconnects == 1


@pytest.mark.asyncio
async def test_guard_fault_after_noscript_preserves_timestamp():
    semaphore, connection, scripts, _ = setup()
    connection.replies = [NoScriptError("missing")]
    guard = Guard()
    original = scripts.script_load

    async def load(script):
        if scripts.calls:
            guard.error = ValueError("secret")
        return await original(script)

    scripts.script_load = load
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    assert outcome.disposition is Disposition.GUARD_ERROR
    assert outcome.attempts[0].status is Status.NOSCRIPT
    assert outcome.attempts[0].dispatched_at == 2
    assert len(connection._writer.writes) == 1
    assert "secret" not in repr(outcome)


@pytest.mark.asyncio
async def test_write_oserror_is_sanitized_unknown_without_retransmission():
    semaphore, connection, _, _ = setup()
    connection._writer.error = OSError("secret")
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.UNKNOWN
    assert outcome.attempts[0].status is Status.UNRESOLVED
    assert len(connection._writer.writes) == connection.disconnects == 1
    assert "secret" not in repr(outcome)


@pytest.mark.asyncio
async def test_guard_cancellation_is_not_a_guard_error():
    semaphore, connection, _, _ = setup()
    guard = Guard()
    guard.error = asyncio.CancelledError()
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    )
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.guarded_renewal_operation.cancelled
    assert task.guarded_renewal_operation.attempts == ()
    assert not connection._writer.writes


@pytest.mark.asyncio
async def test_runtime_guarded_pool_is_bounded_same_identity_and_closed():
    runtime = RedisRuntime("redis://localhost:6379/3", 1)
    await runtime.start()
    transport = runtime.guarded_renewal_transport
    assert runtime.guarded_renewal_transport is transport
    assert transport.pool.max_connections == 16
    assert transport.pool.connection_kwargs["db"] == 3
    assert transport.pool.connection_kwargs["retry"].get_retries() == 0
    closed = []

    async def close(**kwargs):
        closed.append(True)

    transport.close = close
    await runtime.close()
    assert closed == [True]
    with pytest.raises(RuntimeError, match="closed"):
        _ = runtime.guarded_renewal_transport


@pytest.mark.asyncio
async def test_script_lock_contention_has_no_early_dispatch_anchor():
    semaphore, connection, _, _ = setup()
    guard = Guard()
    await semaphore._script_lock.acquire()
    entered = asyncio.Event()
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    )
    asyncio.get_running_loop().call_soon(entered.set)
    await entered.wait()
    guard.now = 3
    semaphore._script_lock.release()
    outcome = await task
    assert outcome.dispatched_at == 3
    assert outcome.attempts[0].dispatched_at == 3
    assert guard.calls == [3] and len(connection._writer.writes) == 1


@pytest.mark.asyncio
async def test_local_generation_retention_only_after_confirmed_success():
    notes = []
    lease = uuid4()
    owner = uuid4()
    attempt = RenewalAttempt(2, "LOCAL", Status.RESULT, RenewResult(True))
    success = GuardedRenewalOutcome(
        Disposition.BACKEND_RESULT, RenewResult(True), 2, (attempt,)
    )

    class Local:
        outcome = success

        async def renew_guarded(self, policies, raw_lease, *, guard):
            assert raw_lease == lease
            return self.outcome

    local = Local()
    coordinator = SimpleNamespace(
        owner_id=owner,
        clock=lambda: 10,
        lease_duration_seconds=5,
        local_generation=lambda generation: (
            SimpleNamespace(concurrency=local) if generation == 7 else None
        ),
        note_local_completion=lambda *args: notes.append(args),
    )
    facade = RecoveryConcurrencySemaphore(object(), coordinator)
    handle = ConcurrencyHandle(lease, Backend.LOCAL, owner, 7)
    assert await facade.renew_guarded([], handle, guard=Guard()) is success
    assert notes == [(7, 15, lease)]
    local.outcome = GuardedRenewalOutcome(Disposition.SUPPRESSED)
    await facade.renew_guarded([], handle, guard=Guard())
    assert len(notes) == 1


@pytest.mark.asyncio
async def test_local_clock_fault_never_becomes_redis_unknown():
    def broken_clock():
        raise ConnectionError("synthetic local fault")

    store = LocalConcurrencyStore(
        8, broken_clock, 5000, SimpleNamespace(record=lambda *a: None)
    )
    outcome = await store.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.INTERNAL_ERROR
    assert outcome.attempts == ()


def test_outcome_rejects_impossible_suppression_and_dependency_histories():
    missing = RenewalAttempt(2, "REDIS", Status.NOSCRIPT)
    with pytest.raises(ValueError):
        GuardedRenewalOutcome(Disposition.SUPPRESSED, attempts=(missing, missing))
    denied = RenewalAttempt(
        2, "REDIS", Status.RESOLVED_ERROR, failure_category=RenewalFailure.REDIS_ERROR
    )
    with pytest.raises(ValueError):
        GuardedRenewalOutcome(Disposition.UNKNOWN, attempts=(denied,))


@pytest.mark.asyncio
async def test_f1_runtime_close_blocks_a_previously_captured_transport():
    semaphore, connection, _, transport = setup()
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime._guarded_renewal_transport = transport
    semaphore._runtime = runtime
    gate = Gate()

    async def load(action, *, force=False, **kwargs):
        await gate.wait()
        return "cached-sha"

    semaphore._load = load
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    await runtime.close()
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert outcome.attempts == () and connection._writer.writes == []


def test_f1_backend_result_cannot_carry_a_guard_fault():
    attempt = RenewalAttempt(2, "REDIS", Status.RESULT, RenewResult(True))
    with pytest.raises(ValueError):
        GuardedRenewalOutcome(
            Disposition.BACKEND_RESULT,
            RenewResult(True),
            2,
            (attempt,),
            RenewalFailure.GUARD_ERROR,
        )


def acceptance_module():
    path = Path(__file__).parents[1] / "integration" / "test_guarded_renewal_runtime.py"
    spec = importlib.util.spec_from_file_location("guarded_redis_acceptance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DockerCLI:
    def __init__(self, creation="timeout_after"):
        self.creation = creation
        self.exists = False
        self.name = None
        self.removed = []
        self.container_id = "a" * 64

    def run(self, command, **kwargs):
        if command[1] == "run":
            self.name = command[command.index("--name") + 1]
            self.exists = self.creation != "error_before"
            if self.creation == "timeout_after":
                raise subprocess.TimeoutExpired("synthetic docker creation", 30)
            if self.creation == "interrupt_after":
                raise KeyboardInterrupt()
            return subprocess.CompletedProcess(
                command, 0 if self.creation == "success" else 1
            )
        if command[1] == "ps":
            value = self.container_id if any("id=" in a for a in command) else self.name
            return subprocess.CompletedProcess(
                command, 0, (value + "\n") if self.exists else "", ""
            )
        if command[1] == "inspect":
            data = {
                "Id": self.container_id,
                "Name": "/" + self.name,
                "Config": {"Labels": {"codex.m4-f4a.owner": self.name}},
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(data), "")
        assert command[1] == "rm"
        self.removed.append(command[-1])
        self.exists = False
        return subprocess.CompletedProcess(command, 0, "", "")


def test_f1_uncertain_container_creation_still_cleans_owned_resource(monkeypatch):
    module = acceptance_module()
    cli = DockerCLI()
    monkeypatch.setattr(module.subprocess, "run", cli.run)
    with pytest.raises(
        (subprocess.TimeoutExpired, RuntimeError, pytest.fail.Exception)
    ):
        next(module.disposable.__wrapped__())
    assert not cli.exists
    assert cli.removed == [cli.container_id]


@pytest.mark.parametrize(
    "creation",
    ["success", "error_after", "timeout_after", "interrupt_after", "error_before"],
)
def test_f1_container_cleanup_handles_every_creation_result(monkeypatch, creation):
    module = acceptance_module()
    cli = DockerCLI(creation)
    monkeypatch.setattr(module.subprocess, "run", cli.run)
    monkeypatch.setattr(
        module.subprocess, "check_output", lambda *a, **k: "127.0.0.1:12345\n"
    )
    monkeypatch.setattr(
        module.socket, "create_connection", lambda *a, **k: nullcontext()
    )
    generator = module.disposable.__wrapped__()
    if creation == "success":
        next(generator)
        generator.close()
    else:
        with pytest.raises(
            (
                subprocess.TimeoutExpired,
                KeyboardInterrupt,
                RuntimeError,
                pytest.fail.Exception,
            )
        ):
            next(generator)
    assert not cli.exists
    assert cli.removed == ([] if creation == "error_before" else [cli.container_id])


@pytest.mark.parametrize("mismatch", ["name", "label", "lookup", "id"])
def test_f1_container_cleanup_rejects_unproven_ownership(monkeypatch, mismatch):
    module = acceptance_module()
    cli = DockerCLI("success")
    cli.name = "m4-f4a-" + "b" * 32
    cli.exists = True
    original = cli.run

    def run(command, **kwargs):
        result = original(command, **kwargs)
        if command[1] == "inspect":
            data = json.loads(result.stdout)
            if mismatch == "name":
                data["Name"] = "/gateway-redis"
            elif mismatch == "label":
                data["Config"]["Labels"] = {}
            elif mismatch == "id":
                data["Id"] = "invalid"
            result.stdout = json.dumps(data)
        elif command[1] == "ps" and mismatch == "lookup":
            result.stdout = "gateway-redis\n"
        return result

    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(RuntimeError):
        module.cleanup_owned_container(cli.name)
    assert cli.exists and not cli.removed


def test_f1_cleanup_failure_is_reported_without_false_success(monkeypatch):
    module = acceptance_module()
    cli = DockerCLI("success")
    cli.name = "m4-f4a-" + "c" * 32
    cli.exists = True
    original = cli.run

    def run(command, **kwargs):
        if command[1] == "rm":
            return subprocess.CompletedProcess(command, 1, "", "synthetic failure")
        return original(command, **kwargs)

    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="removal"):
        module.cleanup_owned_container(cli.name)
    assert cli.exists
    with pytest.raises(RuntimeError, match="not test-owned"):
        module.cleanup_owned_container("gateway-redis")


@pytest.mark.parametrize("stage", ["checkout", "connection", "health", "send"])
@pytest.mark.asyncio
async def test_f1_close_during_preparation_prevents_writer(stage, monkeypatch):
    semaphore, connection, _, transport = setup()
    guard, gate = Guard(), Gate()
    if stage == "checkout":
        transport.pool.checkout = gate
    elif stage == "connection":
        connection.preparation = gate
    elif stage == "health":
        connection.health = gate
    else:
        from app.redis import guarded_renewal

        class SchedulingBoundary:
            def __getattr__(self, name):
                return getattr(asyncio, name)

            async def wait_for(self, coroutine, timeout):
                await gate.wait()
                return await asyncio.wait_for(coroutine, timeout)

        monkeypatch.setattr(guarded_renewal, "asyncio", SchedulingBoundary())
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=guard)
    )
    await bounded(gate.entered)
    await transport.close()
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert not connection._writer.writes and not outcome.attempts
    assert not connection.connected


@pytest.mark.asyncio
async def test_f1_close_during_noscript_reload_preserves_first_attempt():
    semaphore, connection, scripts, transport = setup()
    connection.replies = [NoScriptError("missing")]
    gate = Gate()
    original = scripts.script_load

    async def load(script):
        if scripts.calls:
            await gate.wait()
        return await original(script)

    scripts.script_load = load
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    await transport.close()
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.SUPPRESSED
    assert [a.status for a in outcome.attempts] == [Status.NOSCRIPT]
    assert outcome.attempts[0].dispatched_at == 2
    assert len(connection._writer.writes) == 1 and outcome.result is None


@pytest.mark.asyncio
async def test_f1_close_after_writer_preserves_uncertainty():
    semaphore, connection, _, transport = setup()
    gate = connection._writer.gate = Gate()
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    assert len(connection._writer.writes) == 1
    await transport.close()
    gate.resume.set()
    outcome = await task
    assert outcome.disposition is Disposition.UNKNOWN
    assert [a.status for a in outcome.attempts] == [Status.UNRESOLVED]
    assert outcome.attempts[0].dispatched_at == 2 and outcome.result is None


@pytest.mark.asyncio
async def test_f1_authoritative_reply_is_retained_if_closure_follows_send():
    semaphore, connection, _, transport = setup()
    gate = connection.reply_gate = Gate()
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    await transport.close()
    gate.resume.set()
    outcome = await task
    assert outcome.result == RenewResult(True)
    assert outcome.dispatched_at == 2 and len(outcome.attempts) == 1
    assert not connection.connected


@pytest.mark.asyncio
async def test_f1_close_is_single_owned_idempotent_disposal():
    _, _, _, transport = setup()
    gate, calls = Gate(), []

    async def disconnect():
        calls.append(True)
        await gate.wait()

    transport.pool.disconnect = disconnect
    first = asyncio.create_task(transport.close())
    await bounded(gate.entered)
    second = asyncio.create_task(transport.close())
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert transport.closed
    gate.resume.set()
    await second
    await transport.close()
    assert calls == [True] and transport._close_task.done()


@pytest.mark.asyncio
async def test_f1_runtime_lazy_pool_cannot_resurrect_after_close():
    runtime = RedisRuntime("redis://localhost:6379/3", 1)
    await runtime.start()
    await runtime.close()
    with pytest.raises(RuntimeError, match="closed"):
        _ = runtime.guarded_renewal_transport
    assert runtime._guarded_renewal_transport is None
    await runtime.close()


@pytest.mark.asyncio
async def test_f1_transport_rejects_another_event_loop():
    _, _, _, transport = setup()

    def close_in_other_loop():
        with pytest.raises(RuntimeError, match="another event loop"):
            asyncio.run(transport.close())

    await asyncio.to_thread(close_in_other_loop)
    assert not transport.closed
    await transport.close()


@pytest.mark.asyncio
async def test_f1_cancellation_after_closure_preserves_attempted_send():
    semaphore, connection, _, transport = setup()
    gate = connection._writer.gate = Gate()
    task = asyncio.create_task(
        semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    )
    await bounded(gate.entered)
    await transport.close()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    evidence = task.guarded_renewal_operation
    assert evidence.cancelled and evidence.outcome is None
    assert [a.status for a in evidence.attempts] == [Status.UNRESOLVED]
    assert evidence.attempts[0].dispatched_at == 2
    assert len(connection._writer.writes) == 1 and not connection.connected


@pytest.mark.asyncio
async def test_f1_closed_exception_after_writer_is_never_unsent():
    semaphore, connection, _, _ = setup()
    connection._writer.error = GuardedTransportClosed()
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.UNKNOWN
    assert [a.status for a in outcome.attempts] == [Status.UNRESOLVED]
    assert len(connection._writer.writes) == 1


@pytest.mark.asyncio
async def test_f1_repeated_runtime_close_retains_sanitized_failure(caplog):
    _, _, _, transport = setup()
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime._guarded_renewal_transport = transport
    calls = []

    async def disconnect():
        calls.append(True)
        raise ConnectionError("synthetic-secret")

    transport.pool.disconnect = disconnect
    from app.redis.runtime import RedisDisposalError

    for _ in range(2):
        with pytest.raises(RedisDisposalError) as failure:
            await runtime.close()
        assert failure.value.resources == ("guarded",)
        assert "synthetic-secret" not in str(failure.value)
    assert transport.closed and calls == [True]
    assert transport._close_task.done()
    assert "synthetic-secret" not in caplog.text


@pytest.mark.asyncio
async def test_f2_supervisor_retains_failed_redis_and_closes_other_resource():
    from app.normal_api.supervisor import LifecycleSupervisor

    _, connection, _, transport = setup()
    connection.connected = True
    calls = []

    async def legacy_close(**kwargs):
        calls.append("legacy")

    async def failed_disposal():
        calls.append("guarded")
        raise ConnectionError("synthetic-secret")

    async def other_close():
        calls.append("other")

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy_close
    runtime._guarded_renewal_transport = transport
    transport.pool.disconnect = failed_disposal
    supervisor = LifecycleSupervisor(30)
    await supervisor.close_resources([("redis", runtime.close), ("other", other_close)])
    assert calls == ["guarded", "legacy", "other"]
    assert connection.connected
    assert supervisor.unclosed_resources == {"redis"}
    assert supervisor.resources["redis"].exception() is not None
    await connection.disconnect()


def test_f2_result_attempt_rejects_failure_category():
    with pytest.raises(ValueError):
        RenewalAttempt(
            2, "REDIS", Status.RESULT, RenewResult(True), RenewalFailure.GUARD_ERROR
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "guarded_fails,legacy_fails", [(False, False), (False, True), (True, True)]
)
async def test_f2_all_disposal_obligations(guarded_fails, legacy_fails):
    from app.normal_api.supervisor import LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError

    _, connection, _, transport = setup()
    calls = []

    async def disconnect():
        calls.append("guarded")
        if guarded_fails:
            raise ConnectionError("synthetic-guarded-secret")
        await connection.disconnect()

    async def legacy_close(**kwargs):
        calls.append("legacy")
        if legacy_fails:
            raise ConnectionError("synthetic-legacy-secret")

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy_close
    runtime._guarded_renewal_transport = transport
    transport.pool.disconnect = disconnect
    supervisor = LifecycleSupervisor(30)
    await supervisor.close_resources([("redis", runtime.close)])
    failures = tuple(
        name
        for name, failed in [("guarded", guarded_fails), ("legacy", legacy_fails)]
        if failed
    )
    for _ in range(2):
        if failures:
            with pytest.raises(RedisDisposalError) as failure:
                await runtime.close()
            assert failure.value.resources == failures
            assert failure.value.__context__ is None
            assert "secret" not in str(failure.value)
        else:
            await runtime.close()
    assert calls == ["guarded", "legacy"]
    assert supervisor.unclosed_resources == ({"redis"} if failures else set())


@pytest.mark.asyncio
@pytest.mark.parametrize("cutoff", [False, True])
@pytest.mark.parametrize("late_failure", [False, True])
async def test_f2_cancelled_waiter_retains_shared_disposal(cutoff, late_failure):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    semaphore, connection, _, transport = setup()
    gate = Gate()
    calls = []

    async def disconnect():
        calls.append("guarded")
        await gate.wait()
        if late_failure:
            raise ConnectionError("synthetic-late-secret")
        await connection.disconnect()

    async def legacy_close(**kwargs):
        calls.append("legacy")

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy_close
    runtime._guarded_renewal_transport = transport
    semaphore._runtime = runtime
    transport.pool.disconnect = disconnect
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    cleanup_scope = LifecycleScope(supervisor)

    async def managed_close():
        await runtime.close(cleanup_scope=cleanup_scope)

    waiter = asyncio.create_task(
        supervisor.close_resources([("redis", managed_close)])
        if cutoff
        else runtime.close()
    )
    await bounded(gate.entered)
    concurrent = asyncio.create_task(runtime.close())
    if cutoff:
        await clock.expect(30)
        clock.advance(30)
        await asyncio.wait_for(waiter, 3)
        # The resource wrapper is cancelled at the R1 cutoff, not the owned task.
        await asyncio.gather(supervisor.resources["redis"], return_exceptions=True)
        assert supervisor.unclosed_resources == {"redis"}
    else:
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    assert not runtime._close_task.done()
    assert not transport._close_task.done()
    outcome = await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())
    assert outcome.disposition is Disposition.SUPPRESSED
    assert connection._writer.writes == []
    gate.resume.set()
    if late_failure or cutoff:
        with pytest.raises(RedisDisposalError) as failure:
            await asyncio.wait_for(concurrent, 3)
        assert failure.value.resources == (
            (("guarded",) if late_failure else ()) + (("legacy",) if cutoff else ())
        )
        with pytest.raises(RedisDisposalError):
            await runtime.close()
    else:
        await asyncio.wait_for(concurrent, 3)
        await runtime.close()
    assert calls == (["guarded"] if cutoff else ["guarded", "legacy"])
    if cutoff:
        assert supervisor.unclosed_resources == {"redis"}
        assert runtime.disposal_states == {
            "guarded": "failed" if late_failure else "completed",
            "legacy": "not_attempted",
        }


@pytest.mark.parametrize("status", [Status.RESULT, Status.NOSCRIPT, Status.UNRESOLVED])
@pytest.mark.parametrize("failure", list(RenewalFailure))
def test_f2_attempt_failure_requires_definitive_error(status, failure):
    with pytest.raises(ValueError):
        RenewalAttempt(
            2,
            "REDIS",
            status,
            RenewResult(True) if status is Status.RESULT else None,
            failure,
        )


@pytest.mark.parametrize("failure", list(RenewalFailure))
def test_f2_resolved_error_classification(failure):
    if failure in (RenewalFailure.REDIS_ERROR, RenewalFailure.REDIS_DEPENDENCY):
        assert (
            RenewalAttempt(
                2, "REDIS", Status.RESOLVED_ERROR, failure_category=failure
            ).failure_category
            is failure
        )
    else:
        with pytest.raises(ValueError):
            RenewalAttempt(2, "REDIS", Status.RESOLVED_ERROR, failure_category=failure)


def test_f2_valid_result_and_noscript_evidence():
    assert RenewalAttempt(2, "REDIS", Status.RESULT, RenewResult(True)).result.renewed
    assert RenewalAttempt(2, "REDIS", Status.NOSCRIPT).failure_category is None


@pytest.mark.parametrize("status", list(Status))
def test_f2_non_result_objects_rejected_deterministically(status):
    with pytest.raises(ValueError):
        RenewalAttempt(2, "REDIS", status, result="invalid")


@pytest.mark.asyncio
async def test_f3_exact_cutoff_forbids_legacy_stage_after_guarded_completion():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, _, _, transport = setup()
    gate, calls, clock = Gate(), [], Clock()

    async def disconnect():
        calls.append("guarded")
        await gate.wait()

    async def legacy_close(**kwargs):
        calls.append("legacy")

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy_close
    runtime._guarded_renewal_transport = transport
    transport.pool.disconnect = disconnect
    supervisor = LifecycleSupervisor(30, clock=clock)
    cleanup_scope = LifecycleScope(supervisor)

    async def managed_close():
        await runtime.close(cleanup_scope=cleanup_scope)

    waiter = asyncio.create_task(supervisor.close_resources([("redis", managed_close)]))
    await bounded(gate.entered)
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(waiter, 3)
    await asyncio.gather(supervisor.resources["redis"], return_exceptions=True)
    assert calls == ["guarded"] and supervisor.unclosed_resources == {"redis"}
    gate.resume.set()
    await asyncio.gather(runtime._close_task, return_exceptions=True)
    assert calls == ["guarded"]
    assert runtime.disposal_states == {
        "guarded": "completed",
        "legacy": "not_attempted",
    }


@pytest.mark.asyncio
async def test_f3_expired_first_entry_schedules_no_disposal_task():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, _, _, transport = setup()
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime._guarded_renewal_transport = transport
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)
    clock.advance(30)
    for _ in range(2):
        with pytest.raises(RedisDisposalError) as error:
            await runtime.close(cleanup_scope=scope)
        assert error.value.resources == ("guarded", "legacy")
        assert runtime._close_task is None and transport._close_task is None
    assert transport.closed
    assert runtime.disposal_states == {
        "guarded": "not_attempted",
        "legacy": "not_attempted",
    }
    scope.notification.clear()
    with pytest.raises(RedisDisposalError):
        await runtime.close()
    await supervisor.close_resources([("redis", runtime.close)])
    assert supervisor.unclosed_resources == {"redis"}


@pytest.mark.asyncio
async def test_f3_guarded_task_rechecks_cutoff_at_actual_entry():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, connection, _, transport = setup()
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime._guarded_renewal_transport = transport
    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)
    original = transport.request_close

    def schedule(**kwargs):
        original(**kwargs)
        clock.advance(30)

    transport.request_close = schedule
    with pytest.raises(RedisDisposalError) as error:
        await runtime.close(cleanup_scope=scope)
    assert error.value.resources == ("guarded", "legacy")
    assert connection.disconnects == 0
    assert runtime.disposal_states == {
        "guarded": "not_attempted",
        "legacy": "not_attempted",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("cutoff", [False, True])
async def test_f3_legacy_stage_pending_preserves_started_work(cutoff):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, connection, _, transport = setup()
    gate, calls, clock = Gate(), [], Clock()

    async def legacy_close(**kwargs):
        calls.append("legacy")
        await gate.wait()

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy_close
    runtime._guarded_renewal_transport = transport
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)

    async def managed_close():
        await runtime.close(cleanup_scope=scope)

    waiter = asyncio.create_task(supervisor.close_resources([("redis", managed_close)]))
    await bounded(gate.entered)
    assert runtime.disposal_states == {"guarded": "completed", "legacy": "pending"}
    assert connection.disconnects == 1
    if cutoff:
        await clock.expect(30)
        clock.advance(30)
        await asyncio.wait_for(waiter, 3)
        await asyncio.gather(supervisor.resources["redis"], return_exceptions=True)
        assert supervisor.unclosed_resources == {"redis"}
    else:
        supervisor.resources["redis"].cancel()
        await asyncio.wait_for(waiter, 3)
    assert not runtime._close_task.done()
    gate.resume.set()
    await asyncio.wait_for(runtime._close_task, 3)
    await runtime.close()
    assert calls == ["legacy"]
    assert runtime.disposal_states == {"guarded": "completed", "legacy": "completed"}
    assert supervisor.unclosed_resources == {"redis"}


@pytest.mark.asyncio
@pytest.mark.parametrize("guarded_failure", [False, True])
async def test_f3_eligible_transition_uses_same_deadline(guarded_failure):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, _, _, transport = setup()
    calls, clock = [], Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)

    async def disconnect():
        calls.append("guarded")
        clock.advance(29.999)
        if guarded_failure:
            raise ConnectionError("synthetic-secret")

    async def legacy(**kwargs):
        assert scope.deadline == 30 and not scope.expired
        calls.append("legacy")

    async def other():
        calls.append("other")

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime.client.aclose = legacy
    runtime._guarded_renewal_transport = transport
    transport.pool.disconnect = disconnect

    async def close():
        await runtime.close(cleanup_scope=scope)

    await supervisor.close_resources([("redis", close), ("other", other)])
    assert calls == ["guarded", "legacy", "other"]
    assert supervisor.unclosed_resources == ({"redis"} if guarded_failure else set())
    assert runtime.disposal_states == {
        "guarded": "failed" if guarded_failure else "completed",
        "legacy": "completed",
    }
    assert supervisor.process_deadline == 30


@pytest.mark.asyncio
async def test_f3_runtime_task_handoff_cannot_start_expired_stage():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, connection, _, transport = setup()
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    runtime._guarded_renewal_transport = transport
    clock, gate = Clock(), Gate()
    supervisor = LifecycleSupervisor(30, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)
    original = runtime._dispose

    async def queued():
        await gate.wait()
        await original()

    runtime._dispose = queued
    waiter = asyncio.create_task(runtime.close(cleanup_scope=scope))
    await bounded(gate.entered)
    clock.advance(30)
    gate.resume.set()
    with pytest.raises(RedisDisposalError):
        await waiter
    assert connection.disconnects == 0 and transport._close_task is None
    assert runtime.disposal_states == {
        "guarded": "not_attempted",
        "legacy": "not_attempted",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("entry_time", [29.999, 30, 30.001])
@pytest.mark.parametrize("membership", ["available", "in_use"])
async def test_f4_actual_pool_nested_entry_rejects_exact_cutoff(
    backend, entry_time, membership
):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.make_connection()
    calls = []

    async def record_disconnect():
        calls.append(clock.now())

    connection.disconnect = record_disconnect
    if membership == "available":
        pool._available_connections.append(connection)
    else:
        pool._in_use_connections.add(connection)
    loop = asyncio.get_running_loop()
    original = loop.get_task_factory()

    def factory(loop, coroutine, **kwargs):
        if any(
            frame.function == "disconnect"
            and frame.filename.replace("\\", "/").endswith(
                "redis/asyncio/connection.py"
            )
            for frame in inspect.stack()
        ):
            loop.call_soon(clock.advance, entry_time)
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    async def close():
        await runtime.close(cleanup_scope=scope)

    loop.set_task_factory(factory)
    try:
        await supervisor.close_resources([("redis", close)])
    finally:
        loop.set_task_factory(original)
    await asyncio.wait_for(
        asyncio.gather(runtime._close_task, return_exceptions=True), 3
    )
    assert calls == ([entry_time] if entry_time < 30 else [])
    assert supervisor.unclosed_resources == (set() if entry_time < 30 else {"redis"})
    adapter = (
        runtime._guarded_renewal_transport._pool_disposal
        if backend == "guarded"
        else runtime._legacy_pool_disposal
    )
    assert list(adapter.states.values()) == (
        ["completed"] if entry_time < 30 else ["skipped"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("second", ["skip", "fail"])
async def test_f4_actual_pool_partial_completion_retains_all_obligations(
    backend, second
):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    clock = Clock()
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    calls = []
    for index in range(2):
        connection = pool.make_connection()

        async def disconnect(index=index):
            calls.append(index)
            if index == 0 and second == "skip":
                clock.advance(30)
            if index == 1 and second == "fail":
                raise ConnectionError("synthetic-secret")

        connection.disconnect = disconnect
        pool._available_connections.append(connection)

    async def close():
        await runtime.close(cleanup_scope=scope)

    await supervisor.close_resources([("redis", close)])
    await asyncio.gather(runtime._close_task, return_exceptions=True)
    adapter = (
        runtime._guarded_renewal_transport._pool_disposal
        if backend == "guarded"
        else runtime._legacy_pool_disposal
    )
    assert list(adapter.states.values()) == [
        "completed",
        "skipped" if second == "skip" else "failed",
    ]
    assert supervisor.unclosed_resources == {"redis"}
    for _ in range(2):
        with pytest.raises(RedisDisposalError) as error:
            await runtime.close()
        assert "secret" not in str(error.value)
    assert calls == ([0] if second == "skip" else [0, 1])


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f4_cancelled_nested_task_before_entry_is_not_disposal(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    supervisor = LifecycleSupervisor(30, clock=Clock())
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection, calls = pool.make_connection(), []

    async def disconnect():
        calls.append(True)

    connection.disconnect = disconnect
    pool._available_connections.append(connection)
    loop, old = (
        asyncio.get_running_loop(),
        asyncio.get_running_loop().get_task_factory(),
    )

    def factory(loop, coroutine, **kwargs):
        task = asyncio.Task(coroutine, loop=loop, **kwargs)
        if any(
            frame.function == "disconnect"
            and frame.filename.replace("\\", "/").endswith(
                "redis/asyncio/connection.py"
            )
            for frame in inspect.stack()
        ):
            task.cancel()
        return task

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    loop.set_task_factory(factory)
    try:
        await supervisor.close_resources([("redis", close)])
    finally:
        loop.set_task_factory(old)
    assert calls == [] and supervisor.unclosed_resources == {"redis"}
    adapter = (
        runtime._guarded_renewal_transport._pool_disposal
        if backend == "guarded"
        else runtime._legacy_pool_disposal
    )
    assert list(adapter.states.values()) == ["not_attempted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f4_nested_pending_cutoff_and_caller_cancellation(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    clock, gate, calls = Clock(), Gate(), []
    supervisor = LifecycleSupervisor(30, clock=clock)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.make_connection()

    async def disconnect():
        calls.append(True)
        await gate.wait()

    connection.disconnect = disconnect
    pool._available_connections.append(connection)
    scope = LifecycleScope(supervisor)

    async def close():
        await runtime.close(cleanup_scope=scope)

    waiter = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(gate.entered)
    concurrent = asyncio.create_task(runtime.close())
    concurrent.cancel()
    with pytest.raises(asyncio.CancelledError):
        await concurrent
    adapter = (
        runtime._guarded_renewal_transport._pool_disposal
        if backend == "guarded"
        else runtime._legacy_pool_disposal
    )
    assert list(adapter.states.values()) == ["pending"]
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(waiter, 3)
    await asyncio.gather(supervisor.resources["redis"], return_exceptions=True)
    assert not runtime._close_task.done()
    assert not adapter.tasks[0].done()
    assert supervisor.unclosed_resources == {"redis"}
    gate.resume.set()
    await asyncio.wait_for(
        asyncio.gather(runtime._close_task, return_exceptions=True), 3
    )
    assert list(adapter.states.values()) == ["completed"] and calls == [True]
    if backend == "guarded":
        assert runtime.disposal_states["legacy"] == "not_attempted"
        with pytest.raises(RedisDisposalError):
            await runtime.close()
    else:
        await runtime.close()
    assert supervisor.unclosed_resources == {"redis"}


@pytest.mark.asyncio
@pytest.mark.parametrize("waiter_state", ["pending", "error", "cancelled", "closed"])
async def test_f4_disconnect_return_does_not_fabricate_writer_confirmation(
    waiter_state,
):
    from redis.asyncio.connection import ConnectionPool

    from app.redis.guarded_renewal import ManagedPoolDisposal, ManagedPoolDisposalError

    pool = ConnectionPool()
    connection = pool.make_connection()
    waiter = asyncio.get_running_loop().create_future()
    if waiter_state == "error":
        waiter.set_exception(OSError("synthetic-secret"))
    elif waiter_state == "cancelled":
        waiter.cancel()
    elif waiter_state == "closed":
        waiter.set_result(None)
    connection._writer = SimpleNamespace(
        _protocol=SimpleNamespace(_get_close_waiter=lambda writer: waiter)
    )

    async def disconnect():
        # Model redis-py's swallowed OSError/detached writer path.
        connection._writer = None

    connection.disconnect = disconnect
    pool._available_connections.append(connection)
    adapter = ManagedPoolDisposal(pool, lambda: True)
    if waiter_state == "closed":
        await pool.disconnect()
        assert adapter.states == {0: "completed"}
    else:
        with pytest.raises(ManagedPoolDisposalError) as error:
            await pool.disconnect()
        assert "secret" not in str(error.value)
        assert adapter.states == {0: "unresolved"}
    if not waiter.done():
        waiter.cancel()


@pytest.mark.asyncio
async def test_f4_legacy_hook_completion_cannot_start_late_pool_disconnect():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    clock, gate, calls = Clock(), Gate(), []
    supervisor = LifecycleSupervisor(30, clock=clock)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    connection = pool.make_connection()

    async def hook():
        await gate.wait()

    async def disconnect():
        calls.append(True)

    pool._on_close = hook
    connection.disconnect = disconnect
    pool._available_connections.append(connection)
    scope = LifecycleScope(supervisor)

    async def close():
        await runtime.close(cleanup_scope=scope)

    waiter = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(gate.entered)
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(waiter, 3)
    gate.resume.set()
    await asyncio.wait_for(
        asyncio.gather(runtime._close_task, return_exceptions=True), 3
    )
    assert calls == []
    assert runtime._legacy_pool_disposal.states == {0: "not_attempted"}
    assert supervisor.unclosed_resources == {"redis"}


@pytest.mark.asyncio
async def test_f5_legacy_allocation_during_disposal_is_fenced():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    old = pool.get_available_connection()
    gate = Gate()

    async def disconnect():
        await gate.wait()

    old.disconnect = disconnect
    supervisor = LifecycleSupervisor(30, clock=Clock())
    scope = LifecycleScope(supervisor)

    async def close():
        await runtime.close(cleanup_scope=scope)

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(gate.entered)
    try:
        with pytest.raises(GuardedTransportClosed):
            pool.get_available_connection()
    finally:
        gate.resume.set()
        await asyncio.wait_for(closing, 3)


@pytest.mark.asyncio
async def test_f5_pending_guarded_establishment_cannot_report_confirmed_close(
    monkeypatch,
):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    transport = runtime.guarded_renewal_transport
    connection = transport.pool.get_available_connection()
    gate, closed = Gate(), []
    waiter = asyncio.get_running_loop().create_future()

    def close_writer():
        closed.append(True)
        waiter.set_result(None)

    writer = SimpleNamespace(
        close=close_writer,
        wait_closed=lambda: waiter,
        _protocol=SimpleNamespace(_get_close_waiter=lambda w: waiter),
    )

    async def establishing(self):
        await gate.wait()
        self._reader, self._writer = SimpleNamespace(), writer

    monkeypatch.setattr(RedisConnection, "_connect", establishing)
    connecting = asyncio.create_task(connection._connect())
    await bounded(gate.entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())
    scope = LifecycleScope(supervisor)

    async def close():
        await runtime.close(cleanup_scope=scope)

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(transport.ownership.closed_admission)
    assert transport.ownership.inflight
    assert not runtime._close_task.done()
    gate.resume.set()
    await asyncio.gather(connecting, return_exceptions=True)
    await asyncio.wait_for(closing, 3)
    try:
        assert not connection.is_connected
        assert closed == [True]
    finally:
        connection._reader = connection._writer = None


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f5_checkout_lock_waiter_cannot_slip_past_fence(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    owner = pool._gateway_managed_owner
    await pool._lock.acquire()
    entered = asyncio.Event()

    async def checkout():
        entered.set()
        return await pool.get_connection()

    checking = asyncio.create_task(checkout())
    await bounded(entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(owner.closed_admission)
    try:
        assert owner.inflight and not owner.connections
        for action in (pool.get_available_connection, pool.make_connection):
            with pytest.raises(GuardedTransportClosed):
                action()
        with pytest.raises(GuardedTransportClosed):
            await pool.get_connection()
    finally:
        pool._lock.release()
    with pytest.raises(GuardedTransportClosed):
        await checking
    await asyncio.wait_for(closing, 3)
    assert not owner.inflight and not supervisor.unclosed_resources


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("finish", ["success", "failure", "cancel", "cutoff"])
async def test_f5_real_checkout_establishment_remains_owned(
    monkeypatch, backend, finish
):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    gate, writes, closed, clock = Gate(), [], [], Clock()
    waiter = asyncio.get_running_loop().create_future()

    def close_writer():
        closed.append(True)
        if not waiter.done():
            waiter.set_result(None)

    writer = SimpleNamespace(
        close=close_writer,
        wait_closed=lambda: waiter,
        _protocol=SimpleNamespace(_get_close_waiter=lambda w: waiter),
    )

    async def establish(self):
        await gate.wait()
        if finish == "failure":
            raise ValueError("synthetic establishment failure")
        writes.append(True)
        self._reader, self._writer = SimpleNamespace(), writer

    monkeypatch.setattr(RedisConnection, "_connect", establish)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    owner = pool._gateway_managed_owner
    checking = asyncio.create_task(pool.get_connection())
    await bounded(gate.entered)
    assert len(owner.connections) == 1 and owner.inflight
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)

    async def close():
        await runtime.close(cleanup_scope=scope)

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(owner.closed_admission)
    assert not runtime._close_task.done()
    if finish == "cutoff":
        await clock.expect(30)
        clock.advance(30)
        await asyncio.wait_for(closing, 3)
        await asyncio.gather(runtime._close_task, return_exceptions=True)
        assert owner.inflight and supervisor.unclosed_resources == {"redis"}
    if finish == "cancel":
        checking.cancel()
    else:
        gate.resume.set()
    result = (await asyncio.gather(checking, return_exceptions=True))[0]
    assert isinstance(result, BaseException)
    if finish != "cutoff":
        await asyncio.wait_for(closing, 3)
        assert not supervisor.unclosed_resources and not owner.inflight
        assert closed == ([True] if finish == "success" else [])
    else:
        assert closed == []
        with pytest.raises(RedisDisposalError):
            await runtime.close()
    for connection in owner.connections:
        if finish != "cutoff":
            assert not connection.is_connected
        for action in (
            connection.connect,
            connection.connect_check_health,
            connection._connect,
        ):
            with pytest.raises(GuardedTransportClosed):
                await action()
        connection._reader = connection._writer = None
    assert len(writes) == (1 if finish in ("success", "cutoff") else 0)
    assert not owner.inflight


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f5_multiple_establishments_complete_in_reverse_order(
    monkeypatch, backend
):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    gates, closed, connections = [Gate(), Gate()], [], []

    async def establish(self):
        index = connections.index(self)
        await gates[index].wait()
        future = asyncio.get_running_loop().create_future()

        def close_writer():
            closed.append(index)
            future.set_result(None)

        self._reader = SimpleNamespace()
        self._writer = SimpleNamespace(
            close=close_writer,
            wait_closed=lambda: future,
            _protocol=SimpleNamespace(_get_close_waiter=lambda w: future),
        )

    monkeypatch.setattr(RedisConnection, "_connect", establish)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    owner = pool._gateway_managed_owner
    for _ in range(2):
        connections.append(pool.get_available_connection())
    tasks = [asyncio.create_task(connection._connect()) for connection in connections]
    for gate in gates:
        await bounded(gate.entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(owner.closed_admission)
    gates[1].resume.set()
    await asyncio.gather(tasks[1], return_exceptions=True)
    assert owner.inflight and not runtime._close_task.done()
    gates[0].resume.set()
    await asyncio.gather(tasks[0], return_exceptions=True)
    await asyncio.wait_for(closing, 3)
    assert sorted(closed) == [0, 1]
    assert all(not c.is_connected for c in connections)
    assert not supervisor.unclosed_resources and not owner.inflight
    for connection in connections:
        await pool.release(connection)
    assert set(owner.connections) == set(connections)
    await runtime.close()
    assert sorted(closed) == [0, 1]


@pytest.mark.asyncio
async def test_f5_request_scope_and_other_pool_cannot_change_process_fence():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    first, second = (
        RedisRuntime("redis://localhost:6379/3", 2),
        RedisRuntime("redis://localhost:6379/3", 2),
    )
    await first.start()
    await second.start()
    supervisor = LifecycleSupervisor(30, clock=Clock())
    request = supervisor.register()
    request.begin_cleanup()
    with pytest.raises(RuntimeError, match="process authority"):
        await first.close(cleanup_scope=request)
    assert first._cleanup_scope is None
    assert not first.client.connection_pool._gateway_managed_owner.closing
    supervisor.begin_shutdown()
    await first.close(cleanup_scope=LifecycleScope(supervisor))
    assert not second.client.connection_pool._gateway_managed_owner.closing
    connection = second.client.connection_pool.get_available_connection()
    await second.client.connection_pool.release(connection)
    await second.close()


@pytest.mark.asyncio
async def test_f5_foreign_connection_cannot_disappear_from_accounting():
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    foreign = RedisConnection()
    pool._available_connections.append(foreign)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    assert supervisor.unclosed_resources == {"redis"}
    assert runtime.disposal_states == {"legacy": "failed"}


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f5_preexisting_disconnect_is_owned_without_duplicate(
    monkeypatch, backend
):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    gate, calls = Gate(), []
    original = RedisConnection.disconnect

    async def disconnect(self, *args, **kwargs):
        calls.append(True)
        await gate.wait()
        await original(self, *args, **kwargs)

    monkeypatch.setattr(RedisConnection, "disconnect", disconnect)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    owner = pool._gateway_managed_owner
    existing = asyncio.create_task(connection.disconnect())
    await bounded(gate.entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(owner.closed_admission)
    assert owner.inflight and not closing.done()
    gate.resume.set()
    await existing
    await asyncio.wait_for(closing, 3)
    assert calls == [True]
    assert owner.last_cleanup[connection] == "completed"
    assert not supervisor.unclosed_resources


@pytest.mark.asyncio
async def test_f5_late_managed_authority_cannot_certify_unmanaged_close(monkeypatch):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    gate = Gate()
    original = RedisConnection.disconnect

    async def disconnect(self, *args, **kwargs):
        await gate.wait()
        await original(self, *args, **kwargs)

    monkeypatch.setattr(RedisConnection, "disconnect", disconnect)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    pool.get_available_connection()
    first = asyncio.create_task(runtime.close())
    await bounded(gate.entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    managed = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(pool._gateway_managed_owner.closed_admission)
    with pytest.raises(GuardedTransportClosed):
        pool.make_connection()
    gate.resume.set()
    with pytest.raises(RedisDisposalError):
        await first
    await managed
    assert supervisor.unclosed_resources == {"redis"}
    assert runtime.disposal_states == {"legacy": "unresolved"}
    with pytest.raises(RedisDisposalError):
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f5_cancel_request_is_not_establishment_termination(monkeypatch, backend):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    entered, cancelled, resume = asyncio.Event(), asyncio.Event(), asyncio.Event()
    future = asyncio.get_running_loop().create_future()
    closed = []

    def close_writer():
        closed.append(True)
        future.set_result(None)

    async def establish(self):
        entered.set()
        try:
            await resume.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await resume.wait()
        self._reader = SimpleNamespace()
        self._writer = SimpleNamespace(
            close=close_writer,
            wait_closed=lambda: future,
            _protocol=SimpleNamespace(_get_close_waiter=lambda w: future),
        )

    monkeypatch.setattr(RedisConnection, "_connect", establish)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    owner = pool._gateway_managed_owner
    connecting = asyncio.create_task(connection._connect())
    await bounded(entered)
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(owner.closed_admission)
    connecting.cancel()
    await bounded(cancelled)
    assert not connecting.done() and owner.inflight
    assert not closing.done() and not runtime._close_task.done()
    resume.set()
    await asyncio.gather(connecting, return_exceptions=True)
    await asyncio.wait_for(closing, 3)
    assert closed == [True]
    assert not connection.is_connected and not owner.inflight
    assert not supervisor.unclosed_resources


@pytest.mark.asyncio
async def test_f5_abandoned_late_establishment_exception_is_observed(monkeypatch):
    from redis.asyncio.connection import Connection as RedisConnection

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    gate, clock = Gate(), Clock()

    async def establish(self):
        await gate.wait()
        raise ValueError("synthetic establishment fault")

    monkeypatch.setattr(RedisConnection, "_connect", establish)
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    checking = asyncio.create_task(pool.get_connection())
    await bounded(gate.entered)
    supervisor = LifecycleSupervisor(30, clock=clock)

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(pool._gateway_managed_owner.closed_admission)
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(closing, 3)
    done = asyncio.Event()
    checking.add_done_callback(lambda _: done.set())
    gate.resume.set()
    await bounded(done)
    assert checking.done() and not checking.cancelled()
    assert checking._log_traceback is False
    assert supervisor.unclosed_resources == {"redis"}


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
async def test_f6_writerless_disconnect_preserves_unresolved_confirmation(backend):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    future, attempts = asyncio.get_running_loop().create_future(), []

    def close_writer():
        attempts.append(True)
        raise OSError("synthetic writer close failure")

    connection._reader = SimpleNamespace()
    connection._writer = SimpleNamespace(
        close=close_writer,
        wait_closed=lambda: future,
        _protocol=SimpleNamespace(_get_close_waiter=lambda w: future),
    )
    owner = pool._gateway_managed_owner
    await connection.disconnect()
    assert connection._writer is None and owner.last_cleanup[connection] == "unresolved"
    await connection.disconnect()
    after_noop = owner.last_cleanup[connection]
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    try:
        assert after_noop == "unresolved"
        assert supervisor.unclosed_resources == {"redis"}
        assert not future.done() and attempts == [True]
    finally:
        future.cancel()


class F6Writer:
    def __init__(self, *, fail=True):
        self.future = asyncio.get_running_loop().create_future()
        self._protocol = SimpleNamespace(_get_close_waiter=lambda writer: self.future)
        self.fail = fail
        self.attempts = 0

    def close(self):
        self.attempts += 1
        if self.fail:
            raise OSError("synthetic writer close failure")
        self.future.set_result(None)

    async def wait_closed(self):
        await self.future


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("confirmation", ["success", "error", "cancelled", "invalid"])
async def test_f6_original_writer_confirmation_controls_completion(
    backend, confirmation
):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    writer = F6Writer()
    connection._reader, connection._writer = SimpleNamespace(), writer
    owner = pool._gateway_managed_owner
    await connection.disconnect()
    record = next(iter(owner.writer_obligations[connection].values()))
    for _ in range(3):
        await connection.disconnect()
        assert owner.last_cleanup[connection] == "unresolved"
    if confirmation == "success":
        writer.future.set_result(None)
    elif confirmation == "error":
        writer.future.set_exception(OSError("synthetic close confirmation failure"))
    elif confirmation == "cancelled":
        writer.future.cancel()
    else:
        writer.future.set_result(False)
    assert owner.closure_confirmed(connection) is (confirmation == "success")
    assert record.state == (
        "completed"
        if confirmation == "success"
        else "failed"
        if confirmation == "error"
        else "unresolved"
    )
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    assert supervisor.unclosed_resources == (
        set() if confirmation == "success" else {"redis"}
    )
    for _ in range(2):
        if confirmation == "success":
            await runtime.close()
        else:
            with pytest.raises(RedisDisposalError):
                await runtime.close()
    assert writer.attempts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("second_fails", [False, True])
async def test_f6_two_writer_generations_retain_independent_obligations(
    backend, second_fails
):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    owner = pool._gateway_managed_owner
    first, second = F6Writer(), F6Writer(fail=second_fails)
    for writer in (first, second):
        connection._reader, connection._writer = SimpleNamespace(), writer
        await connection.disconnect()
    records = owner.writer_obligations[connection]
    assert len(records) == (2 if second_fails else 1)
    assert not owner.closure_confirmed(connection)
    assert first.attempts == second.attempts == 1
    first.future.set_result(None)
    assert owner.closure_confirmed(connection) is not second_fails
    if second_fails:
        second.future.set_result(None)
        assert owner.closure_confirmed(connection)
    assert owner.writer_obligations[connection] == {}
    supervisor = LifecycleSupervisor(30, clock=Clock())

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    assert not supervisor.unclosed_resources
    assert first.attempts == second.attempts == 1


@pytest.mark.asyncio
async def test_f6_same_writer_proxy_new_protocol_cannot_confirm_previous_generation():
    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.client.connection_pool
    connection = pool.get_available_connection()
    owner = pool._gateway_managed_owner
    writer = F6Writer()
    connection._reader, connection._writer = SimpleNamespace(), writer
    await connection.disconnect()
    old = writer.future
    old_record = next(iter(owner.writer_obligations[connection].values()))
    writer.future = asyncio.get_running_loop().create_future()
    writer._protocol = SimpleNamespace(_get_close_waiter=lambda w: writer.future)
    writer.fail = False
    connection._reader, connection._writer = SimpleNamespace(), writer
    await connection.disconnect()
    assert not owner.closure_confirmed(connection)
    assert old_record.confirmation is old and not old.done()
    old.set_result(None)
    assert owner.closure_confirmed(connection)
    await runtime.close()
    assert writer.attempts == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["guarded", "legacy"])
@pytest.mark.parametrize("outcome", ["success", "error", "cancelled"])
async def test_f6_late_confirmation_after_cutoff_is_observation_not_retry(
    backend, monkeypatch, outcome
):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.runtime import RedisDisposalError
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = (
        runtime.guarded_renewal_transport.pool
        if backend == "guarded"
        else runtime.client.connection_pool
    )
    connection = pool.get_available_connection()
    writer = F6Writer()
    connection._reader, connection._writer = SimpleNamespace(), writer
    owner = pool._gateway_managed_owner
    await connection.disconnect()
    record = next(iter(owner.writer_obligations[connection].values()))
    clock, waiting = Clock(), asyncio.Event()
    supervisor = LifecycleSupervisor(30, clock=clock)
    scope = LifecycleScope(supervisor)
    original = owner.wait_for_completion

    async def wait(futures):
        if writer.future in futures:
            waiting.set()
        await original(futures)

    monkeypatch.setattr(owner, "wait_for_completion", wait)

    async def close():
        await runtime.close(cleanup_scope=scope)

    closing = asyncio.create_task(supervisor.close_resources([("redis", close)]))
    await bounded(waiting)
    await clock.expect(30)
    clock.advance(30)
    await asyncio.wait_for(closing, 3)
    await asyncio.gather(runtime._close_task, return_exceptions=True)
    assert supervisor.unclosed_resources == {"redis"}
    observed = asyncio.Event()
    writer.future.add_done_callback(lambda _: observed.set())
    if outcome == "success":
        writer.future.set_result(None)
    elif outcome == "error":
        writer.future.set_exception(OSError("synthetic late close failure"))
    else:
        writer.future.cancel()
    await bounded(observed)
    assert owner.closure_confirmed(connection) is (outcome == "success")
    assert record.state == (
        "completed"
        if outcome == "success"
        else "failed"
        if outcome == "error"
        else "unresolved"
    )
    assert writer.future._log_traceback is False
    assert writer.attempts == 1
    with pytest.raises(RedisDisposalError):
        await runtime.close()
    assert supervisor.unclosed_resources == {"redis"}
    with pytest.raises(GuardedTransportClosed):
        pool.make_connection()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preparation", "drain", "response", "noscript"])
@pytest.mark.parametrize("cancel_at", [2.5, 3.0, 4.0])
async def test_f7_cancellation_preserves_evidence_and_guards_cleanup(stage, cancel_at):
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    semaphore, connection, scripts, _transport = setup()
    clock = Clock()
    supervisor = LifecycleSupervisor(3, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)
    entered, resume = asyncio.Event(), asyncio.Event()
    cleanup = []
    original = connection.disconnect

    async def disconnect(**kwargs):
        cleanup.append(clock.now())
        await original(**kwargs)

    async def gate():
        entered.set()
        await resume.wait()

    connection.disconnect = disconnect
    if stage == "preparation":
        scripts.script_load = lambda script: gate()
    elif stage == "drain":
        connection._writer.drain = gate
    elif stage == "response":
        connection.read_response = gate
    else:
        connection.replies = [NoScriptError()]
        original_load = semaphore._load

        async def load(*args, **kwargs):
            if kwargs.get("force"):
                await gate()
            return await original_load(*args, **kwargs)

        semaphore._load = load

    async def invoke():
        with managed_renewal_cleanup(scope):
            return await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())

    task = asyncio.create_task(invoke())
    try:
        await bounded(entered)
        record = task.guarded_renewal_operation
        before = record.attempts
        clock.advance(cancel_at)
        task.cancel()
        task.cancel()  # repeated cancellation cannot create another cleanup path
        with pytest.raises(asyncio.CancelledError):
            await task
        assert record.cancelled and record.attempts == before
        assert record.outcome is None
        if stage in ("drain", "response"):
            assert before[-1].status is Status.UNRESOLVED
            assert cleanup == ([cancel_at] if cancel_at < 3 else [])
        elif stage == "noscript":
            assert before[-1].status is Status.NOSCRIPT
        else:
            assert not before
        assert all(t < 3 for t in cleanup)
        assert all(a.result is None for a in before)
    finally:
        resume.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_f7_scheduled_cleanup_checks_actual_entry_and_started_work_stays_owned():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    _, connection, _, transport = setup()
    clock = Clock()
    supervisor = LifecycleSupervisor(3, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)
    entered, finish = asyncio.Event(), asyncio.Event()
    entries = []

    async def disconnect(**kwargs):
        entries.append(clock.now())
        entered.set()
        await finish.wait()

    connection.disconnect = disconnect
    with managed_renewal_cleanup(scope):
        started = asyncio.create_task(transport._disconnect_operation(connection))
        await bounded(entered)
        queued = asyncio.create_task(transport._disconnect_operation(connection))
        clock.advance(3)
        await queued
        assert entries == [0] and not started.done()
        assert connection._gateway_deferred_scope is scope
        finish.set()
        await started
    assert entries == [0]


@pytest.mark.asyncio
@pytest.mark.parametrize("request_first", [True, False])
async def test_f7_quarantine_retains_capacity_writer_and_supervisor_obligation(
    request_first,
):
    from redis.exceptions import MaxConnectionsError

    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.guarded_renewal_transport.pool
    pool.max_connections = 1
    connection = pool.get_available_connection()
    owner = pool._gateway_managed_owner
    writer = F6Writer(fail=False)
    connection._writer, connection._reader = writer, SimpleNamespace()
    owner.retain_writer(connection, writer)
    clock = Clock()
    supervisor = LifecycleSupervisor(3, clock=clock)
    scope = supervisor.register()
    if request_first:
        scope.begin_cleanup()
        clock.advance(1)
        supervisor.begin_shutdown()
    else:
        supervisor.begin_shutdown()
        clock.advance(1)
        scope.begin_cleanup()
    assert scope.deadline == 3
    clock.advance(3)
    with managed_renewal_cleanup(scope):
        await connection.disconnect()
        await pool.release(connection)
        await connection.disconnect()
    assert connection in owner.connections and connection in pool._in_use_connections
    assert connection not in pool._available_connections
    assert owner.last_cleanup[connection] == "unresolved"
    assert not owner.closure_confirmed(connection) and writer.attempts == 0
    with pytest.raises(MaxConnectionsError):
        pool.get_available_connection()
    with pytest.raises(GuardedTransportClosed):
        await connection.connect()

    async def close():
        await runtime.close(cleanup_scope=LifecycleScope(supervisor))

    await supervisor.close_resources([("redis", close)])
    assert supervisor.unclosed_resources == {"redis"}
    assert not writer.future.done() and writer.attempts == 0
    # Fixture-only evidence production, not a runtime cleanup retry.
    writer.future.set_result(None)
    assert owner.closure_confirmed(connection)
    assert not owner.writer_obligations[connection]
    connection._reader = connection._writer = None


@pytest.mark.asyncio
async def test_f7_inherited_read_response_cleanup_is_fenced_at_actual_entry():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.guarded_renewal_transport.pool
    connection = pool.get_available_connection()
    writer = F6Writer(fail=False)
    connection._reader, connection._writer = SimpleNamespace(), writer
    clock = Clock()
    supervisor = LifecycleSupervisor(3, clock=clock)
    supervisor.begin_shutdown()
    scope = LifecycleScope(supervisor)

    async def parser(**kwargs):
        clock.advance(3)
        raise asyncio.CancelledError

    connection._read_response_from_parser = parser
    with managed_renewal_cleanup(scope), pytest.raises(asyncio.CancelledError):
        await connection.read_response()
    assert writer.attempts == 0
    assert connection._gateway_quarantined
    assert connection in pool._in_use_connections
    assert not pool._gateway_managed_owner.closure_confirmed(connection)
    writer.future.cancel()
    connection._reader = connection._writer = None


@pytest.mark.asyncio
async def test_f7_deferred_scope_cannot_be_replaced_to_retry_cleanup():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    connection = runtime.guarded_renewal_transport.pool.get_available_connection()
    writer = F6Writer(fail=False)
    connection._writer, connection._reader = writer, SimpleNamespace()
    clock = Clock()
    old = LifecycleScope(LifecycleSupervisor(3, clock=clock))
    old.begin_cleanup()
    clock.advance(3)
    with managed_renewal_cleanup(old):
        await connection.disconnect()
    new = LifecycleScope(LifecycleSupervisor(30, clock=clock))
    new.begin_cleanup()
    with managed_renewal_cleanup(new):
        await connection.disconnect()
        await connection.disconnect()
    assert connection._gateway_deferred_scope is old
    assert writer.attempts == 0 and not writer.future.done()
    writer.future.cancel()
    connection._reader = connection._writer = None


@pytest.mark.asyncio
async def test_f7_pool_release_lock_handoff_cannot_reuse_uncertain_connection():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    runtime = RedisRuntime("redis://localhost:6379/3", 2)
    await runtime.start()
    pool = runtime.guarded_renewal_transport.pool
    connection = pool.get_available_connection()
    writer = F6Writer(fail=False)
    connection._writer, connection._reader = writer, SimpleNamespace()
    connection.mark_for_reconnect()
    clock = Clock()
    scope = LifecycleScope(LifecycleSupervisor(3, clock=clock))
    scope.begin_cleanup()
    await pool._lock.acquire()
    entered = asyncio.Event()

    async def release():
        with managed_renewal_cleanup(scope):
            entered.set()
            await pool.release(connection)

    task = asyncio.create_task(release())
    await bounded(entered)
    clock.advance(3)
    pool._lock.release()
    await task
    assert connection._gateway_quarantined
    assert connection in pool._in_use_connections
    assert connection not in pool._available_connections
    assert writer.attempts == 0
    writer.future.cancel()
    connection._reader = connection._writer = None


@pytest.mark.asyncio
async def test_f7_clock_fault_during_cancellation_defers_without_masking_it():
    from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor
    from app.redis.guarded_renewal import managed_renewal_cleanup
    from tests.unit.test_m4_p1c_lifecycle import Clock

    semaphore, connection, _, _ = setup()
    clock = Clock()
    scope = LifecycleScope(LifecycleSupervisor(3, clock=clock))
    scope.begin_cleanup()
    entered, resume = asyncio.Event(), asyncio.Event()

    async def drain():
        entered.set()
        await resume.wait()

    connection._writer.drain = drain

    async def invoke():
        with managed_renewal_cleanup(scope):
            return await semaphore.renew_guarded([policy()], uuid4(), guard=Guard())

    task = asyncio.create_task(invoke())
    try:
        await bounded(entered)
        clock.value = float("nan")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert scope._expired
        assert connection.disconnects == 0
        assert task.guarded_renewal_operation.cancelled
        assert task.guarded_renewal_operation.cleanup_pending
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)
