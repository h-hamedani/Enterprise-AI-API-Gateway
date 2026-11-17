"""Test-only managed-server process observer; never imported by production."""

import asyncio
import contextlib
import json
import os
import runpy
import sys
import threading
import time
from collections import Counter
from uuid import uuid4

import redis
import uvicorn
from starlette.routing import Route

from app import main
from app.core.request_context import new_request_id
from app.normal_api.lifecycle import Policies
from app.normal_api.supervisor import LifecycleSupervisor, MonotonicClock
from app.persistence.models.enums import RateScopeType
from app.redis.concurrency import ResolvedConcurrencyPolicy
from app.redis.guarded_renewal import GuardedRenewalConnection
from tests.unit.test_m4_p1c_lifecycle import change, world

SCENARIO = os.environ["M4_PLATFORM_SCENARIO"]
REQUESTS = {}
FINISH = {}
SUPERVISOR = None
RUNTIME = None
GATE = None
RELEASES = {}
SIGNALS = []


def emit(event, **values):
    print(
        "CERT "
        + json.dumps(
            {
                "event": event,
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "wall": time.monotonic(),
                **values,
            }
        ),
        flush=True,
    )


class Clock:
    """Controlled monotonic domain for exact boundaries, not a new grace."""

    def __init__(self):
        self.value = 0.0
        self.waiters = []

    def now(self):
        return self.value

    def advance(self, value):
        assert value >= self.value
        self.value = value
        for deadline, event in self.waiters:
            if deadline <= value:
                event.set()

    async def until(self, deadline):
        if deadline <= self.value:
            return
        event = asyncio.Event()
        self.waiters.append((deadline, event))
        try:
            await event.wait()
        finally:
            self.waiters.remove((deadline, event))


def snapshot():
    rows = []
    for slot, w in REQUESTS.items():
        owner = w.lifecycle
        evidence = owner.renewal_evidence
        rows.append(
            {
                "slot": slot,
                "end": owner.end,
                "committed": owner.committed,
                "request_deadline": w.deadline_scope.request_deadline,
                "effective_deadline": w.deadline_scope.deadline,
                "live": list(owner.outstanding_tasks),
                "historical": evidence.uncertain_invocations,
                "physical": evidence.physical_obligations,
                "renewal": owner.renewal_obligation,
                "release": owner.release_obligation,
                "release_attempts": RELEASES.get(slot, 0),
                "cleanup_complete": owner.cleanup_complete,
                "phase": getattr(owner.last_renewal_evidence, "phase", None),
            }
        )
    pools = {}
    if RUNTIME is not None:
        guarded = RUNTIME._guarded_renewal_transport
        for name, pool in (
            ("guarded", guarded.pool if guarded else None),
            ("legacy", RUNTIME._client.connection_pool),
        ):
            owner = getattr(pool, "_gateway_managed_owner", None)
            if owner is not None:
                pools[name] = {
                    "connections": len(owner.connections),
                    "quarantined": sum(
                        bool(getattr(c, "_gateway_quarantined", False))
                        for c in owner.connections
                    ),
                    "writer_confirmations": dict(
                        Counter(
                            record.state
                            for records in owner.writer_obligations.values()
                            for record in records.values()
                        )
                    ),
                }
    return {
        "now": SUPERVISOR.clock.now(),
        "deadline": SUPERVISOR.process_deadline,
        "requests": rows,
        "active": len(SUPERVISOR.scopes),
        "residuals": len(SUPERVISOR.residuals),
        "unclosed": sorted(SUPERVISOR.unclosed_resources),
        "disposal": RUNTIME.disposal_states if RUNTIME else {},
        "pools": pools,
    }


original_init = LifecycleSupervisor.__init__


def initialize(self, grace_seconds, **kwargs):
    global SUPERVISOR
    if "clock" not in kwargs:
        kwargs["clock"] = (
            MonotonicClock()
            if SCENARIO in {"P01", "P02", "P03", "P03_cutoff_real", "P12"}
            else Clock()
        )
        SUPERVISOR = self
    original_init(self, grace_seconds, **kwargs)


LifecycleSupervisor.__init__ = initialize
original_begin = LifecycleSupervisor.begin_shutdown


def begin(self):
    first = self.process_deadline is None
    result = original_begin(self)
    if self is SUPERVISOR:
        emit("shutdown", deadline=result, now=self.clock.now(), first=first)
        if first and SCENARIO in {"P09", "P10"}:
            self.clock.advance(result + (SCENARIO == "P10"))
    return result


LifecycleSupervisor.begin_shutdown = begin
server_init = uvicorn.Server.__init__


def observe_server(self, *args, **kwargs):
    server_init(self, *args, **kwargs)
    shutdown = self.shutdown
    activate = self._activate_exit

    def signalled(sig, frame):
        SIGNALS.append(sig)
        emit("signal", signal=sig, now=SUPERVISOR.clock.now())
        activate(sig, frame)

    async def observed_shutdown(*args, **kwargs):
        try:
            return await shutdown(*args, **kwargs)
        finally:
            emit(
                "final",
                **snapshot(),
                listeners_requested=sorted(self.listener_close_requested),
                listeners_confirmed=sorted(self.listener_close_confirmed),
                signal_count=len(SIGNALS),
            )

    self._activate_exit = signalled
    self.shutdown = observed_shutdown


uvicorn.Server.__init__ = observe_server


async def controls(queue):
    while True:
        command = await queue.get()
        kind = command["cmd"]
        if kind == "advance":
            SUPERVISOR.clock.advance(command["value"])
        elif kind == "finish":
            FINISH[command.get("slot", 1)].set()
        elif kind == "open_gate":
            GATE.set()
        emit("state", **snapshot())


class Endpoint:
    async def __call__(self, scope, receive, send):
        slot = scope["path_params"]["slot"]
        w = await world(ttl=5000)
        w.clock, w.supervisor = SUPERVISOR.clock, SUPERVISOR
        w.deadline_scope = SUPERVISOR.register()
        w.lifecycle.concurrency = scope["app"].state.recovery_concurrency
        REQUESTS[slot] = w
        FINISH[slot] = asyncio.Event()
        policy = ResolvedConcurrencyPolicy(
            uuid4(),
            w.auth.identity.tenant_id,
            RateScopeType.SERVICE,
            w.rows["services"][0]["id"],
            1,
        )

        async def policies(resolved):
            return Policies((), (policy,))

        async def deliver(owner):
            emit("upstream_complete", slot=slot)
            if SCENARIO == "P02":
                await FINISH[slot].wait()
            assert owner.before_downstream_commit()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            owner.committed_downstream()
            await send(
                {"type": "http.response.body", "body": b"fixture\n", "more_body": True}
            )
            emit("delivery", slot=slot)
            await FINISH[slot].wait()
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        from tests.unit.test_m4_p1b_resolution import scope as synthetic_scope

        hooks = change(w.hooks, policies=policies, deliver=deliver)
        runner = w.lifecycle.run(
            synthetic_scope(),
            request_id=new_request_id(),
            receive=receive,
            hooks=hooks,
            lifecycle_scope=w.deadline_scope,
        )
        try:
            await runner
        finally:
            emit("request_end", slot=slot, **snapshot())


original_factory = main.create_app


def factory():
    app = original_factory()
    # Test-only ASGI adapter. Production create_app and routing stay untouched.
    app.router.routes.append(Route("/_cert/{slot:int}", Endpoint(), methods=["GET"]))
    return app


main.create_app = factory
original_lifespan = main.lifespan


@contextlib.asynccontextmanager
async def lifespan(app):
    global RUNTIME, GATE
    async with original_lifespan(app):
        RUNTIME = app.state.redis_runtime
        assert redis.__version__ == "8.1.0"
        assert (await RUNTIME.client.info("server"))["redis_version"] == "7.4.11"
        GATE = asyncio.Event()
        facade = app.state.recovery_concurrency
        transport = RUNTIME.guarded_renewal_transport
        original_release = facade.release

        async def release(policies, handle):
            slot = next(n for n, w in REQUESTS.items() if w.lifecycle.handle is handle)
            owner = REQUESTS[slot].lifecycle
            RELEASES[slot] = RELEASES.get(slot, 0) + 1
            emit(
                "release",
                slot=slot,
                now=SUPERVISOR.clock.now(),
                original=True,
                renewal_live=owner._renewal is not None and not owner._renewal.done(),
                invocation_live=bool(owner._renewal_obligations),
            )
            if SCENARIO in {"P07", "P11"}:
                await GATE.wait()
            result = await original_release(policies, handle)
            emit("released", slot=slot, released=result.released)
            return result

        facade.release = release
        original_renew = facade.renew_guarded

        async def renew(*args, **kwargs):
            emit("renew", now=SUPERVISOR.clock.now())
            try:
                result = await original_renew(*args, **kwargs)
            except asyncio.CancelledError:
                record = getattr(
                    asyncio.current_task(), "guarded_renewal_operation", None
                )
                emit(
                    "renew_cancelled",
                    phase=getattr(record, "phase", None),
                    physical=bool(getattr(record, "cleanup_pending", False)),
                )
                raise
            finally:
                record = getattr(
                    asyncio.current_task(), "guarded_renewal_operation", None
                )
                expected = next(
                    w.deadline_scope
                    for w in REQUESTS.values()
                    if w.lifecycle.handle is args[1]
                )
                emit(
                    "context",
                    scope_match=record is not None and record.cleanup_scope is expected,
                )
            emit("renewed", disposition=result.disposition, now=SUPERVISOR.clock.now())
            return result

        facade.renew_guarded = renew

        class Writer:
            def __init__(self, writer):
                self.writer = writer
                self.renewal_written = False

            def __getattr__(self, name):
                return getattr(self.writer, name)

            def writelines(self, command):
                packed = list(command)
                self.writer.writelines(packed)
                if b"EVALSHA\r\n" in b"".join(packed):
                    self.renewal_written = True
                    emit("dispatch", now=SUPERVISOR.clock.now())
                    if SCENARIO == "P06":
                        self.writer.transport.abort()

            async def drain(self):
                if SCENARIO == "P05_drain" and self.renewal_written:
                    emit("gated", phase="drain")
                    await GATE.wait()
                await self.writer.drain()

        class Connection(GuardedRenewalConnection):
            async def _connect(self):
                await super()._connect()
                self._writer = Writer(self._writer)

            async def on_connect_check_health(self, check_health=True):
                if SCENARIO == "P04":
                    emit("gated", phase="preparation")
                    await GATE.wait()
                await super().on_connect_check_health(check_health=check_health)

            async def send_renewal(self, *args, **kwargs):
                result = await super().send_renewal(*args, **kwargs)
                if result:
                    self.renewal_reply = True
                return result

            async def read_response(self, *args, **kwargs):
                if getattr(self, "renewal_reply", False) and SCENARIO in {
                    "P05_response",
                    "P08",
                    "P14",
                }:
                    emit("gated", phase="response")
                    try:
                        await GATE.wait()
                    except asyncio.CancelledError:
                        if SCENARIO not in {"P08", "P14"}:
                            raise
                        emit("cancellation_pending")
                        while not GATE.is_set():
                            try:
                                await GATE.wait()
                            except asyncio.CancelledError:
                                pass
                        raise
                return await super().read_response(*args, **kwargs)

        transport.pool.connection_class = Connection
        close = RUNTIME.close

        async def observed_close(*args, **kwargs):
            emit(
                "redis_close",
                now=SUPERVISOR.clock.now(),
                deadline=SUPERVISOR.process_deadline,
            )
            try:
                return await close(*args, **kwargs)
            finally:
                emit("redis_disposal", states=RUNTIME.disposal_states)

        RUNTIME.close = observed_close
        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def reader():
            for line in sys.stdin:
                loop.call_soon_threadsafe(queue.put_nowait, json.loads(line))

        threading.Thread(target=reader, daemon=True).start()
        controller = asyncio.create_task(controls(queue))
        emit(
            "ready",
            version=redis.__version__,
            grace=SUPERVISOR.grace_seconds,
            clock=type(SUPERVISOR.clock).__name__,
        )
        try:
            yield
        finally:
            # This task is fixture-owned observation, not production cleanup.
            controller.cancel()
    emit("lifespan_exit", **snapshot())


main.lifespan = lifespan
emit("boot", scenario=SCENARIO, platform=sys.platform)
sys.argv = ["app.server", "--host", "127.0.0.1", "--port", "0"]
runpy.run_module("app.server", run_name="__main__")
