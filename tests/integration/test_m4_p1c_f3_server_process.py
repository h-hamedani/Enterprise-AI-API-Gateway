"""Opt-in real managed entrypoint, OS signal, sockets and disposable PG/Redis.

The child bootstrap only observes lifecycle calls and (one case) advances an
injected monotonic clock. runpy executes the actual app.server __main__ path.
"""

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("M4_F3_SERVER_ACCEPTANCE") != "1",
    reason="Opt-in real server acceptance with disposable Docker dependencies.",
)

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = r"""
import asyncio, contextlib, json, os, runpy, signal, sys
from app.normal_api.supervisor import LifecycleSupervisor, MonotonicClock
import app.main as main
import uvicorn
supervisors=[]
servers=[]
class Clock(MonotonicClock):
    offset=0
    def now(self): return super().now()+self.offset
original_init=LifecycleSupervisor.__init__
def initialize(self, grace_seconds, **kwargs):
    original_init(self, grace_seconds, clock=Clock())
    supervisors.append(self)
    print('F3_CONFIG '+str(grace_seconds),flush=True)
LifecycleSupervisor.__init__=initialize
original_begin=LifecycleSupervisor.begin_shutdown
def begin(self):
    first=self.process_deadline is None
    result=original_begin(self)
    print('F3_ACTIVATE '+str(result),flush=True)
    if first and os.environ['F3_EXPIRED']=='1': self.clock.offset+=31
    return result
LifecycleSupervisor.begin_shutdown=begin
original_server_init=uvicorn.Server.__init__
def server_init(self,*args,**kwargs):
    original_server_init(self,*args,**kwargs)
    servers.append(self)
uvicorn.Server.__init__=server_init
original_shutdown=uvicorn.Server.shutdown
async def shutdown(self,*args,**kwargs):
    print('F3_DRAIN',flush=True)
    return await original_shutdown(self,*args,**kwargs)
uvicorn.Server.shutdown=shutdown
original_lifespan=main.lifespan
@contextlib.asynccontextmanager
async def lifespan(app):
    async with original_lifespan(app):
        yield
    print('F3_LIFESPAN_EXIT',flush=True)
main.lifespan=lifespan
# Test-owned prior handler makes Uvicorn's documented post-cleanup signal
# re-raise observable without converting a completed cleanup into OS exit code.
def chained(sig,frame): print('F3_CHAINED',flush=True)
for sig in uvicorn.server.HANDLED_SIGNALS: signal.signal(sig,chained)
sys.argv=['app.server','--host','127.0.0.1','--port','0']
runpy.run_module('app.server',run_name='__main__')
sup=supervisors[0]
server=servers[0]
print('F3_FINAL '+json.dumps({'requested':sorted(server.listener_close_requested),
    'confirmed':sorted(server.listener_close_confirmed),
    'unfinished':sorted(sup.unclosed_resources)}),flush=True)
"""

WINDOWS_SIGNAL = r"""
import ctypes, sys, threading
k=ctypes.WinDLL('kernel32',use_last_error=True)
k.FreeConsole()
if not k.AttachConsole(int(sys.argv[1])): raise ctypes.WinError(ctypes.get_last_error())
received=threading.Event()
handler_type=ctypes.WINFUNCTYPE(ctypes.c_int,ctypes.c_uint)
def acknowledge(code):
    received.set()
    return 1
handler=handler_type(acknowledge)
if not k.SetConsoleCtrlHandler(handler,True): raise ctypes.WinError(ctypes.get_last_error())
if not k.GenerateConsoleCtrlEvent(1,0): raise ctypes.WinError(ctypes.get_last_error())
# Do not detach/reset handlers while the asynchronous console event is pending.
if not received.wait(5): raise RuntimeError('Console event was not acknowledged')
"""


def docker(*args):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, timeout=45
    ).stdout.strip()


@pytest.fixture(scope="module")
def dependencies():
    identifiers = []
    try:
        prefix = "gateway-f3-" + uuid4().hex
        pg = docker(
            "run",
            "--rm",
            "-d",
            "--name",
            prefix + "-pg",
            "-p",
            "127.0.0.1::5432",
            "-e",
            "POSTGRES_USER=gateway",
            "-e",
            "POSTGRES_PASSWORD=fixture_local_only",
            "-e",
            "POSTGRES_DB=gateway",
            "--health-cmd",
            "pg_isready -U gateway -d gateway",
            "--health-interval",
            "1s",
            "--health-timeout",
            "2s",
            "postgres:16",
        )
        identifiers.append(pg)
        redis = docker(
            "run",
            "--rm",
            "-d",
            "--name",
            prefix + "-redis",
            "-p",
            "127.0.0.1::6379",
            "--health-cmd",
            "redis-cli ping",
            "--health-interval",
            "1s",
            "--health-timeout",
            "2s",
            "redis:7-alpine",
        )
        identifiers.append(redis)
        deadline = time.monotonic() + 40
        for identifier in identifiers:
            while (
                docker("inspect", "-f", "{{.State.Health.Status}}", identifier)
                != "healthy"
            ):
                if time.monotonic() >= deadline:
                    pytest.fail("Disposable dependencies did not become healthy")
                # Infrastructure readiness polling, not a concurrency/race proof.
                threading.Event().wait(0.1)
        yield (
            int(docker("port", pg, "5432/tcp").rsplit(":", 1)[1]),
            int(docker("port", redis, "6379/tcp").rsplit(":", 1)[1]),
        )
    finally:
        for identifier in reversed(identifiers):
            docker("stop", identifier)  # Only this fixture's --rm containers.


def hidden_startup():
    if sys.platform != "win32":
        return {}
    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = 0
    return {"startupinfo": startup, "creationflags": subprocess.CREATE_NEW_CONSOLE}


@pytest.mark.parametrize("expired", [False, True])
def test_real_managed_entrypoint_signal_and_listener_shutdown(
    dependencies, tmp_path, expired
):
    pg, redis = dependencies
    env = {
        key: value
        for key, value in os.environ.items()
        if not any(part in key.upper() for part in ("SECRET", "ENCRYPTION", "DSN"))
    }
    env.update(
        PYTHONPATH=str(ROOT),
        LIFECYCLE_GRACE_TIMEOUT_SECONDS="30",
        ENVIRONMENT="development",
        REDIS_URL=f"redis://127.0.0.1:{redis}/0",
        POSTGRES_DSN=f"postgresql+psycopg_async://gateway:fixture_local_only@127.0.0.1:{pg}/gateway",
        F3_EXPIRED="1" if expired else "0",
    )
    process = subprocess.Popen(
        [sys.executable, "-u", "-B", "-c", BOOTSTRAP],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **hidden_startup(),
    )
    lines, messages = [], queue.Queue()

    def read():
        for line in process.stdout:
            lines.append(line.rstrip())
            messages.put(line)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        watchdog = time.monotonic() + 45
        port = None
        while port is None:
            line = messages.get(timeout=max(0.01, watchdog - time.monotonic()))
            match = re.search(r"Uvicorn running on http://127\.0\.0\.1:(\d+)", line)
            if match:
                port = int(match.group(1))
        with httpx.Client(trust_env=False, timeout=5) as client:
            assert client.get(f"http://127.0.0.1:{port}/health/live").json() == {
                "status": "ok"
            }
            assert (
                client.get(f"http://127.0.0.1:{port}/health/ready").status_code == 200
            )
        if sys.platform == "win32":
            subprocess.run(
                [sys.executable, "-B", "-c", WINDOWS_SIGNAL, str(process.pid)],
                check=True,
                capture_output=True,
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        else:
            process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=40) == 0
        reader.join(timeout=3)
        assert not reader.is_alive()
        assert "F3_CONFIG 30.0" in lines
        activations = [
            float(line.split()[1]) for line in lines if line.startswith("F3_ACTIVATE ")
        ]
        assert activations and len(set(activations)) == 1
        assert "F3_CHAINED" in lines
        final = json.loads(
            next(
                line.removeprefix("F3_FINAL ")
                for line in lines
                if line.startswith("F3_FINAL ")
            )
        )
        assert final["requested"] == ["listener_0"]
        if expired:
            assert "F3_DRAIN" not in lines
            assert "application_lifespan" in final["unfinished"]
        else:
            assert lines.index(
                next(line for line in lines if line.startswith("F3_ACTIVATE "))
            ) < lines.index("F3_DRAIN")
            assert "F3_LIFESPAN_EXIT" in lines
            assert final["confirmed"] == ["listener_0"] and not final["unfinished"]
        with (
            httpx.Client(trust_env=False, timeout=1) as client,
            pytest.raises(httpx.TransportError),
        ):
            client.get(f"http://127.0.0.1:{port}/health/live")
    finally:
        if process.poll() is None:
            process.kill()  # Watchdog cleanup of this exact test-owned child only.
        process.wait(timeout=5)
        reader.join(timeout=3)
        process.stdout.close()
