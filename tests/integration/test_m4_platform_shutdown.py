"""Opt-in OS-signal certification on exclusively test-owned dependencies."""

import hashlib
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from tests.integration.test_m4_p1c_f3_server_process import (
    WINDOWS_SIGNAL,
    hidden_startup,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("M4_PLATFORM_CERTIFICATION") != "1",
    reason="Opt-in isolated platform process certification.",
)
ROOT = Path(__file__).resolve().parents[2]
WORKER = Path(__file__).with_name("m4_platform_shutdown_worker.py")


def docker(*args):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, timeout=60
    ).stdout.strip()


@pytest.fixture(scope="module")
def infrastructure():
    external = os.environ.get("M4_PLATFORM_EXTERNAL_SERVICES_FILE")
    if external:
        # Host orchestrator owns creation, identity/version checks and teardown.
        # The Linux runner receives endpoints only, never a Docker socket.
        services = json.loads(Path(external).read_text(encoding="utf-8"))
        assert set(services) == {"pg", "redis", "pg_host", "redis_host"}
        for name in ("pg", "redis"):
            assert type(services[name]) is int and 1 <= services[name] <= 65535
            host = services[name + "_host"]
            assert isinstance(host, str) and host
            assert all(c.isalnum() or c in ".-_" for c in host)
        yield services
        return
    owner = "m4-platform-" + uuid4().hex
    identifiers = []
    names = [owner + "-pg", owner + "-redis"]
    try:
        pg = docker(
            "run",
            "-d",
            "--name",
            owner + "-pg",
            "--label",
            "m4.platform.owner=" + owner,
            "--tmpfs",
            "/var/lib/postgresql/data",
            "-p",
            "127.0.0.1::5432",
            "-e",
            "POSTGRES_PASSWORD=fixture_local_only",
            "-e",
            "POSTGRES_USER=gateway",
            "-e",
            "POSTGRES_DB=gateway",
            "postgres:16",
        )
        identifiers.append(pg)
        redis = docker(
            "run",
            "-d",
            "--name",
            owner + "-redis",
            "--label",
            "m4.platform.owner=" + owner,
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
        )
        identifiers.append(redis)
        # Readiness only: execution/ordering proofs use explicit events.
        for identifier, command in (
            (pg, ["pg_isready", "-U", "gateway"]),
            (redis, ["redis-cli", "ping"]),
        ):
            deadline = time.monotonic() + 30
            while True:
                r = subprocess.run(
                    ["docker", "exec", identifier, *command],
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
                if r.returncode == 0:
                    break
                if time.monotonic() >= deadline:
                    pytest.fail("Disposable dependency readiness failed")
                threading.Event().wait(0.1)
        version = docker("exec", redis, "redis-cli", "info", "server")
        assert "redis_version:7.4.11" in version
        yield {
            "pg": int(docker("port", pg, "5432/tcp").rsplit(":", 1)[1]),
            "redis": int(docker("port", redis, "6379/tcp").rsplit(":", 1)[1]),
        }
    finally:
        for name in reversed(names):
            identifier = docker(
                "ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}"
            )
            if not identifier:
                continue
            metadata = json.loads(docker("inspect", identifier))[0]
            assert metadata["Name"] == "/" + name
            assert metadata["Config"]["Labels"]["m4.platform.owner"] == owner
            identifier = metadata["Id"]
            docker("rm", "-f", "-v", identifier)
            assert not docker(
                "ps", "-a", "--filter", "id=" + identifier, "--format", "{{.ID}}"
            )


class Process:
    def __init__(self, services, scenario, cwd, grace="30"):
        env = {
            k: v
            for k, v in os.environ.items()
            if not any(word in k.upper() for word in ("SECRET", "ENCRYPTION", "DSN"))
        }
        env.update(
            PYTHONPATH=str(ROOT),
            PYTHONDONTWRITEBYTECODE="1",
            ENVIRONMENT="development",
            M4_PLATFORM_SCENARIO=scenario,
            CONCURRENCY_LEASE_DURATION_MS="5000",
            REDIS_URL=f"redis://{services.get('redis_host', '127.0.0.1')}:{services['redis']}/0",
            POSTGRES_DSN=f"postgresql+psycopg_async://gateway:fixture_local_only@{services.get('pg_host', '127.0.0.1')}:{services['pg']}/gateway",
        )
        env.pop("LIFECYCLE_GRACE_TIMEOUT_SECONDS", None)
        if grace is not None:
            env["LIFECYCLE_GRACE_TIMEOUT_SECONDS"] = grace
        bootstrap = (
            "import json,os,runpy,time; "
            "print('CERT '+json.dumps({'event':'process','pid':os.getpid(),"
            "'ppid':os.getppid(),'wall':time.monotonic()}),flush=True); "
            f"runpy.run_path({WORKER.as_posix()!r},run_name='__main__')"
        )
        self.process = subprocess.Popen(
            [sys.executable, "-u", "-B", "-c", bootstrap],
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            **hidden_startup(),
        )
        self.messages = queue.Queue()
        self.events = []
        self.lines = []
        self.forced = False
        self.responses = {}
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        for line in self.process.stdout:
            self.lines.append(line.rstrip())
            if line.startswith("CERT "):
                event = json.loads(line[5:])
                self.events.append(event)
                self.messages.put(event)
            elif "Uvicorn running on" in line:
                self.port = int(line.rsplit(":", 1)[1].split()[0])
                self.messages.put({"event": "listening"})
        self.messages.put({"event": "eof"})

    def wait(self, event, *, timeout=20, **fields):
        deadline = time.monotonic() + timeout
        while True:
            item = self.messages.get(timeout=max(0.01, deadline - time.monotonic()))
            if item["event"] == event and all(
                item.get(k) == v for k, v in fields.items()
            ):
                return item
            if item["event"] == "eof":
                pytest.fail(
                    "Child exited before " + event + ": " + "\n".join(self.lines[-10:])
                )

    def command(self, cmd, **values):
        self.process.stdin.write(json.dumps({"cmd": cmd, **values}) + "\n")
        self.process.stdin.flush()

    def signalled(self):
        if sys.platform == "win32":
            subprocess.run(
                [sys.executable, "-B", "-c", WINDOWS_SIGNAL, str(self.process.pid)],
                check=True,
                capture_output=True,
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        else:
            self.process.send_signal(signal.SIGTERM)
        return self.wait("signal")

    def request(self, slot):
        def invoke():
            try:
                with httpx.Client(trust_env=False, timeout=40) as client:
                    response = client.get(f"http://127.0.0.1:{self.port}/_cert/{slot}")
                    self.responses[slot] = {
                        "status": response.status_code,
                        "body_complete": response.content == b"fixture\n",
                    }
            except httpx.TransportError:
                self.responses[slot] = {"aborted": True}

        thread = threading.Thread(target=invoke, daemon=True)
        thread.start()
        return thread

    def finish(self, timeout=15):
        code = self.process.wait(timeout=timeout)
        self.thread.join(timeout=3)
        assert not self.thread.is_alive()
        assert not self.forced
        finals = [e for e in self.events if e["event"] == "final"]
        return code, finals[-1] if finals else None

    def save(self, scenario):
        output = os.environ.get("M4_PLATFORM_EVIDENCE_DIR")
        if output:
            destination = Path(output)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / f"{sys.platform}-{scenario}.json").write_text(
                json.dumps(
                    {
                        "scenario": scenario,
                        "platform": sys.platform,
                        "parent_pid": os.getpid(),
                        "launcher_pid": self.process.pid,
                        "exit_code": self.process.poll(),
                        "forced": self.forced,
                        "worker_sha256": hashlib.sha256(
                            WORKER.read_bytes()
                        ).hexdigest(),
                        "events": self.events,
                        "responses": self.responses,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

    def close(self):
        if sys.platform == "win32":
            # The venv launcher may have a distinct interpreter child. Never
            # kill by executable name, and never trust a recycled PID alone.
            for pid in {e["pid"] for e in self.events if e["event"] == "process"}:
                if pid == self.process.pid:
                    continue
                expected_path = WORKER.as_posix().replace("'", "''")
                command = (
                    f"$p = Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}'; "
                    "if ($p) { "
                    f"if ($p.ParentProcessId -ne {self.process.pid} -or "
                    f"$p.CommandLine -notlike '*{expected_path}*') {{ throw 'Unproven test process ownership' }}; "
                    f"$owned = Get-Process -Id {pid}; $null = $owned.Handle; "
                    "$owned.Kill(); $owned.WaitForExit(5000); Write-Output 'FORCED' }"
                )
                result = subprocess.run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-NonInteractive",
                        "-Command",
                        command,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if "FORCED" in result.stdout:
                    self.forced = True
        if self.process.poll() is None:
            self.forced = True
            self.process.kill()  # Exact fixture-owned PID; never graceful evidence.
        self.process.wait(timeout=5)
        self.thread.join(timeout=3)
        self.process.stdin.close()
        self.process.stdout.close()


@pytest.mark.parametrize(
    "scenario",
    [
        "P01",
        "P02",
        "P03",
        "P03_cutoff_real",
        "P04",
        "P05_drain",
        "P05_response",
        "P06",
        "P07",
        "P08",
        "P09",
        "P10",
        "P11",
        "P12",
        "P14",
    ],
)
def test_managed_process_shutdown(infrastructure, tmp_path, scenario):
    child = Process(infrastructure, scenario, tmp_path)
    requests = []
    try:
        child.wait("ready")
        child.wait("listening")
        with httpx.Client(trust_env=False, timeout=5) as client:
            assert client.get(f"http://127.0.0.1:{child.port}/health/live").json() == {
                "status": "ok"
            }
            assert (
                client.get(f"http://127.0.0.1:{child.port}/health/ready").status_code
                == 200
            )
        if scenario not in {"P01", "P09", "P10"}:
            requests.append(child.request(1))
            child.wait("upstream_complete" if scenario == "P02" else "delivery", slot=1)
        if scenario in {"P03", "P03_cutoff_real"}:
            child.wait("renewed", disposition="BACKEND_RESULT")
        if scenario in {"P04", "P05_drain", "P05_response", "P06", "P08", "P14"}:
            child.command("advance", value=5 / 3)
            child.wait("renew")
            if scenario == "P06":
                child.wait("renewed", disposition="UNKNOWN")
            else:
                child.wait("gated")
        if scenario in {"P08", "P14"}:
            child.command("advance", value=10 / 3)
            child.wait("cancellation_pending")
            child.command("state")
            state = child.wait("state")
            cutoff = state["requests"][0]["request_deadline"]
            assert cutoff is not None
            child.command("advance", value=cutoff)
            child.wait("request_end")
            child.command("open_gate")
            cancelled = child.wait("renew_cancelled")
            assert cancelled["physical"]
        if scenario in {"P07", "P11"}:
            child.command("finish", slot=1)
            child.wait("release", slot=1)
        if scenario == "P11":
            child.command("advance", value=1)
            child.wait("state")
            requests.append(child.request(2))
            child.wait("delivery", slot=2)
            child.command("finish", slot=2)
            child.wait("release", slot=2)
            child.command("advance", value=2)
            child.wait("state")
        child.signalled()
        shutdown = child.wait("shutdown", first=True)
        if scenario == "P12":
            child.signalled()
            repeated = child.wait("shutdown", first=False)
            assert repeated["deadline"] == shutdown["deadline"]
        if scenario in {"P04", "P05_drain", "P05_response", "P07"}:
            child.command("advance", value=shutdown["deadline"])
        elif scenario == "P11":
            child.command("advance", value=30)
            child.wait("request_end", slot=1)
            child.command("state")
            state = child.wait("state")
            assert (
                next(r for r in state["requests"] if r["slot"] == 2)["release"]
                == "attempted_unconfirmed"
            )
            child.command("advance", value=31)
        elif scenario in {"P02", "P03", "P06", "P12"}:
            child.command("finish", slot=1)
        # Test watchdog only; the original native-monotonic G remains 30.
        code, final = child.finish(timeout=45 if scenario == "P03_cutoff_real" else 15)
        assert code in ({0, 3} if sys.platform == "win32" else {0, -signal.SIGTERM})
        assert final is not None
        assert final["deadline"] == shutdown["deadline"]
        assert final["listeners_requested"] == ["listener_0"]
        assert all(r["release_attempts"] <= 1 for r in final["requests"])
        assert all(e["scope_match"] for e in child.events if e["event"] == "context")
        assert all(
            not e["renewal_live"] and not e["invocation_live"]
            for e in child.events
            if e["event"] == "release"
        )
        assert all(
            e["now"] < e["deadline"]
            for e in child.events
            if e["event"] == "redis_close"
        )
        if scenario in {"P01", "P02", "P03", "P12"}:
            assert not final["unclosed"] and not final["residuals"]
            assert all(r["cleanup_complete"] for r in final["requests"])
            assert final["disposal"] == {"guarded": "completed", "legacy": "completed"}
            for thread in requests:
                thread.join(timeout=3)
                assert not thread.is_alive()
            assert all(
                r == {"status": 200, "body_complete": True}
                for r in child.responses.values()
            )
        if scenario in {"P04", "P05_drain", "P05_response"}:
            row = final["requests"][0]
            assert not row["cleanup_complete"] and row["release"] == "not_attempted"
            assert row["release_attempts"] == 0
            assert row["live"] or row["historical"] or row["physical"]
            assert sum(e["event"] == "dispatch" for e in child.events) == (
                scenario != "P04"
            )
            assert final["active"] + final["residuals"] > 0
        if scenario == "P06":
            row = final["requests"][0]
            assert row["historical"] == 1 and row["physical"] == 0 and not row["live"]
            assert (
                row["release"] == "confirmed_success" and row["release_attempts"] == 1
            )
            assert not row["cleanup_complete"] and final["residuals"] == 1
        if scenario in {"P07", "P11"}:
            assert all(
                r["release"] == "attempted_unconfirmed"
                and r["release_attempts"] == 1
                and not r["cleanup_complete"]
                for r in final["requests"]
            )
        if scenario == "P11":
            assert [r["effective_deadline"] for r in final["requests"]] == [30, 31]
            assert final["deadline"] == 32
        if scenario in {"P08", "P14"}:
            assert "redis" in final["unclosed"]
            assert final["disposal"]["guarded"] != "completed"
            assert final["pools"]["guarded"]["quarantined"] >= 1
            assert final["requests"][0]["physical"] >= 1
            assert not final["requests"][0]["cleanup_complete"]
        if scenario == "P12":
            assert final["signal_count"] == 2
            assert (
                len({e["deadline"] for e in child.events if e["event"] == "shutdown"})
                == 1
            )
        if scenario == "P03_cutoff_real":
            assert final["now"] >= final["deadline"]
            assert final["now"] - shutdown["now"] >= 29.9
            assert not final["requests"][0]["cleanup_complete"]
            assert final["requests"][0]["release_attempts"] == 0
            assert all(
                e["now"] < final["deadline"]
                for e in child.events
                if e["event"] == "dispatch"
            )
        if scenario in {"P09", "P10"}:
            assert "application_lifespan" in final["unclosed"]
            assert not any(e["event"] == "redis_close" for e in child.events)
    finally:
        child.close()
        child.save(scenario)
        for thread in requests:
            thread.join(timeout=3)
        if sys.exc_info()[0] is None:
            assert not child.forced
            assert all(not thread.is_alive() for thread in requests)


@pytest.mark.parametrize("grace", [None, "0", "inf"])
def test_invalid_managed_configuration_fails_before_listening(
    infrastructure, tmp_path, grace
):
    child = Process(infrastructure, "P13", tmp_path, grace=grace)
    try:
        code, final = child.finish()
        assert code != 0 and final is None
        assert not hasattr(child, "port")
        assert not any(e["event"] == "ready" for e in child.events)
    finally:
        child.close()
        child.save("P13-" + str(grace))
        if sys.exc_info()[0] is None:
            assert not child.forced
