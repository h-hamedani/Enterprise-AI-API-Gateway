"""Execute only a uniquely named disposable Compose project; no shared services."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

root = Path(__file__).resolve().parents[2]
output = Path(sys.argv[1]).resolve()
project = "m4-cert-" + uuid4().hex
env = {**os.environ, "M4_CERT_PROJECT": project}
override = output / "smoke.override.json"
override.write_text(
    json.dumps(
        {
            "services": {
                "gateway": {
                    "entrypoint": ["python", "-B", "/lab-observer.py"],
                    "command": [],
                    "environment": {"PYTHONPATH": "/opt/gateway"},
                    "volumes": [
                        {
                            "type": "bind",
                            "source": str(root / "tests/platform_lab/observe_pid1.py"),
                            "target": "/lab-observer.py",
                            "read_only": True,
                        }
                    ],
                }
            }
        }
    ),
    encoding="utf-8",
)
base = [
    "docker",
    "compose",
    "-f",
    str(root / "tests/platform_lab/compose.yaml"),
    "-f",
    str(override),
]


def run(args, timeout=60):
    result = subprocess.run(
        args, capture_output=True, text=True, env=env, timeout=timeout, check=False
    )
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout.strip()


def metadata(cid):
    row = json.loads(run(["docker", "inspect", cid]))[0]
    assert row["Config"]["Labels"]["com.docker.compose.project"] == project
    return row


evidence = {"project": project}
try:
    run(base + ["up", "-d", "--no-build", "--pull", "never"], timeout=100)
    ids = {s: run(base + ["ps", "-q", s]) for s in ("gateway", "redis", "postgres")}
    evidence["ids"] = ids
    for cid in ids.values():
        metadata(cid)
    until = time.monotonic() + 45
    while metadata(ids["gateway"])["State"]["Health"]["Status"] != "healthy":
        if time.monotonic() >= until:
            raise TimeoutError("Gateway readiness")
        time.sleep(0.2)  # Readiness polling only; not a race-ordering proof.
    evidence["connectivity"] = run(
        [
            "docker",
            "exec",
            ids["gateway"],
            "python",
            "-c",
            (
                "import asyncio,redis,psycopg; "
                "r=redis.Redis(host='redis'); assert r.ping(); "
                "assert r.info('server')['redis_version']=='7.4.11'; "
                "c=psycopg.connect('host=postgres user=gateway password=fixture_local_only dbname=gateway'); "
                "assert c.execute('select 1').fetchone()==(1,); c.close(); print('Redis 7.4.11 PING; PostgreSQL SELECT 1 PASS')"
            ),
        ]
    )
    evidence["before"] = metadata(ids["gateway"])
    evidence["process_tree"] = run(["docker", "top", ids["gateway"]])
    evidence["signal_monotonic"] = time.monotonic()
    run(["docker", "kill", "--signal=TERM", ids["gateway"]])
    evidence["exit_code"] = int(run(["docker", "wait", ids["gateway"]], timeout=40))
    evidence["after"] = metadata(ids["gateway"])
    logs = run(["docker", "logs", ids["gateway"]])
    # docker logs may place Uvicorn messages on stderr; LAB JSON uses stdout.
    (output / "pid1.stdout.log").write_text(logs, encoding="utf-8")
    events = [json.loads(x[4:]) for x in logs.splitlines() if x.startswith("LAB ")]
    evidence["events"] = events
    assert all(e["pid"] == 1 for e in events)
    shutdown = [e for e in events if e["event"] == "shutdown"]
    final = next(e for e in events if e["event"] == "final")
    assert shutdown and len({e["deadline"] for e in shutdown}) == 1
    assert final["deadline"] == shutdown[0]["deadline"]
    assert not final["unclosed"] and not final["active"] and not final["residuals"]
    assert final["disposal"].get("legacy") == "completed"
    assert all(value == "completed" for value in final["disposal"].values())
    evidence["guarded_pool_exercised"] = "guarded" in final["disposal"]
    assert not evidence["after"]["State"]["OOMKilled"]
    evidence["result"] = "PASS"
finally:
    evidence["cleanup"] = run(base + ["down", "--volumes", "--remove-orphans"])
    evidence["remaining_containers"] = run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            "label=com.docker.compose.project=" + project,
            "--format",
            "{{.ID}}",
        ]
    )
    assert not evidence["remaining_containers"]
    (output / "pid1-smoke.json").write_text(
        json.dumps(evidence, indent=2), encoding="utf-8"
    )
print(
    json.dumps(
        {
            "project": project,
            "result": evidence.get("result"),
            "exit_code": evidence.get("exit_code"),
        }
    )
)
