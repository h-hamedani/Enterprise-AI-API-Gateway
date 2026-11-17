"""Windows-owned dependency orchestration; Linux-owned server subprocess tests."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

IMAGE_ID = "sha256:48cc6e6d1f4b92d5d10daf877cddad71f31db572f0374268ad7afea9b2c46ffd"
ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path(sys.argv[1]).resolve()
OUTPUT.mkdir(parents=True, exist_ok=False)
OWNER = "m4-linux-" + uuid4().hex
LABEL = "m4.linux.owner=" + OWNER
NAMES = [OWNER + suffix for suffix in ("-pg", "-redis", "-runner")]
records = {"owner": OWNER, "commands": []}


def docker(*args, timeout=60, check=True):
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    records["commands"].append({"args": args, "exit": result.returncode})
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def inspect(cid):
    row = json.loads(docker("inspect", cid).stdout)[0]
    assert row["Config"]["Labels"]["m4.linux.owner"] == OWNER
    return row


try:
    image = json.loads(docker("image", "inspect", "gateway-m4-cert:py312").stdout)[0]
    assert image["Id"] == IMAGE_ID
    records["image"] = {
        "id": image["Id"],
        "os": image["Os"],
        "arch": image["Architecture"],
    }
    docker("network", "create", "--internal", "--label", LABEL, OWNER)
    pg = docker(
        "run",
        "-d",
        "--pull=never",
        "--name",
        NAMES[0],
        "--label",
        LABEL,
        "--network",
        OWNER,
        "--network-alias",
        "pg",
        "--tmpfs",
        "/var/lib/postgresql/data",
        "-e",
        "POSTGRES_USER=gateway",
        "-e",
        "POSTGRES_PASSWORD=fixture_local_only",
        "-e",
        "POSTGRES_DB=gateway",
        "postgres:16",
    ).stdout.strip()
    rd = docker(
        "run",
        "-d",
        "--pull=never",
        "--name",
        NAMES[1],
        "--label",
        LABEL,
        "--network",
        OWNER,
        "--network-alias",
        "redis",
        "--tmpfs",
        "/data",
        "redis:7-alpine",
        "redis-server",
        "--save",
        "",
        "--appendonly",
        "no",
    ).stdout.strip()
    records["dependencies"] = {"pg": pg, "redis": rd}
    for cid, command in [
        (pg, ["pg_isready", "-U", "gateway"]),
        (rd, ["redis-cli", "ping"]),
    ]:
        inspect(cid)
        until = time.monotonic() + 30
        while docker("exec", cid, *command, timeout=5, check=False).returncode:
            if time.monotonic() >= until:
                raise TimeoutError("Disposable dependency readiness")
            time.sleep(0.1)  # Readiness only; never a concurrency ordering proof.
    version = docker("exec", rd, "redis-cli", "info", "server").stdout
    assert "redis_version:7.4.11" in version
    records["redis_version"] = "7.4.11"
    postgres_version = docker("exec", pg, "postgres", "--version").stdout.strip()
    assert " 16." in postgres_version
    records["postgres_version"] = postgres_version
    services = OUTPUT / "services.json"
    services.write_text(
        json.dumps({"pg": 5432, "redis": 6379, "pg_host": "pg", "redis_host": "redis"}),
        encoding="utf-8",
    )
    args = [
        "run",
        "-d",
        "--pull=never",
        "--name",
        NAMES[2],
        "--label",
        LABEL,
        "--network",
        OWNER,
        "--mount",
        f"type=bind,source={OUTPUT},target=/evidence",
        "--mount",
        f"type=bind,source={ROOT / 'tests/integration/test_m4_platform_shutdown.py'},target=/opt/gateway/tests/integration/test_m4_platform_shutdown.py,readonly",
        "-e",
        "M4_PLATFORM_CERTIFICATION=1",
        "-e",
        "M4_PLATFORM_EVIDENCE_DIR=/evidence",
        "-e",
        "M4_PLATFORM_EXTERNAL_SERVICES_FILE=/evidence/services.json",
        "--entrypoint",
        "python",
        "gateway-m4-cert:py312",
        "-B",
        "-m",
        "pytest",
        "-q",
        "-x",
        "-p",
        "no:cacheprovider",
        "tests/integration/test_m4_platform_shutdown.py",
        "--basetemp",
        "/tmp/m4-linux-platform",
    ]
    if os.environ.get("M4_LINUX_READINESS_ONLY") == "1":
        args += ["-k", "test_managed_process_shutdown[P01]"]
    runner = docker(*args).stdout.strip()
    metadata = inspect(runner)
    assert not any("docker.sock" in x["Destination"] for x in metadata["Mounts"])
    records["runner"] = runner
    result = docker("wait", runner, timeout=300)
    records["runner_exit"] = int(result.stdout.strip())
    logs = docker("logs", runner)
    (OUTPUT / "pytest.log").write_text(logs.stdout + logs.stderr, encoding="utf-8")
    records["runner_state"] = inspect(runner)["State"]
    print(logs.stdout, flush=True)
    assert records["runner_exit"] == 0, (
        "Linux acceptance failure; inspect evidence before retry"
    )
finally:
    # Exact names + owner label + immutable ID; includes uncertain create results.
    for name in reversed(NAMES):
        result = docker(
            "ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}"
        )
        if result.stdout.strip():
            row = inspect(result.stdout.strip())
            assert row["Name"] == "/" + name
            if row["State"]["Running"] and name == NAMES[2]:
                records["forced_runner_cleanup"] = True
            docker("rm", "-f", "-v", row["Id"])
    network = docker("network", "inspect", OWNER, check=False)
    if network.returncode == 0:
        row = json.loads(network.stdout)[0]
        assert row["Labels"]["m4.linux.owner"] == OWNER
        docker("network", "rm", row["Id"])
    records["remaining"] = docker(
        "ps", "-a", "--filter", "label=" + LABEL, "--format", "{{.ID}}"
    ).stdout.strip()
    assert not records["remaining"]
    (OUTPUT / "orchestrator.json").write_text(
        json.dumps(records, indent=2), encoding="utf-8"
    )
