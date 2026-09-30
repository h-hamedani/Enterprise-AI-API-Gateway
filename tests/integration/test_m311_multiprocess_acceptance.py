"""Opt-in real two-process Redis/PostgreSQL M3.11-B acceptance.

Run this file alone with M311_MULTIPROCESS_ACCEPTANCE=1. The two test-only
worker processes use the ordinary application lifespan and loopback control.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete, insert, update
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings
from app.persistence.models import Base
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import CircuitIdentity, circuit_key
from app.redis.circuit_cleanup import circuit_failure_key
from app.redis.concurrency import ResolvedConcurrencyPolicy, semaphore_key
from app.redis.namespace import M3_INVALIDATION_CHANNEL
from app.redis.rate_limit import ResolvedRatePolicy, rate_limit_key

pytestmark = pytest.mark.skipif(
    os.environ.get("M311_MULTIPROCESS_ACCEPTANCE") != "1",
    reason="Opt-in two-process acceptance; requires real Redis and PostgreSQL.",
)

_WORKER = Path(__file__).with_name("m311_multiprocess_worker.py")
_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def event_loop_policy():
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


async def _command(port: int, **payload):
    async def exchange():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write((json.dumps(payload) + "\n").encode())
            await writer.drain()
            response = json.loads(await reader.readline())
            assert response["ok"], f"Worker command failed: {response['error_type']}"
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    return await asyncio.wait_for(exchange(), timeout=20)


async def _until(label: str, predicate, timeout: float = 15):
    async def poll():
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.05)

    try:
        return await asyncio.wait_for(poll(), timeout)
    except TimeoutError:
        pytest.fail(f"M3.11-B timed out at {label}")


async def _start_worker():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        (
            str(Path(sys.prefix) / "Lib" / "site-packages"),
            str(_ROOT),
            env.get("PYTHONPATH", ""),
        )
    )
    env["CREDENTIAL_HMAC_SECRET"] = base64.b64encode(b"m" * 32).decode()
    proc = await asyncio.to_thread(
        subprocess.Popen,
        (sys._base_executable, str(_WORKER)),
        cwd=_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        line = await asyncio.wait_for(asyncio.to_thread(proc.stdout.readline), 20)
        assert line, "Worker exited before announcing its port"
        port = int(line)

        async def ready():
            result = await _command(port, cmd="health")
            return (
                result
                if (
                    result["postgres"]
                    and result["redis"]
                    and result["subscriber"]
                    and result["reconciler"]
                    and result["mode"] == "NORMAL"
                    and result["live"] == 200
                    and result["ready"] == 200
                    and result["traffic"] == "NORMAL"
                )
                else None
            )

        health = await _until("worker readiness", ready)
        assert health["pid"] == proc.pid
        return proc, port, health
    except BaseException:
        await _stop_worker(proc)
        raise


async def _stop_worker(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            await asyncio.to_thread(proc.wait, 5)
        except subprocess.TimeoutExpired:
            proc.kill()
            await asyncio.to_thread(proc.wait, 5)
    if proc.stdout is not None:
        proc.stdout.close()


def _rate(
    tenant: UUID,
    limit: int,
    *,
    scope: UUID | None = None,
    scope_type: str = "API_KEY",
    authoritative: bool = False,
):
    return {
        "tenant": str(tenant),
        "policy": str(uuid4()),
        "scope": str(scope or uuid4()),
        "scope_type": scope_type,
        "limit": limit,
        "authoritative": authoritative,
    }


def _circuit(tenant: UUID, service: UUID, route: UUID):
    return {"tenant": str(tenant), "service": str(service), "route": str(route)}


@pytest.mark.asyncio
async def test_two_real_processes_share_redis_and_isolate_recovery(monkeypatch):
    monkeypatch.setenv("CREDENTIAL_HMAC_SECRET", base64.b64encode(b"m" * 32).decode())
    get_settings.cache_clear()
    settings = get_settings()
    db = create_async_engine(settings.postgres_dsn)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    tenant, service = uuid4(), uuid4()
    active_route, disabled_route = uuid4(), uuid4()
    now = datetime.now(UTC)
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    routes = Base.metadata.tables["normal_api_routes"]
    versions = Base.metadata.tables["config_versions"]
    policies = Base.metadata.tables["rate_limit_policies"]
    rate = _rate(tenant, 2, scope=service, scope_type="SERVICE", authoritative=True)
    concurrency = _rate(
        tenant, 1, scope=active_route, scope_type="ROUTE", authoritative=True
    )
    processes = []
    keys: set[str] = set()
    inserted = False
    try:
        assert await redis.ping()
        async with db.begin() as connection:
            await connection.execute(
                insert(tenants).values(
                    id=tenant,
                    name=f"m311b-{tenant}",
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            await connection.execute(
                insert(services).values(
                    id=service,
                    tenant_id=tenant,
                    slug=f"m311b-{service.hex[:20]}",
                    display_name="before",
                    upstream_base_url="https://example.invalid",
                    status="ACTIVE",
                    request_body_limit_bytes=1024,
                    connect_timeout_seconds=5,
                    pool_timeout_seconds=5,
                    write_timeout_seconds=5,
                    read_idle_timeout_seconds=5,
                    pre_response_timeout_seconds=5,
                    created_at=now,
                    updated_at=now,
                )
            )
            for route_id, status in (
                (active_route, "ACTIVE"),
                (disabled_route, "DISABLED"),
            ):
                await connection.execute(
                    insert(routes).values(
                        id=route_id,
                        tenant_id=tenant,
                        service_id=service,
                        path_pattern=f"/{route_id.hex}",
                        method="GET",
                        upstream_path_template="/upstream",
                        priority=0,
                        status=status,
                        created_at=now,
                        updated_at=now,
                    )
                )
            await connection.execute(
                insert(versions).values(
                    tenant_id=tenant,
                    version=1,
                    updated_at=now,
                )
            )
            for policy, rate_count, window, concurrent in (
                (rate, 2, 60, None),
                (concurrency, None, None, 1),
            ):
                await connection.execute(
                    insert(policies).values(
                        id=UUID(policy["policy"]),
                        tenant_id=tenant,
                        scope_type=policy["scope_type"],
                        scope_id=UUID(policy["scope"]),
                        requests_per_window=rate_count,
                        window_seconds=window,
                        max_concurrency=concurrent,
                        degraded_factor=Decimal("0.25"),
                        enabled=True,
                        created_at=now,
                        updated_at=now,
                    )
                )
        inserted = True

        process_a, a, ready_a = await _start_worker()
        processes.append(process_a)
        process_b, b, ready_b = await _start_worker()
        processes.append(process_b)
        assert ready_a["pid"] != ready_b["pid"]

        keys.add(
            rate_limit_key(
                ResolvedRatePolicy(
                    UUID(rate["policy"]),
                    tenant,
                    RateScopeType.SERVICE,
                    UUID(rate["scope"]),
                    2,
                    60,
                )
            )
        )
        assert (await _command(a, cmd="rate", **rate))["allowed"]
        assert (await _command(b, cmd="rate", **rate))["allowed"]
        assert not (await _command(a, cmd="rate", **rate))["allowed"]

        keys.add(
            semaphore_key(
                ResolvedConcurrencyPolicy(
                    UUID(concurrency["policy"]),
                    tenant,
                    RateScopeType.ROUTE,
                    UUID(concurrency["scope"]),
                    1,
                )
            )
        )
        first = await _command(a, cmd="acquire", **concurrency)
        assert first["acquired"] and first["backend"] == "REDIS"
        assert not (await _command(b, cmd="acquire", **concurrency))["acquired"]
        assert (await _command(a, cmd="release", key=first["key"], **concurrency))[
            "released"
        ]
        second = await _command(b, cmd="acquire", **concurrency)
        assert second["acquired"] and second["backend"] == "REDIS"
        assert (await _command(b, cmd="release", key=second["key"], **concurrency))[
            "released"
        ]

        shared = _circuit(tenant, service, active_route)
        shared_identity = CircuitIdentity._create(
            tenant, "route", active_route, service
        )
        keys.update(
            (circuit_key(shared_identity), circuit_failure_key(shared_identity))
        )
        for attempt in range(settings.circuit_failure_threshold):
            initial = await _command(a, cmd="circuit_check", **shared)
            assert initial["eligible"] and initial["state"] == "CLOSED"
            opened = await _command(
                a,
                cmd="circuit_complete",
                key=initial["key"],
                success=False,
                **shared,
            )
            if attempt < settings.circuit_failure_threshold - 1:
                assert opened["state"] == "CLOSED"
        assert opened["applied"] and opened["state"] == "OPEN"
        observed = await _command(b, cmd="circuit_check", **shared)
        assert not observed["eligible"] and observed["state"] == "OPEN"

        async def probe_ready():
            result = await _command(b, cmd="circuit_check", **shared)
            return result if result["eligible"] else None

        probe = await _until(
            "shared half-open probe",
            probe_ready,
            settings.circuit_open_duration_ms / 1000 + 5,
        )
        assert probe["state"] == "HALF_OPEN"
        duplicate = await _command(a, cmd="circuit_check", **shared)
        assert not duplicate["eligible"] and duplicate["state"] == "HALF_OPEN"
        closed = await _command(
            b, cmd="circuit_complete", key=probe["key"], success=True, **shared
        )
        assert closed["applied"] and closed["state"] == "CLOSED"
        assert (await _command(a, cmd="circuit_check", **shared))["state"] == "CLOSED"

        for port in (a, b):
            registered = await _command(
                port, cmd="register", tenant=str(tenant), service=str(service)
            )
            assert registered["result"] == "pass_success"
            assert (await _command(port, cmd="consumer", tenant=str(tenant)))[
                "value"
            ] == "before"

        async def change_config(version: int, name: str):
            async with db.begin() as connection:
                await connection.execute(
                    update(services)
                    .where(services.c.id == service)
                    .values(display_name=name, updated_at=datetime.now(UTC))
                )
                await connection.execute(
                    update(versions)
                    .where(versions.c.tenant_id == tenant)
                    .values(version=version, updated_at=datetime.now(UTC))
                )
            payload = json.dumps(
                {
                    "tenant_id": str(tenant),
                    "resource_type": "SERVICE",
                    "resource_id": str(service),
                    "version": version,
                }
            )
            assert await redis.publish(M3_INVALIDATION_CHANNEL, payload) >= 1

        await change_config(2, "both")
        for port in (a, b):

            async def updated(port=port):
                state = await _command(port, cmd="consumer", tenant=str(tenant))
                return (
                    state
                    if state["value"] == "both" and state["version"] == 2
                    else None
                )

            assert (await _until("dual invalidation", updated))["calls"] >= 2

        assert not (await _command(b, cmd="subscriber_stop"))["ready"]
        await change_config(3, "repaired")

        async def a_updated():
            state = await _command(a, cmd="consumer", tenant=str(tenant))
            return (
                state
                if state["value"] == "repaired" and state["version"] == 3
                else None
            )

        await _until("A invalidation while B disconnected", a_updated)
        stale = await _command(b, cmd="consumer", tenant=str(tenant))
        assert stale["value"] == "both" and stale["version"] == 2
        assert (await _command(b, cmd="reconcile"))["result"] == "pass_success"
        repaired = await _command(b, cmd="consumer", tenant=str(tenant))
        assert repaired["value"] == "repaired" and repaired["version"] == 3
        assert repaired["reconciled"]
        await _command(b, cmd="subscriber_start")
        await _until("B resubscription", lambda: _subscriber_ready(b))

        # Existing integration-test convention: substitute a genuinely refused
        # socket in A's RedisRuntime only. B retains its original real client.
        await _command(a, cmd="fault")
        isolated_rate = _rate(tenant, 1)
        keys.add(
            rate_limit_key(
                ResolvedRatePolicy(
                    UUID(isolated_rate["policy"]),
                    tenant,
                    RateScopeType.API_KEY,
                    UUID(isolated_rate["scope"]),
                    1,
                    60,
                )
            )
        )
        assert (await _command(a, cmd="rate", **isolated_rate))["allowed"]
        degraded = await _command(a, cmd="health")
        assert degraded["mode"] == "DEGRADED_REDIS" and degraded["generation"] == 1
        assert not (await _command(a, cmd="rate", **isolated_rate))["allowed"]
        assert (await _command(b, cmd="rate", **isolated_rate))["allowed"]
        healthy_b = await _command(b, cmd="health")
        assert healthy_b["mode"] == "NORMAL" and healthy_b["generation"] is None

        local_policy = _rate(tenant, 1)
        keys.add(
            semaphore_key(
                ResolvedConcurrencyPolicy(
                    UUID(local_policy["policy"]),
                    tenant,
                    RateScopeType.API_KEY,
                    UUID(local_policy["scope"]),
                    1,
                )
            )
        )
        local_lease = await _command(a, cmd="acquire", **local_policy)
        assert local_lease["acquired"] and local_lease["backend"] == "LOCAL"
        b_lease = await _command(b, cmd="acquire", **local_policy)
        assert b_lease["acquired"] and b_lease["backend"] == "REDIS"

        local_circuit = _circuit(tenant, service, uuid4())
        local_identity = CircuitIdentity._create(
            tenant, "route", UUID(local_circuit["route"]), service
        )
        keys.update((circuit_key(local_identity), circuit_failure_key(local_identity)))
        a_circuit = await _command(a, cmd="circuit_check", **local_circuit)
        assert a_circuit["eligible"] and a_circuit["backend"] == "LOCAL"
        assert "token" in a_circuit
        assert (await _command(b, cmd="circuit_check", **local_circuit))[
            "state"
        ] == "CLOSED"

        await _command(a, cmd="gate_reconcile")
        await _command(a, cmd="restore")

        async def recovering():
            health = await _command(a, cmd="health")
            entered = await _command(a, cmd="reconcile_entered")
            return (
                health
                if (health["mode"] == "RECOVERING_REDIS" and entered["entered"])
                else None
            )

        await _until("A reconciliation during recovery", recovering)
        independent_b = await _command(b, cmd="health")
        assert independent_b["mode"] == "NORMAL"
        independent_rate = _rate(tenant, 2)
        keys.add(
            rate_limit_key(
                ResolvedRatePolicy(
                    UUID(independent_rate["policy"]),
                    tenant,
                    RateScopeType.API_KEY,
                    UUID(independent_rate["scope"]),
                    2,
                    60,
                )
            )
        )
        assert (await _command(b, cmd="rate", **independent_rate))["allowed"]
        await _command(a, cmd="ungate_reconcile")

        async def a_normal():
            health = await _command(a, cmd="health")
            return health if health["mode"] == "NORMAL" else None

        recovered = await _until("A local cutover", a_normal)
        assert recovered["generation"] is None and recovered["retired"] >= 1
        assert (await _command(b, cmd="health"))["mode"] == "NORMAL"
        assert not (
            await _command(
                b,
                cmd="release",
                key=local_lease["key"],
                foreign=local_lease,
                **local_policy,
            )
        )["released"]
        foreign_circuit = await _command(
            b,
            cmd="circuit_complete",
            key=a_circuit["key"],
            foreign=a_circuit,
            success=False,
            **local_circuit,
        )
        assert foreign_circuit == {
            "ok": True,
            "applied": False,
            "disposition": "UNKNOWN_HANDLE",
            "state": None,
        }
        assert (
            await _command(a, cmd="release", key=local_lease["key"], **local_policy)
        )["released"]
        assert (await _command(b, cmd="release", key=b_lease["key"], **local_policy))[
            "released"
        ]
        assert (
            await _command(
                a,
                cmd="circuit_complete",
                key=a_circuit["key"],
                success=False,
                **local_circuit,
            )
        )["applied"]

        retained = [
            CircuitIdentity._create(tenant, "route", route, service)
            for route in (active_route, disabled_route)
        ]
        orphans = [
            CircuitIdentity._create(tenant, "route", uuid4(), service) for _ in range(7)
        ]
        for identity in (*retained, *orphans):
            primary, failures = circuit_key(identity), circuit_failure_key(identity)
            keys.update((primary, failures))
            if identity == shared_identity:
                await redis.delete(primary, failures)
            await redis.set(primary, "synthetic")
            await redis.set(failures, "synthetic")

        async def sweep():
            outcomes = await asyncio.gather(
                _command(a, cmd="sweep"), _command(b, cmd="sweep")
            )
            assert all(0 <= result["examined"] <= 128 for result in outcomes)
            gone = [
                not await redis.exists(circuit_key(i), circuit_failure_key(i))
                for i in orphans
            ]
            return all(gone)

        await _until("concurrent bounded orphan sweeps", sweep, 15)
        for identity in retained:
            assert (
                await redis.exists(circuit_key(identity), circuit_failure_key(identity))
                == 2
            )
        assert (await _command(a, cmd="health"))["mode"] == "NORMAL"
        assert (await _command(b, cmd="health"))["mode"] == "NORMAL"
        for port in (a, b):
            events = (await _command(port, cmd="telemetry"))["events"]
            assert events
            encoded = json.dumps(events)
            assert (
                re.search(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                    encoded,
                )
                is None
            )
            assert "gw:v1:" not in encoded
            assert "127.0.0.1" not in encoded
    finally:
        for proc in reversed(processes):
            await _stop_worker(proc)
        if keys:
            await redis.delete(*keys)
        await redis.aclose()
        if inserted:
            async with db.begin() as connection:
                await connection.execute(
                    delete(versions).where(versions.c.tenant_id == tenant)
                )
                await connection.execute(
                    delete(policies).where(policies.c.tenant_id == tenant)
                )
                await connection.execute(
                    delete(routes).where(routes.c.tenant_id == tenant)
                )
                await connection.execute(
                    delete(services).where(services.c.id == service)
                )
                await connection.execute(delete(tenants).where(tenants.c.id == tenant))
        await db.dispose()
        get_settings.cache_clear()


async def _subscriber_ready(port):
    return (await _command(port, cmd="health"))["subscriber"]
