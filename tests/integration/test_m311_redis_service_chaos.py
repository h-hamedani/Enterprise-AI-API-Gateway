"""Opt-in, exclusive Compose Redis outage/recovery acceptance.

Run this file alone with M311_EXCLUSIVE_REDIS_CHAOS=1. It stops the shared
Compose Redis service; ordinary pytest runs must never do so.
"""

from __future__ import annotations

import asyncio
import base64
import os
import subprocess
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import delete, insert, select, update

from app.control_plane.protection import pre_auth_policy
from app.core.config import get_settings
from app.main import create_app
from app.persistence.models import Base
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import CircuitIdentity, CircuitState, circuit_key
from app.redis.circuit_cleanup import circuit_failure_key
from app.redis.concurrency import ResolvedConcurrencyPolicy, semaphore_key
from app.redis.protection_facade import Backend
from app.redis.rate_limit import ResolvedRatePolicy, rate_limit_key
from app.redis.recovery import CircuitCompletionDisposition, TrafficMode
from app.redis.telemetry import RecordingTelemetry

pytestmark = pytest.mark.skipif(
    os.environ.get("M311_EXCLUSIVE_REDIS_CHAOS") != "1",
    reason="Requires exclusive Redis service control; run this file alone.",
)


async def _compose(*args: str) -> None:
    try:
        result = await asyncio.to_thread(
            subprocess.run,
            ("docker", "compose", *args),
            capture_output=True,
            check=False,
            timeout=45,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"Compose {args[0]} timed out")
    if result.returncode:
        pytest.fail(
            f"Compose {args[0]} failed: {result.stderr.decode(errors='replace')}"
        )


async def _until(phase: str, predicate, *, timeout: float = 15) -> None:
    async def wait() -> None:
        while True:
            result = predicate()
            if asyncio.iscoroutine(result):
                result = await result
            if result:
                return
            await asyncio.sleep(0.05)

    try:
        await asyncio.wait_for(wait(), timeout)
    except TimeoutError:
        pytest.fail(f"M3.11-A timed out at {phase}")


@pytest.mark.asyncio
async def test_real_compose_redis_outage_reconciles_and_cuts_over(monkeypatch):
    monkeypatch.setenv("CREDENTIAL_HMAC_SECRET", base64.b64encode(b"m" * 32).decode())
    get_settings.cache_clear()
    app = create_app()
    tenant_id, service_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    redis_keys: list[str] = []
    tenants = Base.metadata.tables["tenants"]
    services = Base.metadata.tables["normal_api_services"]
    versions = Base.metadata.tables["config_versions"]
    stopped = False
    inserted = False
    try:
        async with app.router.lifespan_context(app):
            runtime = app.state.redis_runtime
            db = app.state.db_engine
            coordinator = app.state.recovery_coordinator
            registry = app.state.invalidation_registry
            subscriber = app.state.invalidation_subscriber
            telemetry = RecordingTelemetry()
            coordinator._telemetry = telemetry
            availability_checks: list[TrafficMode] = []
            original_check = runtime.check

            async def observed_check():
                availability_checks.append(coordinator.mode)
                return await original_check()

            runtime.check = observed_check
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                assert (await runtime.check()).available
                async with db.connect() as connection:
                    assert await connection.scalar(select(1)) == 1
                assert coordinator.mode is TrafficMode.NORMAL
                assert app.state.config_reconciler.running

                async def ready() -> bool:
                    return subscriber.ready

                await _until("initial subscriber", ready)
                for path in ("/health/live", "/health/ready", "/health/traffic"):
                    assert (await client.get(path)).status_code == 200
                assert (await client.get("/health/traffic")).json()[
                    "runtime_mode"
                ] == "NORMAL"

                async with db.begin() as connection:
                    await connection.execute(
                        insert(tenants).values(
                            id=tenant_id,
                            name=f"m311-{tenant_id}",
                            status="ACTIVE",
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    await connection.execute(
                        insert(services).values(
                            id=service_id,
                            tenant_id=tenant_id,
                            slug=f"m311-{service_id.hex[:20]}",
                            display_name="before",
                            upstream_base_url="https://example.invalid",
                            status="DISABLED",
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
                    await connection.execute(
                        insert(versions).values(
                            tenant_id=tenant_id, version=1, updated_at=now
                        )
                    )
                inserted = True

                state = {"name": None, "calls": 0, "gate": False}
                entered = asyncio.Event()
                proceed = asyncio.Event()

                async def reconcile_current_state(_tenant_id):
                    state["calls"] += 1
                    if state["gate"]:
                        entered.set()
                        await proceed.wait()
                    async with db.connect() as connection:
                        state["name"] = await connection.scalar(
                            select(services.c.display_name).where(
                                services.c.id == service_id
                            )
                        )

                async def event_callback(_event):
                    pytest.fail("No event should be delivered during Redis outage")

                await registry.register_required_consumer(
                    tenant_id,
                    "SERVICE",
                    service_id,
                    event_callback,
                    reconcile_current_state,
                )
                assert (
                    await app.state.config_reconciler.reconcile_once() == "pass_success"
                )
                assert registry.is_tenant_reconciled(tenant_id)
                assert state["name"] == "before"

                rate = ResolvedRatePolicy(
                    uuid4(), tenant_id, RateScopeType.API_KEY, uuid4(), 4, 60
                )
                concurrency = ResolvedConcurrencyPolicy(
                    uuid4(), tenant_id, RateScopeType.API_KEY, uuid4(), 4
                )
                circuit = CircuitIdentity._create(
                    tenant_id, "route", uuid4(), service_id
                )
                redis_keys = [
                    rate_limit_key(rate),
                    semaphore_key(concurrency),
                    circuit_key(circuit),
                    circuit_failure_key(circuit),
                    rate_limit_key(pre_auth_policy("127.0.0.1", b"m" * 32)),
                ]
                assert (await app.state.recovery_rate_limiter.evaluate([rate])).allowed
                redis_lease = await app.state.recovery_concurrency.acquire(
                    [concurrency]
                )
                assert (
                    redis_lease.acquired and redis_lease.handle.backend is Backend.REDIS
                )
                redis_circuit = (
                    await app.state.recovery_circuit.check_or_claim_eligibility(circuit)
                )
                assert redis_circuit.eligible
                assert redis_circuit.completion_handle.backend is Backend.REDIS

                stopped = True
                await _compose("stop", "redis")

                async def unavailable() -> bool:
                    return not (await runtime.check()).available

                await _until("Redis stop", unavailable)
                assert (await app.state.recovery_rate_limiter.evaluate([rate])).allowed
                assert coordinator.mode is TrafficMode.DEGRADED_REDIS
                assert not (
                    await app.state.recovery_rate_limiter.evaluate([rate])
                ).allowed
                local_lease = await app.state.recovery_concurrency.acquire(
                    [concurrency]
                )
                assert (
                    local_lease.acquired and local_lease.handle.backend is Backend.LOCAL
                )
                assert not (
                    await app.state.recovery_concurrency.acquire([concurrency])
                ).acquired
                local_circuit = (
                    await app.state.recovery_circuit.check_or_claim_eligibility(circuit)
                )
                assert local_circuit.eligible
                assert local_circuit.state is CircuitState.DEGRADED_HALF_OPEN
                assert local_circuit.completion_handle.backend is Backend.LOCAL
                assert not (
                    await app.state.recovery_circuit.check_or_claim_eligibility(circuit)
                ).eligible
                assert (await client.get("/health/live")).status_code == 200
                assert (await client.get("/health/ready")).status_code == 503
                assert (await client.get("/health/traffic")).json()[
                    "runtime_mode"
                ] == "DEGRADED_REDIS"

                headers = {"Authorization": "Bearer adm_" + "A" * 43}
                for _ in range(5):
                    assert (
                        await client.get("/api/v1/admin/applications", headers=headers)
                    ).status_code == 401
                assert (
                    await client.get("/api/v1/admin/applications", headers=headers)
                ).status_code == 429

                async with db.begin() as connection:
                    await connection.execute(
                        update(services)
                        .where(services.c.id == service_id)
                        .values(display_name="after", updated_at=datetime.now(UTC))
                    )
                    await connection.execute(
                        update(versions)
                        .where(versions.c.tenant_id == tenant_id)
                        .values(version=2, updated_at=datetime.now(UTC))
                    )
                assert state["name"] == "before"
                assert registry.observed_version(tenant_id) == 1
                assert not (await runtime.check()).available
                state["gate"] = True

                await _compose("start", "redis")

                async def available() -> bool:
                    return (await runtime.check()).available

                await _until("Redis restoration", available)
                stopped = False
                await _until("reconciliation entered", entered.is_set)
                assert coordinator.mode is TrafficMode.RECOVERING_REDIS
                assert subscriber.ready
                assert not registry.is_tenant_reconciled(tenant_id)
                assert state["name"] == "before"
                assert (await client.get("/health/traffic")).json()[
                    "runtime_mode"
                ] == "RECOVERING_REDIS"
                proceed.set()

                async def normal() -> bool:
                    return coordinator.mode is TrafficMode.NORMAL

                await _until("NORMAL cutover", normal)
                assert registry.observed_version(tenant_id) == 2
                assert registry.is_tenant_reconciled(tenant_id)
                assert state["name"] == "after"
                assert TrafficMode.RECOVERING_REDIS in availability_checks
                assert (await client.get("/health/ready")).status_code == 200
                assert (await client.get("/health/traffic")).json()[
                    "runtime_mode"
                ] == "NORMAL"
                assert (await app.state.recovery_rate_limiter.evaluate([rate])).allowed
                new_lease = await app.state.recovery_concurrency.acquire([concurrency])
                assert new_lease.acquired and new_lease.handle.backend is Backend.REDIS
                assert (
                    await app.state.recovery_concurrency.release(
                        [concurrency], local_lease.handle
                    )
                ).released
                assert (
                    await app.state.recovery_concurrency.release(
                        [concurrency], redis_lease.handle
                    )
                ).released
                assert (
                    await app.state.recovery_concurrency.release(
                        [concurrency], new_lease.handle
                    )
                ).released
                assert (
                    await app.state.recovery_circuit.record_failure(
                        circuit, local_circuit.completion_handle
                    )
                ).applied
                assert redis_circuit.completion_handle.backend is Backend.REDIS
                redis_completion = await app.state.recovery_circuit.record_success(
                    circuit, redis_circuit.completion_handle
                )
                assert redis_completion.disposition in {
                    CircuitCompletionDisposition.APPLIED,
                    CircuitCompletionDisposition.STALE,
                }
                outcomes = [
                    event.dimensions.get("outcome")
                    for event in telemetry.events
                    if event.family == "recovery"
                ]
                assert "recovery_started" in outcomes
                assert "recovery_succeeded" in outcomes
                modes = [
                    event.dimensions.get("mode")
                    for event in telemetry.events
                    if event.family == "traffic"
                ]
                assert "DEGRADED_REDIS" in modes
                assert "RECOVERING_REDIS" in modes
                assert "NORMAL" in modes
    finally:
        if stopped:
            await _compose("start", "redis")
        if redis_keys:
            redis = Redis.from_url(get_settings().redis_url)
            try:
                await redis.delete(*redis_keys)
            finally:
                await redis.aclose()
        get_settings.cache_clear()
        if inserted:
            # The application lifespan disposes its engine; use a fresh one for cleanup.
            from sqlalchemy.ext.asyncio import create_async_engine

            db = create_async_engine(get_settings().postgres_dsn)
            try:
                async with db.begin() as connection:
                    await connection.execute(
                        delete(versions).where(versions.c.tenant_id == tenant_id)
                    )
                    await connection.execute(
                        delete(services).where(services.c.id == service_id)
                    )
                    await connection.execute(
                        delete(tenants).where(tenants.c.id == tenant_id)
                    )
            finally:
                await db.dispose()
