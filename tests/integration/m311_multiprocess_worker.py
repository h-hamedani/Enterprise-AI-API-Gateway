"""Test-only control process for opt-in M3.11-B acceptance.

The loopback protocol carries synthetic identifiers only. It never exposes a
production endpoint or changes application startup/lifespan behavior.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from uuid import UUID, uuid4

import httpx
from redis.asyncio import Redis
from sqlalchemy import select

from app.core.config import get_settings
from app.main import create_app
from app.persistence.models import Base
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import CircuitIdentity, circuit_key
from app.redis.circuit_cleanup import CircuitAuthority, circuit_failure_key
from app.redis.concurrency import ResolvedConcurrencyPolicy
from app.redis.protection_facade import CircuitCompletionHandle, ConcurrencyHandle
from app.redis.rate_limit import ResolvedRatePolicy
from app.redis.recovery import Backend
from app.redis.telemetry import RecordingTelemetry


class SweepProbe:
    """Test-only Redis call recorder; all operations still reach real Redis."""

    def __init__(self, redis, target: CircuitIdentity):
        self.redis = redis
        self.target_keys = (circuit_key(target), circuit_failure_key(target))
        self.scan_calls = 0
        self.returned_entries = 0
        self.delete_counts: list[int] = []

    async def scan(self, **kwargs):
        self.scan_calls += 1
        cursor, page = await self.redis.scan(**kwargs)
        self.returned_entries += len(page)
        return cursor, page

    async def delete(self, *keys):
        count = await self.redis.delete(*keys)
        if keys == self.target_keys:
            self.delete_counts.append(count)
        return count


class Worker:
    def __init__(self, app):
        self.app = app
        self.handles: dict[str, object] = {}
        self.consumer_values: dict[str, str | None] = {}
        self.consumer_calls: dict[str, int] = {}
        self.original_client = None
        self.refused_client = None
        self.reconcile_gate = asyncio.Event()
        self.reconcile_gate.set()
        self.reconcile_entered = asyncio.Event()
        self.telemetry = RecordingTelemetry()
        self.sweep_target: CircuitIdentity | None = None
        self.sweep_observed: CircuitIdentity | None = None
        self.sweep_initial: CircuitAuthority | None = None
        self.sweep_final: CircuitAuthority | None = None
        self.sweep_lookup_calls = 0
        self.sweep_initial_pending = 0
        self.sweep_entered = asyncio.Event()
        self.sweep_release = asyncio.Event()
        self.sweep_probe: SweepProbe | None = None
        for component in (
            app.state.recovery_coordinator,
            app.state.invalidation_subscriber,
            app.state.config_reconciler,
            app.state.circuit_orphan_sweeper,
        ):
            component._telemetry = self.telemetry

    async def policy_values(self, data):
        if not data.get("authoritative"):
            return data
        table = Base.metadata.tables["rate_limit_policies"]
        async with self.app.state.db_engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(table).where(table.c.id == UUID(data["policy"]))
                    )
                )
                .mappings()
                .one()
            )
        assert row["tenant_id"] == UUID(data["tenant"]) and row["enabled"]
        return {
            "policy": str(row["id"]),
            "tenant": str(row["tenant_id"]),
            "scope_type": row["scope_type"].value,
            "scope": str(row["scope_id"]),
            "limit": row["requests_per_window"]
            if data["cmd"] == "rate"
            else row["max_concurrency"],
            "window": row["window_seconds"],
            "factor": row["degraded_factor"],
        }

    @staticmethod
    def rate_policy(data):
        return ResolvedRatePolicy(
            UUID(data["policy"]),
            UUID(data["tenant"]),
            RateScopeType(data.get("scope_type", "API_KEY")),
            UUID(data["scope"]),
            data["limit"],
            data.get("window") or 60,
            degraded_factor=data.get("factor"),
        )

    @staticmethod
    def concurrency_policy(data):
        return ResolvedConcurrencyPolicy(
            UUID(data["policy"]),
            UUID(data["tenant"]),
            RateScopeType(data.get("scope_type", "API_KEY")),
            UUID(data["scope"]),
            data["limit"],
            degraded_factor=data.get("factor"),
        )

    @staticmethod
    def identity(data):
        return CircuitIdentity._create(
            UUID(data["tenant"]),
            "route",
            UUID(data["route"]),
            UUID(data["service"]),
        )

    async def execute(self, data):
        app = self.app
        command = data["cmd"]
        coordinator = app.state.recovery_coordinator
        registry = app.state.invalidation_registry
        if command == "health":
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                live = await client.get("/health/live")
                ready = await client.get("/health/ready")
                traffic = await client.get("/health/traffic")
            async with app.state.db_engine.connect() as connection:
                postgres = await connection.scalar(select(1)) == 1
            return {
                "pid": os.getpid(),
                "postgres": postgres,
                "redis": (await app.state.redis_runtime.check()).available,
                "subscriber": app.state.invalidation_subscriber.ready,
                "reconciler": app.state.config_reconciler.running,
                "mode": coordinator.mode.value,
                "generation": coordinator.active_generation_id,
                "retired": len(coordinator.retired_generations),
                "live": live.status_code,
                "ready": ready.status_code,
                "traffic": traffic.json()["runtime_mode"],
            }
        if command == "rate":
            result = await app.state.recovery_rate_limiter.evaluate(
                [self.rate_policy(await self.policy_values(data))]
            )
            return {"allowed": result.allowed, "mode": coordinator.mode.value}
        if command == "acquire":
            result = await app.state.recovery_concurrency.acquire(
                [self.concurrency_policy(await self.policy_values(data))]
            )
            if result.handle is None:
                return {"acquired": False}
            key = str(uuid4())
            self.handles[key] = result.handle
            handle = result.handle
            return {
                "acquired": True,
                "key": key,
                "backend": handle.backend.value,
                "lease": str(handle.raw_lease_id),
                "owner": str(handle.owner_id),
                "generation": handle.local_generation_id,
            }
        if command == "release":
            handle = self.handles.get(data["key"])
            if handle is None and "foreign" in data:
                foreign = data["foreign"]
                handle = ConcurrencyHandle(
                    UUID(foreign["lease"]),
                    Backend(foreign["backend"]),
                    UUID(foreign["owner"]),
                    foreign["generation"],
                )
            result = await app.state.recovery_concurrency.release(
                [self.concurrency_policy(await self.policy_values(data))], handle
            )
            return {"released": result.released}
        if command == "circuit_check":
            result = await app.state.recovery_circuit.check_or_claim_eligibility(
                self.identity(data)
            )
            response = {"eligible": result.eligible, "state": result.state.value}
            if result.completion_handle is not None:
                key = str(uuid4())
                self.handles[key] = result.completion_handle
                response.update(key=key, backend=result.completion_handle.backend.value)
                if isinstance(result.completion_handle.raw_token, UUID):
                    response.update(
                        token=str(result.completion_handle.raw_token),
                        owner=str(result.completion_handle.owner_id),
                        generation=result.completion_handle.local_generation_id,
                    )
            return response
        if command == "circuit_complete":
            handle = self.handles.get(data["key"])
            if handle is None and "foreign" in data:
                foreign = data["foreign"]
                handle = CircuitCompletionHandle(
                    UUID(foreign["token"]),
                    Backend.LOCAL,
                    UUID(foreign["owner"]),
                    foreign["generation"],
                    self.identity(data),
                    None,
                )
            method = (
                app.state.recovery_circuit.record_success
                if data["success"]
                else app.state.recovery_circuit.record_failure
            )
            result = await method(self.identity(data), handle)
            return {
                "applied": result.applied,
                "disposition": result.disposition.value,
                "state": result.resulting_state.value
                if result.resulting_state is not None
                else None,
            }
        if command == "register":
            tenant, service = UUID(data["tenant"]), UUID(data["service"])
            table = Base.metadata.tables["normal_api_services"]

            async def load(_ignored):
                self.reconcile_entered.set()
                await self.reconcile_gate.wait()
                async with app.state.db_engine.connect() as connection:
                    value = await connection.scalar(
                        select(table.c.display_name).where(table.c.id == service)
                    )
                self.consumer_values[data["tenant"]] = value
                self.consumer_calls[data["tenant"]] = (
                    self.consumer_calls.get(data["tenant"], 0) + 1
                )

            await registry.register_required_consumer(
                tenant, "SERVICE", service, load, load
            )
            return {"result": await app.state.config_reconciler.reconcile_once()}
        if command == "consumer":
            tenant = UUID(data["tenant"])
            return {
                "value": self.consumer_values.get(data["tenant"]),
                "calls": self.consumer_calls.get(data["tenant"], 0),
                "version": registry.observed_version(tenant),
                "reconciled": registry.is_tenant_reconciled(tenant),
            }
        if command == "subscriber_stop":
            await app.state.invalidation_subscriber.stop()
            return {"ready": app.state.invalidation_subscriber.ready}
        if command == "subscriber_start":
            await app.state.invalidation_subscriber.start()
            return {"ready": app.state.invalidation_subscriber.ready}
        if command == "reconcile":
            return {"result": await app.state.config_reconciler.reconcile_once()}
        if command == "gate_reconcile":
            self.reconcile_entered.clear()
            self.reconcile_gate.clear()
            return {"gated": True}
        if command == "reconcile_entered":
            return {"entered": self.reconcile_entered.is_set()}
        if command == "ungate_reconcile":
            self.reconcile_gate.set()
            return {"gated": False}
        if command == "telemetry":
            return {
                "events": [
                    {"family": item.family, "dimensions": dict(item.dimensions)}
                    for item in self.telemetry.events
                ]
            }
        if command == "fault":
            runtime = app.state.redis_runtime
            assert self.original_client is None
            self.original_client = runtime.client
            self.refused_client = Redis.from_url(
                "redis://127.0.0.1:1/0",
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.2,
            )
            runtime._client = self.refused_client
            return {"faulted": True}
        if command == "restore":
            assert self.original_client is not None
            app.state.redis_runtime._client = self.original_client
            await self.refused_client.aclose()
            self.original_client = None
            self.refused_client = None
            return {"restored": True}
        if command == "arm_sweep":
            assert self.sweep_target is None
            sweeper = app.state.circuit_orphan_sweeper
            await sweeper.stop()
            assert not sweeper.running
            self.sweep_target = self.identity(data)
            original_lookup = sweeper._lookup

            async def fenced_lookup(identity):
                result = await original_lookup(identity)
                if identity == self.sweep_target:
                    self.sweep_lookup_calls += 1
                    if self.sweep_lookup_calls == 1:
                        self.sweep_observed = identity
                        self.sweep_initial = result
                        if result is CircuitAuthority.RETIRED:
                            self.sweep_entered.set()
                            await self.sweep_release.wait()
                    elif self.sweep_lookup_calls == 2:
                        self.sweep_final = result
                return result

            sweeper._lookup = fenced_lookup
            self.sweep_initial_pending = len(sweeper._pending)
            self.sweep_probe = SweepProbe(sweeper._redis, self.sweep_target)
            sweeper._redis = self.sweep_probe
            return {"armed": True, "background_sweeper_running": sweeper.running}
        if command == "sweep_fence_state":
            sweeper = app.state.circuit_orphan_sweeper
            return {
                "entered": self.sweep_entered.is_set(),
                "identity": {
                    "tenant": str(self.sweep_observed.tenant_id),
                    "route": str(self.sweep_observed.target_id),
                    "service": str(self.sweep_observed.dimension_id),
                }
                if self.sweep_observed is not None
                else None,
                "initial": self.sweep_initial.value
                if self.sweep_initial is not None
                else None,
                "final": self.sweep_final.value
                if self.sweep_final is not None
                else None,
                "lookup_calls": self.sweep_lookup_calls,
                "scan_calls": self.sweep_probe.scan_calls,
                "returned_entries": self.sweep_probe.returned_entries,
                "initial_pending": self.sweep_initial_pending,
                "pending_entries": len(sweeper._pending),
                "delete_counts": self.sweep_probe.delete_counts,
                "max_candidates": sweeper._max_candidates,
                "max_scan_calls": sweeper._max_scan_calls,
                "max_entries_examined": sweeper._max_entries_examined,
                "background_sweeper_running": sweeper.running,
            }
        if command == "release_sweep_fence":
            assert self.sweep_entered.is_set()
            self.sweep_release.set()
            return {"released": True}
        if command == "sweep":
            return {"examined": await app.state.circuit_orphan_sweeper.reconcile_once()}
        raise ValueError("Unknown acceptance command")


async def main():
    get_settings.cache_clear()
    app = create_app()
    async with app.router.lifespan_context(app):
        worker = Worker(app)

        async def handle(reader, writer):
            try:
                data = json.loads(await asyncio.wait_for(reader.readline(), 20))
                result = await worker.execute(data)
                response = {"ok": True, **result}
            except Exception as exc:  # noqa: BLE001 - bounded test protocol
                response = {"ok": False, "error_type": type(exc).__name__}
            writer.write((json.dumps(response) + "\n").encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        print(server.sockets[0].getsockname()[1], flush=True)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
