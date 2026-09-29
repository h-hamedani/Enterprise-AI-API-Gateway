"""Bounded, PostgreSQL-authorized reclamation of orphan circuit keys."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from enum import StrEnum
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.persistence.models import Base
from app.redis.circuit import CircuitIdentity, circuit_key
from app.redis.redis_failure import is_redis_availability_failure
from app.redis.telemetry import BoundedEvent, BoundedTelemetry, LoggingBoundedTelemetry

logger = logging.getLogger(__name__)
_PREFIX = "gw:v1:cb:"


class CircuitAuthority(StrEnum):
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"
    AUTHORITY_UNAVAILABLE = "AUTHORITY_UNAVAILABLE"


def circuit_failure_key(identity: CircuitIdentity) -> str:
    return f"{circuit_key(identity)}:failures"


def parse_circuit_key(raw: str | bytes) -> CircuitIdentity | None:
    """Accept only the canonical primary or its one failure-history suffix."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("ascii")
        except UnicodeDecodeError:
            return None
    if not isinstance(raw, str) or not raw.startswith(_PREFIX):
        return None
    candidate = raw.removesuffix(":failures")
    parts = candidate[len(_PREFIX) :].split(":")
    if len(parts) != 4 or not parts[0].startswith("{") or not parts[0].endswith("}"):
        return None
    try:
        identity = CircuitIdentity._create(
            UUID(parts[0][1:-1]), parts[1], UUID(parts[2]), UUID(parts[3])
        )
    except (ValueError, TypeError):
        return None
    return (
        identity
        if raw in (circuit_key(identity), circuit_failure_key(identity))
        else None
    )


class CircuitAuthorityRepository:
    """Status-independent existence of the exact tenant-owned relationship."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._routes = Base.metadata.tables["normal_api_routes"]
        self._services = Base.metadata.tables["normal_api_services"]
        self._targets = Base.metadata.tables["llm_provider_targets"]
        self._models = Base.metadata.tables["llm_models"]

    async def lookup(self, identity: CircuitIdentity) -> CircuitAuthority:
        if identity.target_kind == "route":
            route, service = self._routes, self._services
            statement = (
                select(route.c.id)
                .join(
                    service,
                    and_(
                        route.c.tenant_id == service.c.tenant_id,
                        route.c.service_id == service.c.id,
                    ),
                )
                .where(
                    route.c.tenant_id == identity.tenant_id,
                    route.c.id == identity.target_id,
                    service.c.id == identity.dimension_id,
                )
            )
        else:
            target, model = self._targets, self._models
            statement = (
                select(model.c.id)
                .join(
                    target,
                    and_(
                        model.c.tenant_id == target.c.tenant_id,
                        model.c.provider_target_id == target.c.id,
                    ),
                )
                .where(
                    model.c.tenant_id == identity.tenant_id,
                    model.c.id == identity.dimension_id,
                    target.c.id == identity.target_id,
                )
            )
        try:
            async with self._engine.connect() as connection:
                found = (await connection.execute(statement)).first()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - fail safe if authority cannot be proven
            return CircuitAuthority.AUTHORITY_UNAVAILABLE
        return CircuitAuthority.ACTIVE if found else CircuitAuthority.RETIRED


AuthorityLookup = Callable[[CircuitIdentity], Awaitable[CircuitAuthority]]


class CircuitOrphanSweeper:
    """One bounded, resumable SCAN pass per process; never traffic-gating."""

    def __init__(
        self,
        redis: Redis,
        lookup: AuthorityLookup,
        *,
        scan_count: int = 64,
        max_candidates: int = 128,
        max_scan_calls: int = 8,
        max_entries_examined: int = 256,
        interval_seconds: float = 30.0,
        telemetry: BoundedTelemetry | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (
            scan_count <= 0
            or max_candidates <= 0
            or max_scan_calls <= 0
            or max_entries_examined <= 0
            or interval_seconds <= 0
        ):
            raise ValueError("Circuit sweep bounds must be positive.")
        self._redis = redis
        self._lookup = lookup
        self._scan_count = scan_count
        self._max_candidates = max_candidates
        self._max_scan_calls = max_scan_calls
        self._max_entries_examined = max_entries_examined
        self._interval_seconds = interval_seconds
        self._telemetry = telemetry or LoggingBoundedTelemetry()
        self._sleep = sleep
        self._cursor = 0
        self._pending: deque[str | bytes] = deque()
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _record(self, outcome: str) -> None:
        self._telemetry.record(
            BoundedEvent("circuit", {"operation": "reconcile", "outcome": outcome})
        )

    async def _authority(self, identity: CircuitIdentity) -> CircuitAuthority:
        try:
            return await self._lookup(identity)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - uncertain authority never deletes state
            return CircuitAuthority.AUTHORITY_UNAVAILABLE

    async def reconcile_once(self) -> int:
        processed = 0
        seen: set[CircuitIdentity] = set()
        scanned = False
        scan_calls = 0
        examined = 0
        while (
            processed < self._max_candidates and examined < self._max_entries_examined
        ):
            if not self._pending:
                if (
                    scanned and self._cursor == 0
                ) or scan_calls >= self._max_scan_calls:
                    break
                try:
                    self._cursor, page = await self._redis.scan(
                        cursor=self._cursor,
                        match=f"{_PREFIX}*",
                        count=min(
                            self._scan_count,
                            self._max_candidates - processed,
                            self._max_entries_examined - examined,
                        ),
                    )
                    self._pending = deque(page)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - maintenance failure is contained
                    outcome = (
                        "redis_unavailable"
                        if is_redis_availability_failure(exc)
                        else "cleanup_error"
                    )
                    self._record(outcome)
                    logger.warning("Circuit orphan scan outcome=%s", outcome)
                    break
                scanned = True
                scan_calls += 1
                if not self._pending and self._cursor == 0:
                    break
                if not self._pending:
                    continue
            raw = self._pending.popleft()
            examined += 1
            identity = parse_circuit_key(raw)
            if identity is None or identity in seen:
                continue
            seen.add(identity)
            processed += 1
            self._record("candidate_seen")
            first = await self._authority(identity)
            if first is CircuitAuthority.ACTIVE:
                self._record("still_active")
                continue
            if first is CircuitAuthority.AUTHORITY_UNAVAILABLE:
                self._record("authority_unavailable")
                continue
            if first is not CircuitAuthority.RETIRED:
                self._record("authority_unavailable")
                continue
            final = await self._authority(identity)
            if final is CircuitAuthority.ACTIVE:
                self._record("still_active")
                continue
            if final is CircuitAuthority.AUTHORITY_UNAVAILABLE:
                self._record("authority_unavailable")
                continue
            if final is not CircuitAuthority.RETIRED:
                self._record("authority_unavailable")
                continue
            try:
                count = await self._redis.delete(
                    circuit_key(identity), circuit_failure_key(identity)
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - cleanup failure is repairable
                outcome = (
                    "redis_unavailable"
                    if is_redis_availability_failure(exc)
                    else "cleanup_error"
                )
                self._record(outcome)
                logger.warning("Circuit orphan deletion outcome=%s", outcome)
                continue
            self._record("orphan_deleted" if count else "already_missing")
        return processed

    async def start(self) -> None:
        if not self.running:
            self._task = asyncio.create_task(self._run(), name="circuit-orphan-sweeper")

    async def stop(self) -> None:
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep managed maintenance alive
                self._record("cleanup_error")
                logger.warning("Circuit orphan reconciliation failed")
            await self._sleep(self._interval_seconds)
