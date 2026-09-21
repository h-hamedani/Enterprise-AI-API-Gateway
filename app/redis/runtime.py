from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisAvailability(StrEnum):
    UNKNOWN = "UNKNOWN"
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class RedisTelemetryEvent:
    operation: str
    outcome: str
    latency_ms: float


@dataclass(frozen=True, slots=True)
class RedisHealthResult:
    available: bool
    state: RedisAvailability
    latency_ms: float


class RedisTelemetry(Protocol):
    def record_operation(self, event: RedisTelemetryEvent) -> None: ...

    def record_transition(
        self,
        previous: RedisAvailability,
        current: RedisAvailability,
    ) -> None: ...


class LoggingRedisTelemetry:
    """Emit bounded dependency telemetry without connection or command details."""

    def record_operation(self, event: RedisTelemetryEvent) -> None:
        logger.info(
            "Redis dependency operation",
            extra={
                "dependency": "redis",
                "operation_class": event.operation,
                "operation_outcome": event.outcome,
                "latency_ms": event.latency_ms,
            },
        )

    def record_transition(
        self,
        previous: RedisAvailability,
        current: RedisAvailability,
    ) -> None:
        logger.info(
            "Redis dependency state transition",
            extra={
                "dependency": "redis",
                "previous_state": previous.value,
                "current_state": current.value,
            },
        )


RedisFactory = Callable[..., Redis]


class RedisRuntime:
    """Own one reconnect-capable async Redis client for an application lifespan."""

    def __init__(
        self,
        redis_url: str,
        timeout_seconds: float,
        *,
        factory: RedisFactory = Redis.from_url,
        telemetry: RedisTelemetry | None = None,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._redis_url = redis_url
        self._timeout_seconds = timeout_seconds
        self._factory = factory
        self._telemetry = telemetry or LoggingRedisTelemetry()
        self._monotonic = monotonic
        self._client: Redis | None = None
        self._state = RedisAvailability.UNKNOWN
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

    @property
    def state(self) -> RedisAvailability:
        return self._state

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("Redis runtime has not been started.")
        return self._client

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._client is not None:
                return
            if self._closed:
                raise RuntimeError("Redis runtime has already been closed.")
            self._client = self._factory(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=self._timeout_seconds,
                socket_timeout=self._timeout_seconds,
            )

    async def check(self) -> RedisHealthResult:
        client = self.client
        started_at = self._monotonic()
        available = False
        try:
            async with asyncio.timeout(self._timeout_seconds):
                available = bool(await client.ping())
        except Exception:  # noqa: BLE001 - dependency failures become safe status
            available = False

        latency_ms = max(0.0, (self._monotonic() - started_at) * 1000)
        current = (
            RedisAvailability.AVAILABLE if available else RedisAvailability.UNAVAILABLE
        )
        self._telemetry.record_operation(
            RedisTelemetryEvent(
                operation="ping",
                outcome="success" if available else "failure",
                latency_ms=latency_ms,
            )
        )
        await self._set_state(current)
        return RedisHealthResult(available, current, latency_ms)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            client = self._client
            if client is None:
                return
            try:
                await client.aclose(close_connection_pool=True)
            except Exception:  # noqa: BLE001 - shutdown is safe and secret-free
                logger.warning(
                    "Redis dependency shutdown failed",
                    extra={"dependency": "redis", "operation_class": "shutdown"},
                )

    async def _set_state(self, current: RedisAvailability) -> None:
        async with self._state_lock:
            previous = self._state
            self._state = current
            if previous is not current:
                self._telemetry.record_transition(previous, current)
