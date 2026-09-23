from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.redis.namespace import M3_INVALIDATION_CHANNEL

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InvalidationEvent:
    tenant_id: UUID
    resource_type: str
    resource_id: UUID
    version: int


def parse_invalidation_event(data: object) -> InvalidationEvent | None:
    try:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        value = json.loads(data) if isinstance(data, str) else data
        if not isinstance(value, dict) or set(value) != {
            "tenant_id",
            "resource_type",
            "resource_id",
            "version",
        }:
            return None
        version = value["version"]
        if type(version) is not int or version <= 0:
            return None
        resource_type = value["resource_type"]
        if not isinstance(resource_type, str) or not resource_type:
            return None
        return InvalidationEvent(
            UUID(str(value["tenant_id"])),
            resource_type,
            UUID(str(value["resource_id"])),
            version,
        )
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None


class InvalidationRegistry:
    def __init__(self) -> None:
        self._versions: dict[UUID, int] = {}
        self._callbacks: dict[
            tuple[UUID, str, UUID],
            Callable[[InvalidationEvent], Awaitable[None] | None],
        ] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        tenant_id: UUID,
        resource_type: str,
        resource_id: UUID,
        callback: Callable[[InvalidationEvent], Awaitable[None] | None],
    ) -> None:
        self._callbacks[(tenant_id, resource_type, resource_id)] = callback

    async def observe(self, event: InvalidationEvent) -> str:
        async with self._lock:
            previous = self._versions.get(event.tenant_id, 0)
            if event.version <= previous:
                return "DUPLICATE" if event.version == previous else "STALE"
            self._versions[event.tenant_id] = event.version
            callback = self._callbacks.get(
                (event.tenant_id, event.resource_type, event.resource_id)
            )
        if callback is not None:
            result = callback(event)
            if asyncio.iscoroutine(result):
                await result
        return "APPLIED"

    def version(self, tenant_id: UUID) -> int:
        return self._versions.get(tenant_id, 0)


class RedisInvalidationSubscriber:
    def __init__(self, redis: Redis, registry: InvalidationRegistry) -> None:
        self._redis = redis
        self._registry = registry
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._pubsub = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(
                self._run(), name="config-invalidation-subscriber"
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._pubsub is not None:
            await self._pubsub.aclose()
            self._pubsub = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            pubsub = None
            try:
                pubsub = self._redis.pubsub()
                self._pubsub = pubsub
                await pubsub.subscribe(M3_INVALIDATION_CHANNEL)
                while not self._stop.is_set():
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message is None:
                        continue
                    event = parse_invalidation_event(message.get("data"))
                    if event is not None:
                        await self._registry.observe(event)
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError, RuntimeError):
                logger.warning("Config invalidation subscriber reconnecting")
                await asyncio.sleep(0.5)
            finally:
                if pubsub is not None:
                    await pubsub.aclose()
                self._pubsub = None
