from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from inspect import isawaitable
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
        self._tenants: set[UUID] = set()
        self._callbacks: dict[
            tuple[UUID, str, UUID],
            Callable[[InvalidationEvent], Awaitable[None] | None],
        ] = {}
        self._tenant_callbacks: dict[UUID, Callable[[int], Awaitable[None] | None]] = {}
        self._pending_initialization: set[UUID] = set()
        self._locks: dict[UUID, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def register(
        self,
        tenant_id: UUID,
        resource_type: str,
        resource_id: UUID,
        callback: Callable[[InvalidationEvent], Awaitable[None] | None],
    ) -> None:
        async with self._locks[tenant_id]:
            self._tenants.add(tenant_id)
            self._callbacks[(tenant_id, resource_type, resource_id)] = callback

    async def register_tenant_callback(
        self, tenant_id: UUID, callback: Callable[[int], Awaitable[None] | None]
    ) -> None:
        async with self._locks[tenant_id]:
            self._tenants.add(tenant_id)
            self._tenant_callbacks[tenant_id] = callback
            self._pending_initialization.add(tenant_id)

    def tenant_ids(self) -> frozenset[UUID]:
        return frozenset(self._tenants)

    def applied_version(self, tenant_id: UUID) -> int | None:
        return self._versions.get(tenant_id)

    async def observe(self, event: InvalidationEvent) -> str:
        async with self._locks[event.tenant_id]:
            previous = self._versions.get(event.tenant_id, 0)
            if event.version <= previous:
                return "DUPLICATE" if event.version == previous else "STALE"
            callback = self._callbacks.get(
                (event.tenant_id, event.resource_type, event.resource_id)
            )
            if callback is not None:
                result = callback(event)
                if isawaitable(result):
                    await result
            self._versions[event.tenant_id] = event.version
        return "APPLIED"

    async def reconcile(self, tenant_id: UUID, version: int) -> str:
        async with self._locks[tenant_id]:
            previous = self._versions.get(tenant_id)
            pending = tenant_id in self._pending_initialization
            if not pending and previous is not None:
                if version == previous:
                    return "up_to_date"
                if version < previous:
                    return "stale_db_ignored"
            callback = self._tenant_callbacks.get(tenant_id)
            if callback is not None:
                result = callback(version)
                if isawaitable(result):
                    await result
            self._versions[tenant_id] = (
                version if previous is None else max(previous, version)
            )
            if pending:
                self._pending_initialization.discard(tenant_id)
            return "initialized" if pending or previous is None else "reconciled"

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
                        try:
                            await self._registry.observe(event)
                        except asyncio.CancelledError:
                            raise
                        except Exception:  # noqa: BLE001 - isolate callbacks
                            logger.warning("Config invalidation callback failed")
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError, RuntimeError):
                logger.warning("Config invalidation subscriber reconnecting")
                await asyncio.sleep(0.5)
            finally:
                if pubsub is not None:
                    await pubsub.aclose()
                self._pubsub = None
