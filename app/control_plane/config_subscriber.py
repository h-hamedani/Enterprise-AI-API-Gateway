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
        self._required_current_state: dict[
            tuple[UUID, str, UUID], Callable[[UUID], Awaitable[None] | None]
        ] = {}
        self._pending_required: set[tuple[UUID, str, UUID]] = set()
        self._reconciliation_required: set[UUID] = set()
        self._certified_tenants: set[UUID] = set()
        self._invalidation_epoch = 0
        self._locks: dict[UUID, asyncio.Lock] = defaultdict(asyncio.Lock)

    @property
    def invalidation_epoch(self) -> int:
        """Process-local fence against cutover after a certificate invalidation."""
        return self._invalidation_epoch

    def _invalidate_certificate(self, tenant_id: UUID) -> None:
        self._certified_tenants.discard(tenant_id)
        self._invalidation_epoch += 1

    async def register(
        self,
        tenant_id: UUID,
        resource_type: str,
        resource_id: UUID,
        callback: Callable[[InvalidationEvent], Awaitable[None] | None],
    ) -> None:
        async with self._locks[tenant_id]:
            key = (tenant_id, resource_type, resource_id)
            if key in self._required_current_state:
                raise ValueError(
                    "Required consumer replacement needs current-state capability."
                )
            self._tenants.add(tenant_id)
            self._callbacks[key] = callback
            self._invalidate_certificate(tenant_id)

    async def register_tenant_callback(
        self, tenant_id: UUID, callback: Callable[[int], Awaitable[None] | None]
    ) -> None:
        async with self._locks[tenant_id]:
            self._tenants.add(tenant_id)
            self._tenant_callbacks[tenant_id] = callback
            self._pending_initialization.add(tenant_id)
            self._invalidate_certificate(tenant_id)

    async def register_required_consumer(
        self,
        tenant_id: UUID,
        resource_type: str,
        resource_id: UUID,
        event_callback: Callable[[InvalidationEvent], Awaitable[None] | None],
        reconcile_current_state: Callable[[UUID], Awaitable[None] | None],
    ) -> None:
        """Register distinct event and authoritative current-state capabilities."""
        async with self._locks[tenant_id]:
            key = (tenant_id, resource_type, resource_id)
            self._tenants.add(tenant_id)
            self._callbacks[key] = event_callback
            self._required_current_state[key] = reconcile_current_state
            self._pending_required.add(key)
            self._invalidate_certificate(tenant_id)

    def reconciliation_required(self, tenant_id: UUID) -> bool:
        return tenant_id in self._reconciliation_required

    def required_initialization_pending(self, tenant_id: UUID) -> bool:
        return any(key[0] == tenant_id for key in self._pending_required)

    def tenant_ids(self) -> frozenset[UUID]:
        return frozenset(self._tenants)

    def applied_version(self, tenant_id: UUID) -> int | None:
        """Historical name for observed progress, not completeness."""
        return self._versions.get(tenant_id)

    def observed_version(self, tenant_id: UUID) -> int | None:
        return self._versions.get(tenant_id)

    def is_tenant_reconciled(self, tenant_id: UUID) -> bool:
        return (
            tenant_id in self._tenants
            and tenant_id in self._certified_tenants
            and tenant_id not in self._reconciliation_required
            and tenant_id not in self._pending_initialization
            and not self.required_initialization_pending(tenant_id)
        )

    async def observe(self, event: InvalidationEvent) -> str:
        async with self._locks[event.tenant_id]:
            previous = self._versions.get(event.tenant_id, 0)
            if event.version <= previous:
                return "DUPLICATE" if event.version == previous else "STALE"
            self._invalidate_certificate(event.tenant_id)
            if event.version > previous + 1:
                self._reconciliation_required.add(event.tenant_id)
            callback = self._callbacks.get(
                (event.tenant_id, event.resource_type, event.resource_id)
            )
            if callback is not None:
                try:
                    result = callback(event)
                    if isawaitable(result):
                        await result
                except (Exception, asyncio.CancelledError):
                    if (
                        event.tenant_id,
                        event.resource_type,
                        event.resource_id,
                    ) in self._required_current_state:
                        self._reconciliation_required.add(event.tenant_id)
                    raise
            self._versions[event.tenant_id] = event.version
        return "APPLIED"

    async def reconcile(self, tenant_id: UUID, version: int) -> str:
        """Legacy unfenced application; never certifies tenant completeness."""
        async with self._locks[tenant_id]:
            if any(key[0] == tenant_id for key in self._required_current_state):
                raise RuntimeError("Required consumers need fenced reconciliation.")
            previous = self._versions.get(tenant_id)
            pending_tenant = tenant_id in self._pending_initialization
            pending_required = self.required_initialization_pending(tenant_id)
            dirty = tenant_id in self._reconciliation_required
            pending = pending_tenant or pending_required
            if not pending and previous is not None:
                if version == previous and not dirty:
                    return "up_to_date"
                if version < previous:
                    return "stale_db_ignored"
            required_snapshot = tuple(
                (key, callback)
                for key, callback in self._required_current_state.items()
                if key[0] == tenant_id
            )
            failure: Exception | None = None
            callback = self._tenant_callbacks.get(tenant_id)
            if callback is not None:
                try:
                    result = callback(version)
                    if isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    self._reconciliation_required.add(tenant_id)
                    raise
                except Exception as exc:  # noqa: BLE001 - isolate callback failures
                    failure = exc
                else:
                    self._pending_initialization.discard(tenant_id)
            for key, current_state_callback in required_snapshot:
                try:
                    result = current_state_callback(tenant_id)
                    if isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    self._reconciliation_required.add(tenant_id)
                    raise
                except Exception as exc:  # noqa: BLE001 - attempt full snapshot
                    if failure is None:
                        failure = exc
                else:
                    self._pending_required.discard(key)
            if failure is not None:
                self._reconciliation_required.add(tenant_id)
                raise failure
            self._versions[tenant_id] = (
                version if previous is None else max(previous, version)
            )
            return "initialized" if pending or previous is None else "reconciled"

    async def reconcile_fenced(
        self,
        tenant_id: UUID,
        lookup: Callable[[frozenset[UUID]], Awaitable[dict[UUID, int]]],
    ) -> str:
        """Apply a stable consumer snapshot between two authoritative reads."""
        async with self._locks[tenant_id]:
            was_certified = tenant_id in self._certified_tenants
            self._invalidate_certificate(tenant_id)
            try:
                start_version = (await lookup(frozenset({tenant_id})))[tenant_id]
            except asyncio.CancelledError:
                raise
            except Exception:
                self._reconciliation_required.add(tenant_id)
                raise

            previous = self._versions.get(tenant_id)
            pending_tenant = tenant_id in self._pending_initialization
            pending_required = self.required_initialization_pending(tenant_id)
            dirty = tenant_id in self._reconciliation_required
            if (
                previous is not None
                and start_version < previous
                and not (pending_tenant or pending_required)
            ):
                return "stale_db_ignored"

            required_snapshot = tuple(
                (key, callback)
                for key, callback in self._required_current_state.items()
                if key[0] == tenant_id
            )
            work_needed = (
                previous is None
                or start_version != previous
                or dirty
                or pending_tenant
                or pending_required
                or not was_certified
            )
            if work_needed:
                failure: Exception | None = None
                tenant_callback = self._tenant_callbacks.get(tenant_id)
                if tenant_callback is not None:
                    try:
                        result = tenant_callback(start_version)
                        if isawaitable(result):
                            await result
                    except asyncio.CancelledError:
                        self._reconciliation_required.add(tenant_id)
                        raise
                    except Exception as exc:  # noqa: BLE001 - isolate callback failure
                        failure = exc
                for _, callback in required_snapshot:
                    try:
                        result = callback(tenant_id)
                        if isawaitable(result):
                            await result
                    except asyncio.CancelledError:
                        self._reconciliation_required.add(tenant_id)
                        raise
                    except Exception as exc:  # noqa: BLE001 - attempt full snapshot
                        if failure is None:
                            failure = exc
                if failure is not None:
                    self._reconciliation_required.add(tenant_id)
                    raise failure

            try:
                end_version = (await lookup(frozenset({tenant_id})))[tenant_id]
            except asyncio.CancelledError:
                self._reconciliation_required.add(tenant_id)
                raise
            except Exception:
                self._reconciliation_required.add(tenant_id)
                raise
            if end_version != start_version or (
                previous is not None and end_version < previous
            ):
                self._reconciliation_required.add(tenant_id)
                return "fence_mismatch"

            self._versions[tenant_id] = (
                end_version if previous is None else max(previous, end_version)
            )
            self._pending_initialization.discard(tenant_id)
            for key, _ in required_snapshot:
                self._pending_required.discard(key)
            self._reconciliation_required.discard(tenant_id)
            self._certified_tenants.add(tenant_id)
            if not work_needed:
                return "up_to_date"
            return (
                "initialized"
                if pending_tenant or pending_required or previous is None
                else "reconciled"
            )

    def version(self, tenant_id: UUID) -> int:
        return self._versions.get(tenant_id, 0)


class RedisInvalidationSubscriber:
    def __init__(self, redis: Redis, registry: InvalidationRegistry) -> None:
        self._redis = redis
        self._registry = registry
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._pubsub = None
        self._subscribed = False
        self._subscription_epoch = 0

    @property
    def subscription_epoch(self) -> int:
        """Change whenever a canonical subscription is established or lost."""
        return self._subscription_epoch

    @property
    def ready(self) -> bool:
        return (
            self._subscribed
            and self._task is not None
            and not self._task.done()
            and not self._stop.is_set()
        )

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(
                self._run(), name="config-invalidation-subscriber"
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._subscribed:
            self._subscribed = False
            self._subscription_epoch += 1
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
                self._subscribed = True
                self._subscription_epoch += 1
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
                if self._subscribed:
                    self._subscribed = False
                    self._subscription_epoch += 1
                if pubsub is not None:
                    await pubsub.aclose()
                self._pubsub = None
