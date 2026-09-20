from __future__ import annotations

import json
import logging
from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis

from app.control_plane.mutation_coordinator import CommittedMutation

logger = logging.getLogger(__name__)

CONFIG_INVALIDATION_CHANNEL = "gateway:config"


class InvalidationPublisher(Protocol):
    async def publish(
        self, mutation: CommittedMutation, *, request_id: UUID
    ) -> bool: ...


class RedisConfigInvalidationPublisher:
    """Publish safe, best-effort config invalidations after database commit."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, mutation: CommittedMutation, *, request_id: UUID) -> bool:
        payload = json.dumps(
            {
                "tenant_id": str(mutation.tenant_id),
                "resource_type": mutation.resource_type.value,
                "resource_id": str(mutation.resource_id),
                "version": mutation.version,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            await self._redis.publish(CONFIG_INVALIDATION_CHANNEL, payload)
        except Exception:  # noqa: BLE001 - publication must never fail committed writes
            logger.error(
                "Config invalidation publication failed",
                extra={
                    "request_id": str(request_id),
                    "tenant_id": str(mutation.tenant_id),
                    "config_version": mutation.version,
                    "resource_type": mutation.resource_type.value,
                    "resource_id": str(mutation.resource_id),
                    "publication_outcome": "FAILED",
                },
            )
            return False
        return True
