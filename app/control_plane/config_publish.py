from __future__ import annotations

import json
import logging
from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis

from app.control_plane.mutation_coordinator import CommittedMutation
from app.redis.namespace import (
    HISTORICAL_M2_INVALIDATION_CHANNEL,
    M3_INVALIDATION_CHANNEL,
)

logger = logging.getLogger(__name__)

CONFIG_INVALIDATION_CHANNEL = HISTORICAL_M2_INVALIDATION_CHANNEL
CANONICAL_INVALIDATION_CHANNEL = M3_INVALIDATION_CHANNEL


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
        outcomes: list[bool] = []
        for channel in (CANONICAL_INVALIDATION_CHANNEL, CONFIG_INVALIDATION_CHANNEL):
            try:
                await self._redis.publish(channel, payload)
            except Exception:  # noqa: BLE001 - publication must never fail committed writes
                outcomes.append(False)
                continue
            outcomes.append(True)
        if not any(outcomes):
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
        if not all(outcomes):
            logger.warning("Config invalidation publication partially succeeded")
        return True
