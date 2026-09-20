from __future__ import annotations

import json
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from app.control_plane.config_publish import (
    CONFIG_INVALIDATION_CHANNEL,
    RedisConfigInvalidationPublisher,
)
from app.control_plane.mutation_coordinator import CommittedMutation, ResourceType
from app.core.config import get_settings


@pytest.mark.asyncio
async def test_real_redis_receives_frozen_invalidation_payload():
    redis = Redis.from_url(get_settings().redis_url, decode_responses=True)
    subscriber = redis.pubsub()
    mutation = CommittedMutation(
        tenant_id=uuid4(),
        version=81,
        resource_type=ResourceType.ROUTE,
        resource_id=uuid4(),
    )
    try:
        await subscriber.subscribe(CONFIG_INVALIDATION_CHANNEL)
        await subscriber.get_message(ignore_subscribe_messages=False, timeout=2)

        assert await RedisConfigInvalidationPublisher(redis).publish(
            mutation, request_id=uuid4()
        )
        message = await subscriber.get_message(
            ignore_subscribe_messages=True, timeout=2
        )

        assert message is not None
        assert message["channel"] == CONFIG_INVALIDATION_CHANNEL
        assert json.loads(message["data"]) == {
            "tenant_id": str(mutation.tenant_id),
            "resource_type": "ROUTE",
            "resource_id": str(mutation.resource_id),
            "version": 81,
        }
    finally:
        await subscriber.aclose()
        await redis.aclose()
