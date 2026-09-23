from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from app.control_plane.config_publish import (
    CANONICAL_INVALIDATION_CHANNEL,
    CONFIG_INVALIDATION_CHANNEL,
    RedisConfigInvalidationPublisher,
)
from app.control_plane.config_subscriber import (
    InvalidationRegistry,
    RedisInvalidationSubscriber,
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
        await subscriber.subscribe(
            CANONICAL_INVALIDATION_CHANNEL, CONFIG_INVALIDATION_CHANNEL
        )
        for _ in range(2):
            await subscriber.get_message(ignore_subscribe_messages=False, timeout=2)

        assert await RedisConfigInvalidationPublisher(redis).publish(
            mutation, request_id=uuid4()
        )
        message = await subscriber.get_message(
            ignore_subscribe_messages=True, timeout=2
        )

        assert message is not None
        assert message["channel"] in {
            CANONICAL_INVALIDATION_CHANNEL,
            CONFIG_INVALIDATION_CHANNEL,
        }
        assert json.loads(message["data"]) == {
            "tenant_id": str(mutation.tenant_id),
            "resource_type": "ROUTE",
            "resource_id": str(mutation.resource_id),
            "version": 81,
        }
    finally:
        await subscriber.aclose()
        await redis.aclose()


@pytest.mark.asyncio
async def test_real_redis_subscriber_resubscribes_after_targeted_client_kill():
    settings = get_settings()
    client_name = f"m36-reconnect-{uuid4()}"
    redis = Redis.from_url(
        settings.redis_url, decode_responses=True, client_name=client_name
    )
    control = Redis.from_url(settings.redis_url, decode_responses=True)
    registry = InvalidationRegistry()
    subscriber = RedisInvalidationSubscriber(redis, registry)
    tenant_id, resource_id = uuid4(), uuid4()

    async def wait_for(predicate):
        for _ in range(100):
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.05)
        pytest.fail("Subscriber did not reach the expected Redis state")

    async def subscribed_client(exclude_id=None):
        clients = await control.client_list()
        return next(
            (
                client
                for client in clients
                if client.get("name") == client_name
                and int(client.get("sub", 0)) == 1
                and client.get("id") != exclude_id
            ),
            None,
        )

    async def observed(version):
        return registry.version(tenant_id) == version

    def payload(version):
        return json.dumps(
            {
                "tenant_id": str(tenant_id),
                "resource_type": "ROUTE",
                "resource_id": str(resource_id),
                "version": version,
            }
        )

    try:
        await subscriber.start()
        first_client = await wait_for(subscribed_client)
        await control.publish(CANONICAL_INVALIDATION_CHANNEL, payload(1))
        await wait_for(lambda: observed(1))
        await control.execute_command("CLIENT", "KILL", "ID", first_client["id"])
        assert subscriber._task is not None and not subscriber._task.done()
        await wait_for(lambda: subscribed_client(first_client["id"]))
        await asyncio.sleep(0.7)
        await wait_for(lambda: subscribed_client(first_client["id"]))
        await control.publish(CONFIG_INVALIDATION_CHANNEL, payload(2))
        await asyncio.sleep(0.2)
        assert registry.version(tenant_id) == 1
        await control.publish(CANONICAL_INVALIDATION_CHANNEL, payload(2))
        await wait_for(lambda: observed(2))
        assert subscriber._task is not None and not subscriber._task.done()
    finally:
        await subscriber.stop()
        await redis.aclose()
        await control.aclose()
