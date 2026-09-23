from uuid import uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.control_plane.config_subscriber import (
    InvalidationEvent,
    InvalidationRegistry,
    RedisInvalidationSubscriber,
    parse_invalidation_event,
)
from app.redis.namespace import M3_INVALIDATION_CHANNEL


def test_parse_rejects_extra_fields_and_invalid_versions():
    tenant_id, resource_id = uuid4(), uuid4()
    base = {
        "tenant_id": str(tenant_id),
        "resource_type": "ROUTE",
        "resource_id": str(resource_id),
        "version": 1,
    }
    assert parse_invalidation_event(base) is not None
    assert parse_invalidation_event({**base, "extra": 1}) is None
    assert parse_invalidation_event({**base, "version": 0}) is None
    for malformed in (
        "not-json",
        [],
        {**base, "tenant_id": "bad"},
        {**base, "resource_type": ""},
        {**base, "resource_type": 1},
        {**base, "resource_id": "bad"},
        {key: value for key, value in base.items() if key != "version"},
        {**base, "version": True},
        {**base, "version": 1.5},
        {**base, "version": "1"},
        {**base, "version": -1},
    ):
        assert parse_invalidation_event(malformed) is None


@pytest.mark.asyncio
async def test_registry_is_tenant_scoped_and_monotonic():
    tenant_a, tenant_b, resource = uuid4(), uuid4(), uuid4()
    registry = InvalidationRegistry()
    applied = []
    registry.register(
        tenant_a, "ROUTE", resource, lambda event: applied.append(event.version)
    )
    assert (
        await registry.observe(InvalidationEvent(tenant_a, "ROUTE", resource, 2))
        == "APPLIED"
    )
    assert (
        await registry.observe(InvalidationEvent(tenant_a, "ROUTE", resource, 2))
        == "DUPLICATE"
    )
    assert (
        await registry.observe(InvalidationEvent(tenant_a, "ROUTE", resource, 1))
        == "STALE"
    )
    assert (
        await registry.observe(InvalidationEvent(tenant_b, "ROUTE", resource, 1))
        == "APPLIED"
    )
    assert applied == [2]


@pytest.mark.asyncio
async def test_subscriber_reconnects_after_redis_errors_and_cancels_cleanly():
    class FakePubSub:
        def __init__(self, error=None):
            self.error = error
            self.closed = False
            self.channels = []

        async def subscribe(self, *channels):
            self.channels.extend(channels)
            if self.error:
                error, self.error = self.error, None
                raise error

        async def get_message(self, **kwargs):
            await asyncio.sleep(0.01)

        async def aclose(self):
            self.closed = True

    class FakeRedis:
        def __init__(self):
            self.created = []
            self.errors = [RedisConnectionError(), RedisTimeoutError(), None]

        def pubsub(self):
            pubsub = FakePubSub(self.errors.pop(0) if self.errors else None)
            self.created.append(pubsub)
            return pubsub

    import asyncio

    redis = FakeRedis()
    subscriber = RedisInvalidationSubscriber(redis, InvalidationRegistry())
    await subscriber.start()
    await asyncio.sleep(1.2)
    assert subscriber._task is not None and not subscriber._task.done()
    assert all(item.channels == [M3_INVALIDATION_CHANNEL] for item in redis.created)
    await subscriber.stop()
    assert subscriber._task is None
    assert all(item.closed for item in redis.created)
