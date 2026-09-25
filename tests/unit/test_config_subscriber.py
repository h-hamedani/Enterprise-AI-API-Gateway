import asyncio
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
    await registry.register(
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


@pytest.mark.asyncio
async def test_subscriber_survives_callback_failure_and_retries_same_version(caplog):
    class QueuePubSub:
        def __init__(self):
            self.messages = asyncio.Queue()
            self.channels = []

        async def subscribe(self, *channels):
            self.channels.extend(channels)

        async def get_message(self, **kwargs):
            try:
                return await asyncio.wait_for(self.messages.get(), 0.05)
            except TimeoutError:
                return None

        async def aclose(self):
            pass

    class QueueRedis:
        def __init__(self):
            self.stream = QueuePubSub()

        def pubsub(self):
            return self.stream

    redis = QueueRedis()
    registry = InvalidationRegistry()
    subscriber = RedisInvalidationSubscriber(redis, registry)
    tenant, resource = uuid4(), uuid4()
    failed = asyncio.Event()
    calls = []

    def callback(event):
        calls.append(event.version)
        if event.version in (12, 14) and calls.count(event.version) == 1:
            failed.set()
            raise ValueError("secret-bearing callback detail")

    async def publish(version):
        await redis.stream.messages.put(
            {
                "data": {
                    "tenant_id": str(tenant),
                    "resource_type": "ROUTE",
                    "resource_id": str(resource),
                    "version": version,
                }
            }
        )

    async def wait_for_version(version):
        async with asyncio.timeout(1):
            while registry.version(tenant) != version:
                await asyncio.sleep(0)

    await registry.register(tenant, "ROUTE", resource, callback)
    await subscriber.start()
    try:
        await publish(12)
        await asyncio.wait_for(failed.wait(), 1)
        assert registry.version(tenant) == 0
        assert subscriber._task is not None and not subscriber._task.done()
        failed.clear()
        await publish(12)
        await wait_for_version(12)
        await publish(14)
        await asyncio.wait_for(failed.wait(), 1)
        assert registry.version(tenant) == 12
        assert subscriber._task is not None and not subscriber._task.done()
        await publish(15)
        await wait_for_version(15)
        assert calls == [12, 12, 14, 15]
        assert redis.stream.channels == [M3_INVALIDATION_CHANNEL]
        assert "secret-bearing callback detail" not in caplog.text
    finally:
        await subscriber.stop()
