"""Live Redis subscription, recovery probe, and post-cutover rate admission."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from app.control_plane.config_reconciler import ConfigReconciler
from app.control_plane.config_subscriber import (
    InvalidationRegistry,
    RedisInvalidationSubscriber,
)
from app.core.config import get_settings
from app.persistence.models.enums import RateScopeType
from app.redis.protection_facade import RecoveryRateLimiter
from app.redis.rate_limit import RedisTokenBucket, ResolvedRatePolicy
from app.redis.recovery import RecoveryCoordinator, TrafficMode
from app.redis.runtime import RedisRuntime


@pytest.mark.asyncio
async def test_live_redis_subscriber_and_cutover_with_empty_membership():
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url, settings.dependency_timeout_seconds)
    await runtime.start()
    registry = InvalidationRegistry()
    subscriber = RedisInvalidationSubscriber(runtime.client, registry)

    async def no_full_table_scan(_tenant_ids):
        pytest.fail("Empty membership must not query PostgreSQL")

    reconciler = ConfigReconciler(registry, no_full_table_scan)
    coordinator = RecoveryCoordinator(runtime, subscriber, reconciler, registry)
    facade = RecoveryRateLimiter(RedisTokenBucket(runtime), coordinator)
    try:
        assert (await runtime.check()).available
        await subscriber.start()

        async def wait_for_subscription():
            while not subscriber.ready:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_subscription(), timeout=5)
        coordinator.mark_unreachable()
        assert coordinator.mode is TrafficMode.DEGRADED_REDIS
        assert await coordinator.attempt_recovery() == "recovery_succeeded"
        assert coordinator.mode is TrafficMode.NORMAL
        policy = ResolvedRatePolicy(
            uuid4(), uuid4(), RateScopeType.ADMIN_TOKEN, uuid4(), 5, 60
        )
        assert (await facade.evaluate([policy])).allowed
    finally:
        await subscriber.stop()
        await runtime.close()
