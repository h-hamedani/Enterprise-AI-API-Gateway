from __future__ import annotations

import asyncio
import os
import time

import pytest

from app.core.config import get_settings
from app.redis.runtime import RedisAvailability, RedisRuntime


async def _docker_compose(action: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        action,
        "redis",
    )
    return_code = await asyncio.wait_for(process.wait(), timeout=30)
    assert return_code == 0


@pytest.mark.asyncio
async def test_real_redis_ping_and_pool_reconnect() -> None:
    runtime = RedisRuntime(
        get_settings().redis_url,
        get_settings().dependency_timeout_seconds,
    )
    await runtime.start()
    try:
        first = await runtime.check()
        assert first.available is True
        assert runtime.state is RedisAvailability.AVAILABLE

        await runtime.client.connection_pool.disconnect()

        recovered = await runtime.check()
        assert recovered.available is True
        assert runtime.state is RedisAvailability.AVAILABLE
    finally:
        await runtime.close()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_OUTAGE_TEST") != "1",
    reason="controlled Compose Redis outage test is opt-in",
)
@pytest.mark.asyncio
async def test_real_redis_outage_and_recovery_without_runtime_restart() -> None:
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url,
        settings.dependency_timeout_seconds,
    )
    await runtime.start()
    try:
        assert (await runtime.check()).available is True

        await _docker_compose("stop")
        started_at = time.perf_counter()
        failed = await runtime.check()
        failure_seconds = time.perf_counter() - started_at
        assert failed.available is False
        assert failure_seconds < settings.dependency_timeout_seconds + 1
        assert runtime.state is RedisAvailability.UNAVAILABLE

        await _docker_compose("start")
        for _ in range(20):
            recovered = await runtime.check()
            if recovered.available:
                break
            await asyncio.sleep(0.25)

        assert recovered.available is True
        assert runtime.state is RedisAvailability.AVAILABLE
    finally:
        await _docker_compose("start")
        await runtime.close()
