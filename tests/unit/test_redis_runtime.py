from __future__ import annotations

import asyncio
import logging

import pytest

from app.redis.runtime import (
    RedisAvailability,
    RedisRuntime,
    RedisTelemetryEvent,
)


class RecordingTelemetry:
    def __init__(self) -> None:
        self.operations: list[RedisTelemetryEvent] = []
        self.transitions: list[tuple[RedisAvailability, RedisAvailability]] = []

    def record_operation(self, event: RedisTelemetryEvent) -> None:
        self.operations.append(event)

    def record_transition(
        self, previous: RedisAvailability, current: RedisAvailability
    ) -> None:
        self.transitions.append((previous, current))


class FakeRedis:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.ping_calls = 0
        self.close_calls = 0

    async def ping(self) -> bool:
        self.ping_calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, asyncio.Event):
            await outcome.wait()
        return bool(outcome)

    async def aclose(self, *, close_connection_pool: bool = True) -> None:
        assert close_connection_pool is True
        self.close_calls += 1


@pytest.mark.asyncio
async def test_construction_does_not_imply_connectivity_and_client_is_created_once():
    client = FakeRedis([True])
    calls = 0

    def factory(*args, **kwargs):
        nonlocal calls
        calls += 1
        assert kwargs["socket_connect_timeout"] == 1.0
        assert kwargs["socket_timeout"] == 1.0
        return client

    runtime = RedisRuntime("redis://user:secret@redis.internal/0", 1.0, factory=factory)

    assert runtime.state is RedisAvailability.UNKNOWN
    await runtime.start()
    await runtime.start()
    assert calls == 1
    assert runtime.state is RedisAvailability.UNKNOWN
    assert client.ping_calls == 0


@pytest.mark.asyncio
async def test_ping_failure_and_recovery_use_same_runtime_without_restart():
    telemetry = RecordingTelemetry()
    client = FakeRedis([True, RuntimeError("redis://user:secret@host"), True])
    runtime = RedisRuntime(
        "redis://user:secret@host/0",
        1.0,
        factory=lambda *args, **kwargs: client,
        telemetry=telemetry,
    )
    await runtime.start()

    assert (await runtime.check()).available is True
    assert (await runtime.check()).available is False
    assert (await runtime.check()).available is True
    assert runtime.state is RedisAvailability.AVAILABLE
    assert telemetry.transitions == [
        (RedisAvailability.UNKNOWN, RedisAvailability.AVAILABLE),
        (RedisAvailability.AVAILABLE, RedisAvailability.UNAVAILABLE),
        (RedisAvailability.UNAVAILABLE, RedisAvailability.AVAILABLE),
    ]
    assert [event.outcome for event in telemetry.operations] == [
        "success",
        "failure",
        "success",
    ]
    assert all(event.operation == "ping" for event in telemetry.operations)


@pytest.mark.asyncio
async def test_ping_is_bounded_and_failure_telemetry_has_bounded_fields():
    blocked = asyncio.Event()
    telemetry = RecordingTelemetry()
    runtime = RedisRuntime(
        "redis://user:secret@host/0",
        0.01,
        factory=lambda *args, **kwargs: FakeRedis([blocked]),
        telemetry=telemetry,
    )
    await runtime.start()

    result = await runtime.check()

    assert result.available is False
    assert result.state is RedisAvailability.UNAVAILABLE
    assert result.latency_ms >= 0
    assert telemetry.operations[0].outcome == "failure"
    assert vars(telemetry.operations[0]).keys() == {
        "operation",
        "outcome",
        "latency_ms",
    }


@pytest.mark.asyncio
async def test_shutdown_closes_pool_and_is_safe_after_failure(caplog):
    client = FakeRedis([RuntimeError("redis://user:secret@host")])
    runtime = RedisRuntime(
        "redis://user:secret@host/0",
        1.0,
        factory=lambda *args, **kwargs: client,
    )
    await runtime.start()
    assert (await runtime.check()).available is False

    with caplog.at_level(logging.INFO, logger="app.redis.runtime"):
        await runtime.close()
        await runtime.close()

    assert client.close_calls == 1
    assert "secret" not in caplog.text
    assert "redis://" not in caplog.text


def test_versioned_namespace_foundation_contains_no_runtime_algorithm_keys():
    from app.redis.namespace import (
        HISTORICAL_M2_INVALIDATION_CHANNEL,
        M3_INVALIDATION_CHANNEL,
        REDIS_NAMESPACE_PREFIX,
    )

    assert REDIS_NAMESPACE_PREFIX == "gw:v1:"
    assert M3_INVALIDATION_CHANNEL == "gw:v1:invalidate"
    assert HISTORICAL_M2_INVALIDATION_CHANNEL == "gateway:config"
