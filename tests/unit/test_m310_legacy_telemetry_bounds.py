import logging
from uuid import uuid4

import pytest

from app.redis.circuit import LoggingCircuitTelemetry
from app.redis.concurrency import LoggingConcurrencyTelemetry
from app.redis.local_degraded import LoggingLocalDegradedTelemetry
from app.redis.rate_limit import LoggingRateLimitTelemetry, RateLimitTelemetryEvent
from app.redis.runtime import LoggingRedisTelemetry, RedisRuntime, RedisTelemetryEvent


@pytest.mark.parametrize(
    ("logger_name", "emit"),
    [
        (
            "app.redis.runtime",
            lambda dangerous: LoggingRedisTelemetry().record_operation(
                RedisTelemetryEvent(dangerous, dangerous, 1.0)
            ),
        ),
        (
            "app.redis.rate_limit",
            lambda dangerous: LoggingRateLimitTelemetry().record(
                RateLimitTelemetryEvent(dangerous, (dangerous,))
            ),
        ),
        (
            "app.redis.concurrency",
            lambda dangerous: LoggingConcurrencyTelemetry().record(
                dangerous, dangerous, (dangerous,)
            ),
        ),
        (
            "app.redis.circuit",
            lambda dangerous: LoggingCircuitTelemetry().record(
                dangerous, dangerous, dangerous, dangerous
            ),
        ),
    ],
)
def test_legacy_telemetry_adapters_never_emit_arbitrary_labels(
    caplog, monkeypatch, logger_name, emit
) -> None:
    dangerous = f"adm_secret:{uuid4()}:redis://password"
    logger = logging.getLogger(logger_name)
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", True)
    with caplog.at_level(logging.INFO, logger=logger_name):
        emit(dangerous)
    assert caplog.records
    assert dangerous not in str([record.__dict__ for record in caplog.records])


def test_local_capacity_logging_rejects_unknown_store_or_outcome() -> None:
    adapter = LoggingLocalDegradedTelemetry()
    with pytest.raises(ValueError):
        adapter.record("tenant:secret", "local_capacity_exhausted")
    with pytest.raises(ValueError):
        adapter.record("rate", "tenant:secret")


def test_circuit_logging_preserves_known_bounded_state(caplog, monkeypatch) -> None:
    logger = logging.getLogger("app.redis.circuit")
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", True)
    with caplog.at_level(logging.INFO, logger="app.redis.circuit"):
        LoggingCircuitTelemetry().record("eligibility", "closed", "half_open", "route")
    assert caplog.records[-1].circuit_state == "half_open"
    assert caplog.records[-1].target_kind == "route"


@pytest.mark.asyncio
async def test_redis_dependency_failure_log_omits_url_and_exception(
    caplog, monkeypatch
) -> None:
    secret = "redis://user:password@host/0"

    class Client:
        async def ping(self):
            raise RuntimeError(secret)

    logger = logging.getLogger("app.redis.runtime")
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", True)
    runtime = RedisRuntime(secret, 0.01, factory=lambda *_args, **_kwargs: Client())
    await runtime.start()
    with caplog.at_level(logging.INFO, logger=logger.name):
        assert not (await runtime.check()).available
    assert caplog.records
    assert secret not in str([record.__dict__ for record in caplog.records])
    assert "password" not in str([record.__dict__ for record in caplog.records])
