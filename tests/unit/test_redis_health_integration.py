from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.main import create_app
from app.redis.runtime import RedisAvailability, RedisHealthResult


class HealthSequenceRuntime:
    def __init__(self, available: list[bool]) -> None:
        self.available = available

    async def check(self) -> RedisHealthResult:
        available = self.available.pop(0)
        state = (
            RedisAvailability.AVAILABLE if available else RedisAvailability.UNAVAILABLE
        )
        return RedisHealthResult(available, state, 1.0)


def test_liveness_does_not_check_redis(monkeypatch) -> None:
    app = create_app()
    runtime = HealthSequenceRuntime([])

    with TestClient(app) as client:
        app.state.redis_runtime = runtime
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_reports_outage_safely_and_recovers(monkeypatch) -> None:
    app = create_app()
    runtime = HealthSequenceRuntime([False, True])
    monkeypatch.setattr("app.api.health._check_postgres", AsyncMock(return_value=True))

    with TestClient(app) as client:
        app.state.redis_runtime = runtime
        unavailable = client.get("/health/ready")
        recovered = client.get("/health/ready")

    assert unavailable.status_code == 503
    assert unavailable.json() == {"status": "not_ready"}
    assert recovered.status_code == 200
    assert recovered.json() == {
        "status": "ready",
        "postgres": "ok",
        "redis": "ok",
    }
    serialized = unavailable.text + recovered.text
    assert "redis://" not in serialized
    assert "password" not in serialized


def test_traffic_health_does_not_prematurely_depend_on_redis(monkeypatch) -> None:
    app = create_app()
    monkeypatch.setattr("app.api.health._check_postgres", AsyncMock(return_value=True))

    with TestClient(app) as client:
        app.state.redis_runtime = HealthSequenceRuntime([False])
        response = client.get("/health/traffic")

    assert response.status_code == 200
    assert response.json() == {
        "status": "accepting",
        "runtime_mode": "NORMAL",
    }
