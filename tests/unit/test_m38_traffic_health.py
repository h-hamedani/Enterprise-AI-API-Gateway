import json
from types import SimpleNamespace

import pytest

from app.api import health


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configured,expected_status,expected_mode",
    [
        ("NORMAL", 200, "DEGRADED"),
        ("DRAINING", 503, "DRAINING"),
    ],
)
async def test_traffic_health_exposes_degraded_without_overriding_drain(
    monkeypatch, configured, expected_status, expected_mode
):
    async def healthy(engine):
        return True

    monkeypatch.setattr(health, "_check_postgres", healthy)
    protection = SimpleNamespace(local=SimpleNamespace(mode_degraded=True))
    state = SimpleNamespace(
        db_engine=object(),
        runtime_mode=configured,
        local_degraded_protection=protection.local,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    response = await health.health_traffic(request)
    assert response.status_code == expected_status
    assert json.loads(response.body)["runtime_mode"] == expected_mode
