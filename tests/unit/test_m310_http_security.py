import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
import yaml
from starlette.requests import Request

from app.api import health
from app.core.errors import GatewayHttpError, _internal_error_handler, error_response


def _request() -> Request:
    return Request(
        {"type": "http", "method": "GET", "path": "/health/ready", "headers": []}
    )


@pytest.mark.asyncio
async def test_ready_success_matches_frozen_health_response(monkeypatch) -> None:
    async def healthy(_engine):
        return True

    class Redis:
        async def check(self):
            return SimpleNamespace(available=True)

    monkeypatch.setattr(health, "_check_postgres", healthy)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(db_engine=object(), redis_runtime=Redis())
        )
    )
    response = await health.health_ready(request)
    body = json.loads(response.body)
    spec = yaml.safe_load(
        (
            Path(__file__).parents[2]
            / "docs/04-api/Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml"
        ).read_text(encoding="utf-8")
    )
    schema = spec["components"]["schemas"]["HealthResponse"]
    assert response.status_code == 200
    assert body == {"status": "ready"}
    assert schema["additionalProperties"] is False
    assert set(body) <= set(schema["properties"])
    assert set(schema["required"]) <= set(body)


def test_typed_dependency_error_returns_no_raw_dependency_content() -> None:
    response = error_response(
        _request(),
        GatewayHttpError(503, "upstream_unavailable", "Dependency unavailable."),
    )
    body = response.body.decode()
    assert response.status_code == 503
    assert "redis://" not in body
    assert "SELECT " not in body
    assert "password" not in body
    assert UUID(json.loads(body)["request_id"]).version == 7


@pytest.mark.asyncio
async def test_internal_error_handler_suppresses_raw_exception_and_stack(
    caplog,
) -> None:
    request = _request()
    secret = "redis://user:password@localhost/0 SELECT * FROM secrets"
    response = await _internal_error_handler(request, RuntimeError(secret))
    assert response.status_code == 500
    assert secret not in response.body.decode()
    assert secret not in caplog.text
    assert "Traceback" not in response.body.decode()
