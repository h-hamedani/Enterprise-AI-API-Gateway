import asyncio
import time
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.api.health import _check_postgres
from app.main import create_app


@pytest.mark.asyncio
async def test_postgres_readiness_times_out_within_one_second() -> None:
    class HangingConnection:
        async def __aenter__(self):
            await asyncio.Event().wait()

        async def __aexit__(self, *_args):
            return None

    class HangingEngine:
        def connect(self):
            return HangingConnection()

    started = time.monotonic()
    assert await _check_postgres(HangingEngine()) is False
    assert time.monotonic() - started < 1.2


def test_health_live() -> None:
    app = create_app()

    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    request_id = response.headers["X-Request-ID"]
    parsed_request_id = UUID(request_id)

    assert parsed_request_id.version == 7


def test_client_request_id_does_not_replace_gateway_request_id() -> None:
    app = create_app()

    with TestClient(app) as client:
        response = client.get(
            "/health/live",
            headers={"X-Request-ID": "client-correlation-id"},
        )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] != "client-correlation-id"
    assert UUID(response.headers["X-Request-ID"]).version == 7
