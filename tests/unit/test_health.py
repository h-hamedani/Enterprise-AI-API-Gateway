from uuid import UUID

from fastapi.testclient import TestClient

from app.main import create_app


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
