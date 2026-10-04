from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.control_plane.normal_api_registry import router
from app.api.dependencies import require_admin_context
from app.control_plane.auth import AdminContext
from app.core.config import Settings
from app.core.errors import install_error_handlers
from app.core.request_context import request_context_middleware
from app.main import create_app
from app.schemas.control_plane import ServiceCreate, ServicePatch

OPENAPI_PATH = (
    Path(__file__).parents[2]
    / "docs/04-api/Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml"
)
SERVICE_INPUT = {
    "name": "orders",
    "base_url": "https://upstream.example",
    "slug": "orders",
}


@pytest.mark.parametrize("value", [1, 12345, 67108864])
def test_service_create_and_patch_accept_bounded_integer(value: int) -> None:
    assert (
        ServiceCreate(
            **SERVICE_INPUT, request_body_limit_bytes=value
        ).request_body_limit_bytes
        == value
    )
    assert ServicePatch(request_body_limit_bytes=value).model_dump(
        exclude_unset=True
    ) == {"request_body_limit_bytes": value}


def test_service_default_and_patch_omission() -> None:
    assert ServiceCreate(**SERVICE_INPUT).request_body_limit_bytes == 10485760
    assert "request_body_limit_bytes" not in ServicePatch(name="new").model_dump(
        exclude_unset=True
    )


@pytest.mark.parametrize("value", [None, 0, -1, 67108865, "1", 1.5, True])
def test_invalid_admin_service_limit_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        ServiceCreate(**SERVICE_INPUT, request_body_limit_bytes=value)
    with pytest.raises(ValidationError):
        ServicePatch(request_body_limit_bytes=value)


def test_invalid_admin_limit_uses_existing_400_envelope() -> None:
    app = FastAPI()
    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    app.include_router(router)
    app.dependency_overrides[require_admin_context] = lambda: AdminContext(
        uuid4(), uuid4(), uuid4(), uuid4()
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/admin/services",
            json={**SERVICE_INPUT, "request_body_limit_bytes": None},
        )
        patched = client.patch(
            f"/api/v1/admin/services/{uuid4()}",
            json={"request_body_limit_bytes": 67108865},
        )
    for response in (created, patched):
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"


def test_generated_and_static_service_body_limit_schemas_match() -> None:
    generated = create_app().openapi()["components"]["schemas"]
    static = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))["components"][
        "schemas"
    ]
    for static_name, generated_name in (
        ("ServiceCreate", "ServiceCreate"),
        ("ServicePatch", "ServicePatch"),
        ("Service", "ServiceResponse"),
    ):
        static_schema = static[static_name]
        generated_schema = generated[generated_name]
        static_property = static_schema["properties"]["request_body_limit_bytes"]
        generated_property = generated_schema["properties"]["request_body_limit_bytes"]
        for key in ("type", "minimum", "maximum"):
            assert generated_property[key] == static_property[key]
        assert static_property["type"] == "integer"
        assert "null" not in str(generated_property.get("type"))
        assert "anyOf" not in generated_property
        assert ("request_body_limit_bytes" in static_schema.get("required", [])) == (
            "request_body_limit_bytes" in generated_schema.get("required", [])
        )
    assert (
        static["ServiceCreate"]["properties"]["request_body_limit_bytes"]["default"]
        == generated["ServiceCreate"]["properties"]["request_body_limit_bytes"][
            "default"
        ]
        == 10485760
    )
    assert (
        "default"
        not in generated["ServicePatch"]["properties"]["request_body_limit_bytes"]
    )
    assert (
        "default"
        not in static["ServicePatch"]["properties"]["request_body_limit_bytes"]
    )


def test_deployment_setting_loads_default_and_integer_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NORMAL_API_REQUEST_BODY_LIMIT_BYTES", raising=False)
    assert Settings(_env_file=None).normal_api_request_body_limit_bytes == 10485760
    for value in ("1", "10485760", "67108864", "01"):
        monkeypatch.setenv("NORMAL_API_REQUEST_BODY_LIMIT_BYTES", value)
        assert Settings(_env_file=None).normal_api_request_body_limit_bytes == int(
            value
        )


@pytest.mark.parametrize("value", ["0", "67108865", "1.5", "bad", ""])
def test_invalid_deployment_environment_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("NORMAL_API_REQUEST_BODY_LIMIT_BYTES", value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", [None, True, 1.0])
def test_invalid_programmatic_deployment_limit_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, normal_api_request_body_limit_bytes=value)
