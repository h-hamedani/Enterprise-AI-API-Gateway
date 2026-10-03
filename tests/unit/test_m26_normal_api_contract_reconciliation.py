from pathlib import Path

import yaml

from app.main import create_app
from app.persistence.models import Base

OPENAPI_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml"
)
TIMEOUT_FIELDS = {
    "connect_timeout_seconds": 5,
    "pool_timeout_seconds": 5,
    "write_timeout_seconds": 30,
    "read_idle_timeout_seconds": 60,
    "pre_response_timeout_seconds": 120,
}


def _document() -> dict:
    return yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def test_openapi_parses_and_all_local_references_resolve() -> None:
    document = _document()

    assert document["openapi"] == "3.1.0"
    assert {"info", "paths", "components"} <= document.keys()
    operation_ids: list[str] = []
    for node in _walk(document):
        reference = node.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/"):
            target = document
            for segment in reference[2:].split("/"):
                target = target[segment.replace("~1", "/").replace("~0", "~")]
            assert target is not None
        operation_id = node.get("operationId")
        if operation_id is not None:
            operation_ids.append(operation_id)
    assert len(operation_ids) == len(set(operation_ids))


def test_service_schemas_use_physical_phase_timeouts() -> None:
    schemas = _document()["components"]["schemas"]

    for schema_name in ("Service", "ServiceCreate", "ServicePatch"):
        properties = schemas[schema_name]["properties"]
        assert "timeout_ms" not in properties
        assert TIMEOUT_FIELDS.keys() <= properties.keys()
        for field in TIMEOUT_FIELDS:
            assert properties[field]["type"] == "integer"
            assert properties[field]["minimum"] == 1

    assert TIMEOUT_FIELDS.keys() <= set(schemas["Service"]["required"])
    for field, default in TIMEOUT_FIELDS.items():
        assert schemas["ServiceCreate"]["properties"][field]["default"] == default


def test_route_schemas_are_method_specific_and_expose_physical_configuration() -> None:
    schemas = _document()["components"]["schemas"]

    for schema_name in ("RouteCreate", "RoutePatch"):
        properties = schemas[schema_name]["properties"]
        assert "methods" not in properties
        assert properties["method"] == {
            "type": "string",
            "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"],
        }
        assert "upstream_path_template" in properties
        assert "header_policy" in properties

    route_create = schemas["RouteCreate"]
    assert "method" in route_create["required"]
    assert "upstream_path_template" in route_create["required"]
    assert "method" in schemas["RoutePatch"]["properties"]
    assert schemas["Route"]["allOf"][0]["$ref"].endswith("/RouteCreate")


def test_route_header_policy_openapi_is_nullable_and_strict() -> None:
    schemas = _document()["components"]["schemas"]
    for schema_name in ("RouteCreate", "RoutePatch"):
        policy = schemas[schema_name]["properties"]["header_policy"]
        assert policy["type"] == ["object", "null"]
        assert policy["required"] == ["request_allowlist"]
        assert policy["additionalProperties"] is False
        names = policy["properties"]["request_allowlist"]
        assert names["type"] == "array"
        assert names["uniqueItems"] is True
        assert names["items"]["type"] == "string"
        assert names["items"]["minLength"] == 1
        assert names["items"]["pattern"]
    assert "header_policy" in schemas["Route"]["allOf"][1]["required"]


def test_generated_openapi_route_policy_matches_static_contract() -> None:
    generated = create_app().openapi()["components"]["schemas"]
    static = _document()["components"]["schemas"]
    generated_policy = generated["RouteHeaderPolicy"]
    generated_names = generated_policy["properties"]["request_allowlist"]
    for name in ("RouteCreate", "RoutePatch"):
        static_usage = static[name]["properties"]["header_policy"]
        generated_usage = generated[name]["properties"]["header_policy"]
        assert {item.get("type") for item in generated_usage["anyOf"]} == {
            None,
            "null",
        }
        assert {item.get("$ref") for item in generated_usage["anyOf"]} == {
            None,
            "#/components/schemas/RouteHeaderPolicy",
        }
        assert static_usage["type"] == ["object", "null"]
        assert generated_policy["type"] == "object"
        assert (
            generated_policy["additionalProperties"]
            == static_usage["additionalProperties"]
        )
        assert generated_policy["required"] == static_usage["required"]
        static_names = static_usage["properties"]["request_allowlist"]
        for key in ("type", "uniqueItems"):
            assert generated_names[key] == static_names[key]
        for key in ("type", "minLength", "pattern"):
            assert generated_names["items"][key] == static_names["items"][key]
        assert "case-insensitive" in generated_names["description"]
        assert "case-insensitive" in static_usage["description"]
    assert "header_policy" in generated["RouteResponse"]["required"]
    assert "header_policy" in static["Route"]["allOf"][1]["required"]


def test_normal_api_endpoint_paths_are_unchanged() -> None:
    paths = _document()["paths"]
    assert {
        "/api/v1/admin/services",
        "/api/v1/admin/services/{service_id}",
        "/api/v1/admin/services/{service_id}/credential",
        "/api/v1/admin/routes",
        "/api/v1/admin/routes/{route_id}",
    } <= paths.keys()


def test_existing_schema_needs_no_route_grouping_structure() -> None:
    route = Base.metadata.tables["normal_api_routes"]

    assert "method" in route.c
    assert "methods" not in route.c
    assert "route_group_id" not in route.c
    assert "logical_routes" not in Base.metadata.tables
    assert "route_methods" not in Base.metadata.tables
