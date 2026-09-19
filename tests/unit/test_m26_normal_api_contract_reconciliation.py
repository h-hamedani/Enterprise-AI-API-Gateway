from pathlib import Path

import yaml

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
