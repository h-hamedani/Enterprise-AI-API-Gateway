from __future__ import annotations

from collections import Counter
from pathlib import Path

import yaml

from app.main import create_app

OPENAPI_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml"
)

PAGINATED = {
    "/api/v1/admin/applications": ("ApplicationPage", "Application"),
    "/api/v1/admin/api-keys": ("ApiKeyPage", "ApiKeyMetadata"),
    "/api/v1/admin/services": ("ServicePage", "Service"),
    "/api/v1/admin/routes": ("RoutePage", "Route"),
    "/api/v1/admin/llm/providers": ("LlmProviderPage", "LlmProvider"),
    "/api/v1/admin/llm/targets": ("LlmTargetPage", "LlmTarget"),
    "/api/v1/admin/llm/models": ("LlmModelPage", "LlmModel"),
    "/api/v1/admin/llm/aliases": ("LlmAliasPage", "LlmAlias"),
    "/api/v1/admin/audit-logs": ("AuditPage", None),
}


def _spec():
    return yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))


def test_all_paginated_admin_reads_have_frozen_parameters_and_page_schema():
    spec = _spec()
    parameters = spec["components"]["parameters"]
    assert parameters["Limit"]["schema"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": 200,
        "default": 50,
    }
    assert parameters["Cursor"]["required"] is False
    assert parameters["Cursor"]["schema"]["type"] == "string"

    for path, (page_name, item_name) in PAGINATED.items():
        operation = spec["paths"][path]["get"]
        refs = {parameter["$ref"] for parameter in operation["parameters"]}
        assert refs == {
            "#/components/parameters/Limit",
            "#/components/parameters/Cursor",
        }
        response = operation["responses"]["200"]["content"]["application/json"]
        assert response["schema"]["$ref"] == f"#/components/schemas/{page_name}"
        page = spec["components"]["schemas"][page_name]
        assert set(page["required"]) == {"data", "next_cursor"}
        assert page["properties"]["next_cursor"]["type"] == ["string", "null"]
        if item_name is not None:
            assert page["properties"]["data"]["items"]["$ref"] == (
                f"#/components/schemas/{item_name}"
            )


def test_child_collections_remain_unpaginated_and_reads_use_admin_token_only():
    spec = _spec()
    for path in (
        "/api/v1/admin/api-keys/{api_key_id}/permissions",
        "/api/v1/admin/llm/models/{model_id}/prices",
    ):
        parameters = spec["paths"][path]["get"].get("parameters", [])
        refs = {parameter.get("$ref") for parameter in parameters}
        assert "#/components/parameters/Limit" not in refs
        assert "#/components/parameters/Cursor" not in refs

    for path in (*PAGINATED, "/api/v1/admin/config-version", "/api/v1/admin/health"):
        assert spec["paths"][path]["get"]["security"] == [{"AdminToken": []}]


def test_openapi_refs_resolve_and_operation_ids_are_unique():
    spec = _spec()
    operation_ids = []

    def walk(node):
        if isinstance(node, dict):
            reference = node.get("$ref")
            if reference is not None and reference.startswith("#/"):
                target = spec
                for part in reference[2:].split("/"):
                    target = target[part.replace("~1", "/").replace("~0", "~")]
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(spec)
    for path_item in spec["paths"].values():
        for method, operation in path_item.items():
            if method in {"get", "post", "put", "patch", "delete"}:
                operation_ids.append(operation["operationId"])
    assert all(count == 1 for count in Counter(operation_ids).values())


def test_later_milestone_routes_are_documented_but_not_reclassified_as_m2():
    spec = _spec()
    assert "/api/v1/admin/rate-limits/{scope_type}/{scope_id}" in spec["paths"]
    assert "/api/v1/admin/session" in spec["paths"]
    live_paths = create_app().openapi()["paths"]
    assert "/api/v1/admin/rate-limits/{scope_type}/{scope_id}" not in live_paths
    assert "/api/v1/admin/session" not in live_paths


def test_every_implemented_m2_admin_operation_uses_admin_token_only():
    spec = _spec()
    live = create_app().openapi()
    for path, path_item in live["paths"].items():
        if not path.startswith("/api/v1/admin"):
            continue
        for method in {"get", "post", "put", "patch", "delete"} & path_item.keys():
            assert spec["paths"][path][method]["security"] == [{"AdminToken": []}]
