from pathlib import Path

import yaml

OPENAPI_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml"
)
ECONOMIC_FIELDS = {
    "input_price_per_unit",
    "output_price_per_unit",
    "unit_tokens",
    "currency",
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


def test_price_create_matches_non_null_physical_economic_fields() -> None:
    schema = _document()["components"]["schemas"]["ModelPriceCreate"]
    properties = schema["properties"]

    assert ECONOMIC_FIELDS | {"effective_from"} <= set(schema["required"])
    assert ECONOMIC_FIELDS <= properties.keys()
    assert properties["input_price_per_unit"]["type"] == "number"
    assert properties["output_price_per_unit"]["type"] == "number"
    assert properties["unit_tokens"] == {"type": "integer", "minimum": 1}
    assert properties["currency"]["pattern"] == "^[A-Z]{3}$"
    for field in ECONOMIC_FIELDS:
        assert properties[field].get("default") is None
        assert properties[field]["type"] not in ("null", ["null"])


def test_price_patch_distinguishes_omission_from_explicit_null() -> None:
    schema = _document()["components"]["schemas"]["ModelPricePatch"]
    properties = schema["properties"]

    assert "required" not in schema
    for field in ECONOMIC_FIELDS | {"effective_from"}:
        assert properties[field]["type"] not in ("null", ["null"])
    assert properties["effective_to"]["type"] == ["string", "null"]


def test_per_million_and_implicit_assumptions_are_absent() -> None:
    schemas = _document()["components"]["schemas"]
    serialized = str(
        {
            name: schemas[name]
            for name in ("ModelPriceCreate", "ModelPricePatch", "ModelPrice")
        }
    )

    assert "input_price_per_1m_tokens" not in serialized
    assert "output_price_per_1m_tokens" not in serialized
    assert "USD" not in serialized
    assert "1000000" not in serialized
    assert schemas["ModelPrice"]["allOf"][0]["$ref"].endswith("/ModelPriceCreate")


def test_pricing_paths_remain_stable_and_openapi_references_resolve() -> None:
    document = _document()
    paths = document["paths"]

    assert "/api/v1/admin/llm/models/{model_id}/prices" in paths
    assert "/api/v1/admin/model-prices/{price_id}" in paths
    assert paths["/api/v1/admin/llm/models/{model_id}/prices"]["post"]["requestBody"][
        "content"
    ]["application/json"]["schema"]["$ref"].endswith("/ModelPriceCreate")

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
