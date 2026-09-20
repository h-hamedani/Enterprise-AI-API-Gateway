from app.main import create_app


def test_frozen_m211_read_routes_are_exposed_and_protected():
    app = create_app()
    paths = app.openapi()["paths"]

    assert "get" in paths["/api/v1/admin/audit-logs"]
    assert "get" in paths["/api/v1/admin/config-version"]
    assert "get" in paths["/api/v1/admin/health"]
    assert paths["/api/v1/admin/audit-logs"]["get"]["security"]
    assert paths["/api/v1/admin/config-version"]["get"]["security"]
    assert paths["/api/v1/admin/health"]["get"]["security"]


def test_reconciled_response_models_exclude_superseded_fields():
    schemas = create_app().openapi()["components"]["schemas"]
    service_fields = schemas["ServiceResponse"]["properties"]
    route_fields = schemas["RouteResponse"]["properties"]
    price_fields = schemas["ModelPriceResponse"]["properties"]

    assert "timeout_ms" not in service_fields
    assert {
        "connect_timeout_seconds",
        "pool_timeout_seconds",
        "write_timeout_seconds",
        "read_idle_timeout_seconds",
        "pre_response_timeout_seconds",
    } <= service_fields.keys()
    assert "methods" not in route_fields
    assert {"method", "upstream_path_template", "header_policy", "timeout_ms"} <= (
        route_fields.keys()
    )
    assert {
        "input_price_per_unit",
        "output_price_per_unit",
        "unit_tokens",
        "currency",
        "effective_from",
        "effective_to",
    } <= price_fields.keys()
    assert "input_price_per_1m_tokens" not in price_fields
    assert "output_price_per_1m_tokens" not in price_fields
