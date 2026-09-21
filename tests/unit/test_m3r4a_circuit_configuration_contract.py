from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R4A_Circuit_Configuration_Source_Contract_Reconciliation.md"
)


def _contract() -> str:
    return " ".join(CONTRACT_PATH.read_text(encoding="utf-8").split())


def test_authority_scope_and_hld_configurability_are_explicit() -> None:
    contract = _contract()
    assert (
        "validated deployment runtime settings are the circuit parameter authority"
        in contract
    )
    assert "deployment-wide infrastructure configuration" in contract
    assert "configurable defaults, not immutable constants" in contract
    assert "not a dynamic per-tenant Control Plane setting" in contract
    assert "identical values" in contract


def test_frozen_defaults_and_v1_single_probe_close_constraints() -> None:
    contract = _contract()
    for setting, default in (
        ("circuit_failure_threshold", "5"),
        ("circuit_failure_window_ms", "60000"),
        ("circuit_open_duration_ms", "30000"),
        ("circuit_half_open_probe_limit", "1"),
        ("circuit_successes_to_close", "1"),
        ("circuit_probe_lease_duration_ms", "30000"),
    ):
        assert f"`{setting}` | `{default}`" in contract
    assert "configured values other than 1" in contract
    assert "fail startup" in contract
    assert "restricts `circuit_successes_to_close` to 1" in contract


def test_validation_reload_and_existing_state_rules() -> None:
    contract = _contract()
    assert (
        "Booleans, fractional values, and string-to-integer coercion are invalid"
        in contract
    )
    assert "No arbitrary upper bound is invented" in contract
    assert "validated at application startup before serving traffic" in contract
    assert "changing them requires process restart" in contract
    assert "deadlines retain their absolute meaning" in contract
    assert "future transitions use the current settings" in contract


def test_no_schema_control_plane_or_runtime_expansion() -> None:
    contract = _contract()
    assert "typed, validated `CircuitConfig`" in contract
    assert "no circuit runtime, Lua script, dynamic reload" in contract
    assert "Alembic migration, or Control Plane endpoint" in contract
    assert "Redis remains ephemeral circuit coordination state" in contract
