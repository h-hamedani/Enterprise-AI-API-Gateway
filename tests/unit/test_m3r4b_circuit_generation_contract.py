from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R4B_Circuit_Generation_Eligibility_Token_Contract_Reconciliation.md"
)


def _contract() -> str:
    return " ".join(CONTRACT_PATH.read_text(encoding="utf-8").split())


def test_state_only_check_and_normal_token() -> None:
    contract = _contract()
    assert "state-only CLOSED check is explicitly insufficient" in contract
    assert "NormalEligibilityToken" in contract
    assert "`incarnation_id` and `generation`" in contract
    assert "bound to the exact circuit identity" in contract
    assert "not reconstructed from current state" in contract


def test_missing_state_and_restart_are_safe() -> None:
    contract = _contract()
    assert "state = CLOSED" in contract
    assert "generation = 1" in contract
    assert "atomically materialize metadata" in contract
    assert "winning persisted incarnation" in contract
    assert "fresh incarnation even if generation returns to 1" in contract
    assert "same-incarnation, same-generation result while still CLOSED" in contract


def test_generation_transition_matrix_and_stale_outcomes() -> None:
    contract = _contract()
    for transition in (
        "CLOSED -> OPEN | unchanged",
        "OPEN -> HALF_OPEN | unchanged",
        "Expired HALF_OPEN owner replaced | unchanged",
        "HALF_OPEN probe failure -> OPEN | unchanged",
        "Active-owner probe success -> CLOSED | increment exactly once",
    ):
        assert transition in contract
    assert "old token in OPEN, in HALF_OPEN" in contract
    assert "after successful recovery to CLOSED" in contract
    assert "both token fields match current metadata" in contract


def test_result_atomicity_and_probe_separation() -> None:
    contract = _contract()
    assert "Normal `record_success` applies only if current state is CLOSED" in contract
    assert "Normal `record_failure` has the same eligibility predicate" in contract
    assert "`applied = false` with no mutation" in contract
    assert "single Redis-side atomic operation" in contract
    assert "exact active, unexpired `probe_id` owner" in contract
    assert "normal eligibility token does not grant probe ownership" in contract


def test_overflow_and_scope() -> None:
    contract = _contract()
    assert "9007199254740991" in contract
    assert "typed internal circuit-state error" in contract
    assert "must not wrap, reset to 1 under the same incarnation" in contract
    assert "gw:v1:cb:{tenant}:{target_kind}:{target_id}:{dimension}" in contract
    assert "no Alembic migration is needed" in contract
    assert "no Control Plane or public HTTP schema change" in contract
    assert "not use the incarnation ID, generation, eligibility" in contract
