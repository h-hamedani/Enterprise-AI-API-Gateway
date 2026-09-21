from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R2_Atomic_Token_Bucket_Algorithm_Contract_Reconciliation.md"
)


def _contract() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def _prose() -> str:
    return " ".join(_contract().split())


def test_freezes_one_atomic_multi_key_all_or_nothing_evaluation() -> None:
    contract = _prose()

    assert "one Redis-side atomic Lua invocation" in contract
    assert "If any bucket rejects, no request-cost units are deducted" in contract
    assert "Sequential independent mutations are forbidden" in contract
    assert "no policy mutation priority" in contract


def test_freezes_resolved_policy_boundary_and_deterministic_order() -> None:
    contract = _prose()

    assert "M3.2 accepts already-resolved enabled rate policies" in contract
    for field in (
        "policy_id",
        "tenant_id",
        "scope_type",
        "scope_id",
        "requests_per_window",
        "window_seconds",
    ):
        assert f"`{field}`" in contract
    assert "does not discover policies in PostgreSQL" in contract
    assert "Entries at the same logical position are ordered by `policy_id`" in contract


def test_freezes_redis_time_and_exact_integer_refill() -> None:
    contract = _contract()

    assert "Redis `TIME` is the only shared clock authority" in contract
    assert "now_ms = seconds * 1000 + floor(microseconds / 1000)" in contract
    assert "capacity_units = C * W" in contract
    assert "one_request_cost_units = W" in contract
    assert "refill_units = effective_elapsed_ms * C" in contract
    assert "Binary floating-point token balances are forbidden" in contract
    assert "effective_elapsed_ms = min(elapsed_ms, W)" in contract


def test_freezes_initial_state_retry_rounding_and_maximum() -> None:
    contract = _contract()

    assert "A missing key begins full" in contract
    assert "policy_retry_after_ms = (deficit_units + C - 1) // C" in contract
    assert "exact ceiling division" in contract
    assert "retry_after_ms = max(policy_retry_after_ms" in contract
    assert "Allowed and empty-policy results have `retry_after_ms = 0`" in contract


def test_freezes_ttl_and_rejection_state_persistence() -> None:
    contract = _prose()

    assert "`last_refill_ms = now_ms`, is persisted" in contract
    assert "even when the request is rejected" in contract
    assert "ttl_ms = 2 * W" in contract
    assert "TTL is refreshed on every evaluation that persists state" in contract
    assert "Redis expiry is the only M3.2 cleanup mechanism" in contract


def test_freezes_numeric_safety_bound_without_schema_change() -> None:
    contract = _contract()

    assert "2^53 - 1" in contract
    assert "C * W <= 9007199254740991" in contract
    assert "reject a resolved policy before script execution" in contract
    assert "No database constraint or migration is added" in contract


def test_freezes_tenant_hash_tag_and_typed_key_construction() -> None:
    contract = _prose()

    assert "`gw:v1:rl:{tenant}:{scope_type}:{scope_id}`" in contract
    assert "same `tenant_id`" in contract
    assert "one Redis Cluster hash slot" in contract
    assert "Callers cannot provide arbitrary Redis key text" in contract


def test_freezes_redis_failure_and_package_boundaries() -> None:
    contract = _prose()

    assert "typed runtime dependency failure" in contract
    assert "does not fail open" in contract
    assert "M3.8 owns degraded behavior" in contract
    assert "one bounded script reload and one re-evaluation" in contract
    assert "adds no table, column, constraint, migration, public endpoint" in contract
