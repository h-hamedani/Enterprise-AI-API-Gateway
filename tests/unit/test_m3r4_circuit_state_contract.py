from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R4_Distributed_Circuit_State_Contract_Reconciliation.md"
)


def _contract() -> str:
    return " ".join(CONTRACT_PATH.read_text(encoding="utf-8").split())


def test_addenda_config_and_identity() -> None:
    contract = _contract()
    assert "M3-R4A" in contract and "M3-R4B" in contract
    assert "typed, resolved `CircuitConfig`" in contract
    assert "not hard-coded algorithm branches" in contract
    assert "gw:v1:cb:{tenant}:{target_kind}:{target_id}:{dimension}" in contract
    assert "`route` | tenant-owned `normal_api_routes.id`" in contract
    assert "its `service_id`, the V1 service backend/upstream target" in contract
    assert "`provider_target` | tenant-owned `llm_provider_targets.id`" in contract
    assert "tenant-owned `llm_models.id`, the model/deployment identity" in contract
    assert "same `{tenant}` hash tag" in contract


def test_failure_window_and_closed_outcomes() -> None:
    contract = _contract()
    assert "`(now_ms - W, now_ms]`" in contract
    assert "`<= now_ms - W`" in contract
    assert "CircuitConfig.failure_threshold" in contract
    assert "current state is CLOSED and both token" in contract
    assert "it clears failure history and remains CLOSED" in contract
    assert "Normal failure has the same applicability predicate" in contract
    assert "`applied = false` with no mutation" in contract
    assert "CLOSED(7) is rejected in OPEN(7), HALF_OPEN(7)" in contract
    assert "recovered CLOSED(8)" in contract
    assert "previous incarnation is rejected after Redis" in contract


def test_open_probe_expiry_and_results() -> None:
    contract = _contract()
    assert "`open_until_ms = now_ms + CircuitConfig.open_duration_ms`" in contract
    assert "`now_ms < open_until_ms`" in contract
    assert "`now_ms >= open_until_ms`" in contract
    assert "exactly one distributed winner" in contract
    assert "CircuitConfig.probe_lease_duration_ms" in contract
    assert "`probe_expires_at_ms <= now_ms`" in contract
    assert "immediately replace an expired owner" in contract
    assert "increment generation exactly once" in contract
    assert "On failure, atomically reopen" in contract
    assert "Wrong, previous, expired, or missing ownership" in contract
    assert "normal token never authorizes a probe result" in contract


def test_clock_atomicity_restart_and_scope() -> None:
    contract = _contract()
    assert "Redis TIME is the sole distributed correctness clock" in contract
    assert "seconds * 1000 + floor(microseconds / 1000)" in contract
    assert "atomically materializes a fresh" in contract
    assert "one Redis-side atomic operation" in contract
    assert "Old normal tokens and probe IDs cannot" in contract
    assert "Existing absolute `open_until_ms`" in contract
    assert "typed circuit dependency failure" in contract
    assert "M3.8 owns conservative degraded local protection" in contract
    assert "no circuit database table" in contract
    assert "public HTTP field" in contract


def test_security_and_overflow() -> None:
    contract = _contract()
    assert "9007199254740991" in contract
    assert "before any partial mutation" in contract
    assert "no additional independent epoch" in contract
    assert "unique opaque collision-resistant event ID" in contract
    assert "Never label metrics with tenant/target UUIDs" in contract
    assert "must not contain API keys, Admin tokens" in contract
