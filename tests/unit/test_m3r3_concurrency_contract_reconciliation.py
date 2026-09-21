from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R3_Leased_Concurrency_Semaphore_Contract_Reconciliation.md"
)


def _contract() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def _prose() -> str:
    return " ".join(_contract().split())


def test_freezes_lease_configuration_and_identity() -> None:
    contract = _prose()
    assert "`concurrency_lease_duration_ms`" in contract
    assert "default of `30000`, minimum `5000`, and maximum `120000`" in contract
    assert "opaque UUIDv7 `lease_id`" in contract
    assert "same lease ID owns every acquired layer" in contract


def test_freezes_redis_time_expiry_and_state_model() -> None:
    contract = _contract()
    assert "member is `lease_id`" in contract
    assert "score is absolute\n`expires_at_ms`" in contract
    assert "Redis `TIME` is the sole\nclock authority" in contract
    assert "expires_at_ms <= now_ms" in contract
    assert "prunes scores through\n`now_ms`" in contract


def test_freezes_atomic_all_or_nothing_layered_acquire() -> None:
    contract = _prose()
    assert "either adds the same lease ID to every key or none" in contract
    assert "Sequential partial acquisition" in contract
    assert "grants no acquisition priority" in contract
    assert "`API_KEY`, `ROUTE`, `SERVICE`" in contract


def test_freezes_renewal_formula_and_unlimited_lifetime() -> None:
    contract = _prose()
    assert "new_expires_at_ms = now_ms + concurrency_lease_duration_ms" in contract
    assert "Renewal never extends from the old expiry" in contract
    assert "an expired lease cannot be resurrected" in contract
    assert "no maximum renewal count or cumulative lease lifetime" in contract
    assert "Missing or expired ownership in any layer renews none" in contract


def test_freezes_release_and_wrong_owner_behavior() -> None:
    contract = _prose()
    assert (
        "first complete release returns true and repeated release returns false"
        in contract
    )
    assert "removes that exact caller-owned residual ID wherever present" in contract
    assert "returns `released=false`" in contract
    assert "never removes or reveals another member" in contract


def test_freezes_crash_cleanup_restart_and_noscript() -> None:
    contract = _prose()
    assert "key_ttl_ms = 2 * concurrency_lease_duration_ms" in contract
    assert "without a worker" in contract
    assert "After Redis restart leases may be absent" in contract
    assert "one bounded `NOSCRIPT` reload and one retry" in contract


def test_freezes_failure_empty_policy_and_package_boundaries() -> None:
    contract = _prose()
    assert "`acquired=true` and `lease_id=null` without Redis access" in contract
    assert "typed concurrency dependency failure" in contract
    assert "never fails open" in contract
    assert "M3.8 owns degraded behavior" in contract
    assert "No table, column, constraint, migration, endpoint" in contract


def test_freezes_resolved_policy_and_security_boundaries() -> None:
    contract = _prose()
    for field in (
        "policy_id",
        "tenant_id",
        "scope_type",
        "scope_id",
        "max_concurrency",
    ):
        assert f"`{field}`" in contract
    assert "does not query PostgreSQL" in contract
    assert "one tenant" in contract
    assert "lease UUIDs, Redis keys" in contract
