from pathlib import Path

CONTRACT_PATH = (
    Path(__file__).parents[2]
    / "docs"
    / "04-api"
    / "Enterprise_AI_API_Gateway_M3-R1_Redis_Invalidation_Channel_Contract_Reconciliation.md"
)


def _contract() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def _prose() -> str:
    return " ".join(_contract().split())


def test_one_canonical_m3_channel_and_unambiguous_consumer_contract() -> None:
    contract = _prose()

    assert "exactly one canonical M3 channel" in contract
    assert "canonical M3 channel: `gw:v1:invalidate`" in contract
    assert "M3 consumers subscribe only to `gw:v1:invalidate`" in contract
    assert "must not subscribe to both channels" in contract


def test_historical_m2_compatibility_and_release_preservation_are_explicit() -> None:
    contract = _prose()

    assert "historical M2 compatibility channel: `gateway:config`" in contract
    assert "same encoded event to both channels" in contract
    assert "The `m2-pass` tag and all M2 commits remain immutable" in contract
    assert "does not retroactively rename unrelated M2 keys" in contract


def test_payload_contract_remains_exact_and_secret_safe() -> None:
    contract = _contract()

    payload_block = contract.split("```json", maxsplit=1)[1].split("```", maxsplit=1)[0]
    assert '"tenant_id": "<uuid>"' in payload_block
    assert '"resource_type": "<stable type>"' in payload_block
    assert '"resource_id": "<uuid>"' in payload_block
    assert '"version": 42' in payload_block
    for forbidden in (
        "timestamp",
        "sequence_id",
        "request_id",
        "secret",
        "authorization",
        "idempotency_key",
        "configuration_body",
    ):
        assert forbidden not in payload_block.lower()


def test_delivery_and_durable_authority_are_not_redefined() -> None:
    contract = _prose()

    assert "PostgreSQL remains the durable" in contract
    assert "Delivery continues to use Redis Pub/Sub" in contract
    assert "best effort, non-durable, and not exactly once" in contract
    assert "no outbox, durable event record, background retry worker" in contract
    assert "No database or Alembic migration is required" in contract


def test_package_boundary_defers_runtime_change_to_m36() -> None:
    contract = _prose()

    assert "M3.1 does not change invalidation publication or consumption" in contract
    assert "M3.6 owns the minimal dual-publication compatibility change" in contract
    assert "M3.7 owns PostgreSQL version reconciliation" in contract
