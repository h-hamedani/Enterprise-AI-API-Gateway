"""Bounded internal outcomes for exhausted local degraded stores."""

import logging
from uuid import uuid4

import pytest

from app.redis.circuit import CircuitConfig, CircuitIdentity, CircuitState
from app.redis.local_degraded import LocalDegradedProtection, logger
from tests.unit.test_m38_local_degraded import concurrency, rate


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def record(self, store: str, outcome: str) -> None:
        self.events.append((store, outcome))


@pytest.mark.asyncio
async def test_rate_exhaustion_records_one_bounded_outcome() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    assert (await local.rate.evaluate([rate(1)])).allowed
    assert not (await local.rate.evaluate([rate(1)])).allowed
    assert telemetry.events == [("rate", "local_capacity_exhausted")]


@pytest.mark.asyncio
async def test_pre_auth_exhaustion_has_its_own_category() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    assert (await local.pre_auth.evaluate(uuid4())).allowed
    assert not (await local.pre_auth.evaluate(uuid4())).allowed
    assert telemetry.events == [("pre_auth", "local_capacity_exhausted")]


@pytest.mark.asyncio
async def test_concurrency_store_exhaustion_records_one_outcome() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    assert (await local.concurrency.acquire([concurrency(1)])).acquired
    assert not (await local.concurrency.acquire([concurrency(1)])).acquired
    assert telemetry.events == [("concurrency", "local_capacity_exhausted")]


@pytest.mark.asyncio
async def test_circuit_store_exhaustion_records_one_outcome() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    first = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    second = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    assert (await local.circuit.check_or_claim_eligibility(first, config)).eligible
    result = await local.circuit.check_or_claim_eligibility(second, config)
    assert not result.eligible
    assert telemetry.events == [("circuit", "local_capacity_exhausted")]


@pytest.mark.asyncio
async def test_ordinary_rate_rejection_is_not_store_exhaustion() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    policy = rate(1)
    assert (await local.rate.evaluate([policy])).allowed
    assert not (await local.rate.evaluate([policy])).allowed
    assert telemetry.events == []


@pytest.mark.asyncio
async def test_ordinary_concurrency_rejection_is_not_store_exhaustion() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    policy = concurrency(1)
    assert (await local.concurrency.acquire([policy])).acquired
    assert not (await local.concurrency.acquire([policy])).acquired
    assert telemetry.events == []


@pytest.mark.asyncio
async def test_open_and_active_probe_rejections_are_not_store_exhaustion() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    config = CircuitConfig(2, 60000, 30000, 1, 1, 30000)
    identity = CircuitIdentity._create(uuid4(), "route", uuid4(), uuid4())
    probe = await local.circuit.check_or_claim_eligibility(identity, config)
    assert not (
        await local.circuit.check_or_claim_eligibility(identity, config)
    ).eligible
    await local.circuit.record_failure(identity, probe.probe_id, config)
    rejected = await local.circuit.check_or_claim_eligibility(identity, config)
    assert not rejected.eligible and rejected.state is CircuitState.OPEN
    assert telemetry.events == []


@pytest.mark.asyncio
async def test_telemetry_carries_no_identifiers_or_secrets() -> None:
    telemetry = RecordingTelemetry()
    local = LocalDegradedProtection(max_entries=1, telemetry=telemetry)
    tenant_id = uuid4()
    policy_id = uuid4()
    first = rate(1, tenant=tenant_id, scope=policy_id)
    assert (await local.rate.evaluate([first])).allowed
    assert not (await local.rate.evaluate([rate(1)])).allowed
    assert telemetry.events == [("rate", "local_capacity_exhausted")]
    serialized = repr(telemetry.events)
    assert str(tenant_id) not in serialized
    assert str(policy_id) not in serialized


@pytest.mark.asyncio
async def test_default_telemetry_log_contains_only_bounded_outcome(
    caplog, monkeypatch
) -> None:
    # Alembic's fileConfig disables pre-existing application loggers in the
    # full suite; this test must explicitly enable the logger it captures.
    monkeypatch.setattr(logger, "disabled", False)
    caplog.set_level(logging.WARNING, logger="app.redis.local_degraded")
    local = LocalDegradedProtection(max_entries=1)
    tenant_id = uuid4()
    scope_id = uuid4()
    assert (
        await local.rate.evaluate([rate(1, tenant=tenant_id, scope=scope_id)])
    ).allowed
    assert not (await local.rate.evaluate([rate(1)])).allowed
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert message == (
        "local degraded protection event "
        "protection_store=rate protection_outcome=local_capacity_exhausted"
    )
    assert str(tenant_id) not in message
    assert str(scope_id) not in message
