from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert, inspect, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.core.config import get_settings
from app.persistence.models import Base
from app.persistence.models.llm_registry import LlmProviderTarget


@pytest.fixture
def db_connection():
    settings = get_settings()
    if settings.environment == "production":
        pytest.fail("Integration database tests must never run in production.")

    engine = create_engine(settings.postgres_migration_dsn)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()
    engine.dispose()


def now() -> datetime:
    return datetime.now(UTC)


def add_tenant(connection, *, name: str):
    tenant_id = uuid4()
    timestamp = now()
    connection.execute(
        insert(Base.metadata.tables["tenants"]).values(
            id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return tenant_id


def add_provider(connection, tenant_id, *, name="provider", provider_type="OPENAI"):
    provider_id = uuid4()
    timestamp = now()
    connection.execute(
        insert(Base.metadata.tables["llm_providers"]).values(
            id=provider_id,
            tenant_id=tenant_id,
            name=name,
            provider_type=provider_type,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return provider_id


def target_values(tenant_id, provider_id, *, name="target", **overrides):
    timestamp = now()
    values = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "provider_id": provider_id,
        "name": name,
        "base_url": "https://provider.example.test",
        "status": "ACTIVE",
        "certification_status": "UNVERIFIED",
        "allow_uncertified_runtime": False,
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    values.update(overrides)
    return values


def add_target(connection, tenant_id, provider_id, *, name="target", **overrides):
    values = target_values(tenant_id, provider_id, name=name, **overrides)
    connection.execute(
        insert(Base.metadata.tables["llm_provider_targets"]).values(**values)
    )
    return values["id"]


def add_model(connection, tenant_id, target_id, *, provider_model_name="model"):
    model_id = uuid4()
    timestamp = now()
    connection.execute(
        insert(Base.metadata.tables["llm_models"]).values(
            id=model_id,
            tenant_id=tenant_id,
            provider_target_id=target_id,
            provider_model_name=provider_model_name,
            display_name="Model",
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return model_id


def registry_graph(connection, *, suffix: str):
    tenant_id = add_tenant(connection, name=f"tenant-{suffix}")
    provider_id = add_provider(connection, tenant_id, name=f"provider-{suffix}")
    target_id = add_target(connection, tenant_id, provider_id, name=f"target-{suffix}")
    model_id = add_model(
        connection, tenant_id, target_id, provider_model_name=f"model-{suffix}"
    )
    return tenant_id, provider_id, target_id, model_id


def test_provider_name_uniqueness_is_tenant_scoped(db_connection):
    tenant_a = add_tenant(db_connection, name="provider-tenant-a")
    tenant_b = add_tenant(db_connection, name="provider-tenant-b")
    add_provider(db_connection, tenant_a, name="shared")
    add_provider(db_connection, tenant_b, name="shared")

    with pytest.raises(IntegrityError):
        add_provider(db_connection, tenant_a, name="shared")


@pytest.mark.parametrize(
    "provider_type",
    ["OPENAI", "ANTHROPIC", "VLLM", "GENERIC_OPENAI_COMPAT"],
)
def test_all_provider_types_are_accepted(db_connection, provider_type):
    tenant_id = add_tenant(db_connection, name=f"provider-type-{provider_type.lower()}")
    add_provider(db_connection, tenant_id, provider_type=provider_type)


def test_unknown_provider_type_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, name="provider-type-invalid")
    with pytest.raises(DBAPIError):
        add_provider(db_connection, tenant_id, provider_type="UNKNOWN")


def test_target_identity_and_tenant_provider_fk(db_connection):
    tenant_a = add_tenant(db_connection, name="target-tenant-a")
    tenant_b = add_tenant(db_connection, name="target-tenant-b")
    provider_a = add_provider(db_connection, tenant_a, name="a")
    provider_b = add_provider(db_connection, tenant_a, name="b")
    add_target(db_connection, tenant_a, provider_a, name="shared")
    add_target(db_connection, tenant_a, provider_b, name="shared")

    with pytest.raises(IntegrityError):
        add_target(db_connection, tenant_b, provider_a, name="cross-tenant")


def test_duplicate_target_identity_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, name="target-duplicate")
    provider_id = add_provider(db_connection, tenant_id)
    add_target(db_connection, tenant_id, provider_id)
    with pytest.raises(IntegrityError):
        add_target(db_connection, tenant_id, provider_id)


def test_target_application_defaults_are_exact_and_concurrency_has_none():
    target = LlmProviderTarget(
        tenant_id=uuid4(),
        provider_id=uuid4(),
        name="defaults",
        base_url="https://provider.example.test",
        created_at=now(),
        updated_at=now(),
    )
    defaults = {
        column.name: column.default.arg
        for column in LlmProviderTarget.__table__.columns
        if column.default is not None
    }
    assert defaults["timeout_ms"] == 30000
    assert defaults["pre_output_idle_timeout_ms"] == 20000
    assert defaults["pre_output_budget_ms"] == 30000
    assert defaults["post_output_idle_timeout_ms"] == 60000
    assert LlmProviderTarget.__table__.c.max_concurrent_requests.default is None
    assert target.max_concurrent_requests is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_ms", 0),
        ("pre_output_idle_timeout_ms", 999),
        ("pre_output_budget_ms", 20000),
        ("pre_output_budget_ms", 120001),
        ("post_output_idle_timeout_ms", 999),
        ("max_concurrent_requests", 0),
    ],
)
def test_invalid_target_timeout_or_concurrency_is_rejected(db_connection, field, value):
    tenant_id = add_tenant(db_connection, name=f"invalid-target-{field}-{value}")
    provider_id = add_provider(db_connection, tenant_id)
    with pytest.raises(IntegrityError):
        add_target(db_connection, tenant_id, provider_id, **{field: value})


def test_invalid_certification_status_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, name="certification-invalid")
    provider_id = add_provider(db_connection, tenant_id)
    with pytest.raises(DBAPIError):
        add_target(
            db_connection,
            tenant_id,
            provider_id,
            certification_status="EXPERIMENTAL",
        )


@pytest.mark.parametrize(
    ("status", "certification", "override", "eligible"),
    [
        ("ACTIVE", "CERTIFIED", False, True),
        ("ACTIVE", "UNVERIFIED", False, False),
        ("ACTIVE", "UNVERIFIED", True, True),
        ("DISABLED", "CERTIFIED", True, False),
    ],
)
def test_target_runtime_eligibility_foundation(
    db_connection, status, certification, override, eligible
):
    tenant_id = add_tenant(
        db_connection, name=f"eligibility-{status}-{certification}-{override}"
    )
    provider_id = add_provider(db_connection, tenant_id)
    target_id = add_target(
        db_connection,
        tenant_id,
        provider_id,
        status=status,
        certification_status=certification,
        allow_uncertified_runtime=override,
    )
    targets = Base.metadata.tables["llm_provider_targets"]
    eligible_ids = db_connection.execute(
        select(targets.c.id).where(
            targets.c.status == "ACTIVE",
            (targets.c.certification_status == "CERTIFIED")
            | targets.c.allow_uncertified_runtime.is_(True),
        )
    ).scalars()
    assert (target_id in set(eligible_ids)) is eligible


def test_allow_uncertified_runtime_persists(db_connection):
    tenant_id, _, target_id, _ = registry_graph(db_connection, suffix="override")
    targets = Base.metadata.tables["llm_provider_targets"]
    db_connection.execute(
        update(targets)
        .where(targets.c.tenant_id == tenant_id, targets.c.id == target_id)
        .values(allow_uncertified_runtime=True)
    )
    assert (
        db_connection.scalar(
            select(targets.c.allow_uncertified_runtime).where(targets.c.id == target_id)
        )
        is True
    )


def credential_values(tenant_id, target_id, *, status="ACTIVE", ciphertext=b"safe"):
    timestamp = now()
    return {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "provider_target_id": target_id,
        "secret_ciphertext": ciphertext,
        "key_version": "test-key-v1",
        "status": status,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def test_credential_tenant_isolation_and_one_active_rule(db_connection):
    tenant_a, _, target_a, _ = registry_graph(db_connection, suffix="credential-a")
    tenant_b = add_tenant(db_connection, name="credential-tenant-b")
    credentials = Base.metadata.tables["llm_provider_credentials"]
    db_connection.execute(
        insert(credentials).values(**credential_values(tenant_a, target_a))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(credentials).values(**credential_values(tenant_b, target_a))
        )


def test_second_active_credential_is_rejected(db_connection):
    tenant_id, _, target_id, _ = registry_graph(
        db_connection, suffix="credential-active"
    )
    credentials = Base.metadata.tables["llm_provider_credentials"]
    db_connection.execute(
        insert(credentials).values(**credential_values(tenant_id, target_id))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(credentials).values(**credential_values(tenant_id, target_id))
        )


def test_credential_history_rotation_and_cross_target_reuse(db_connection):
    tenant_id = add_tenant(db_connection, name="credential-history")
    provider_id = add_provider(db_connection, tenant_id)
    target_a = add_target(db_connection, tenant_id, provider_id, name="a")
    target_b = add_target(db_connection, tenant_id, provider_id, name="b")
    credentials = Base.metadata.tables["llm_provider_credentials"]
    old_id = uuid4()
    old = credential_values(tenant_id, target_a, status="DISABLED")
    old["id"] = old_id
    db_connection.execute(insert(credentials).values(**old))
    db_connection.execute(
        insert(credentials).values(
            **credential_values(tenant_id, target_a, status="DISABLED")
        )
    )
    db_connection.execute(
        update(credentials).where(credentials.c.id == old_id).values(status="ACTIVE")
    )
    db_connection.execute(
        update(credentials).where(credentials.c.id == old_id).values(status="DISABLED")
    )
    db_connection.execute(
        insert(credentials).values(**credential_values(tenant_id, target_a))
    )
    db_connection.execute(
        insert(credentials).values(**credential_values(tenant_id, target_b))
    )


def test_credential_schema_contains_no_plaintext_secret_column(db_connection):
    names = {
        column["name"]
        for column in inspect(db_connection).get_columns("llm_provider_credentials")
    }
    assert "secret_ciphertext" in names
    assert names.isdisjoint({"secret", "api_key", "token", "plaintext_secret"})


def test_model_identity_is_target_scoped(db_connection):
    tenant_id = add_tenant(db_connection, name="model-identity")
    provider_id = add_provider(db_connection, tenant_id)
    target_a = add_target(db_connection, tenant_id, provider_id, name="a")
    target_b = add_target(db_connection, tenant_id, provider_id, name="b")
    add_model(db_connection, tenant_id, target_a, provider_model_name="same")
    add_model(db_connection, tenant_id, target_b, provider_model_name="same")
    with pytest.raises(IntegrityError):
        add_model(db_connection, tenant_id, target_a, provider_model_name="same")


def test_cross_tenant_model_target_is_rejected(db_connection):
    _, _, target_a, _ = registry_graph(db_connection, suffix="model-cross-a")
    tenant_b = add_tenant(db_connection, name="model-cross-b")
    with pytest.raises(IntegrityError):
        add_model(db_connection, tenant_b, target_a)


@pytest.mark.parametrize("capability", ["CHAT", "STREAMING", "TOOLS", "EMBEDDINGS"])
def test_model_capabilities_are_enforced(db_connection, capability):
    tenant_id, _, _, model_id = registry_graph(
        db_connection, suffix=f"capability-{capability.lower()}"
    )
    capabilities = Base.metadata.tables["llm_model_capabilities"]
    db_connection.execute(
        insert(capabilities).values(
            tenant_id=tenant_id,
            model_id=model_id,
            capability=capability,
            created_at=now(),
        )
    )


def test_duplicate_invalid_and_cross_tenant_capability_are_rejected(db_connection):
    tenant_a, _, _, model_id = registry_graph(db_connection, suffix="capability-a")
    capabilities = Base.metadata.tables["llm_model_capabilities"]
    values = {
        "tenant_id": tenant_a,
        "model_id": model_id,
        "capability": "CHAT",
        "created_at": now(),
    }
    db_connection.execute(insert(capabilities).values(**values))
    with pytest.raises(IntegrityError):
        db_connection.execute(insert(capabilities).values(**values))
    # Cross-tenant behavior is covered separately so a failed statement does not
    # mask the composite FK with PostgreSQL's aborted-transaction state.


def test_cross_tenant_capability_is_rejected(db_connection):
    _, _, _, model_id = registry_graph(db_connection, suffix="cap-cross-a")
    tenant_b = add_tenant(db_connection, name="cap-cross-b")
    capabilities = Base.metadata.tables["llm_model_capabilities"]
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(capabilities).values(
                tenant_id=tenant_b,
                model_id=model_id,
                capability="CHAT",
                created_at=now(),
            )
        )


def test_invalid_capability_enum_is_rejected(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="cap-invalid")
    with pytest.raises(DBAPIError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_model_capabilities"]).values(
                tenant_id=tenant_id,
                model_id=model_id,
                capability="AUDIO",
                created_at=now(),
            )
        )


def add_alias(connection, tenant_id, *, name="public-model"):
    alias_id = uuid4()
    timestamp = now()
    connection.execute(
        insert(Base.metadata.tables["llm_aliases"]).values(
            id=alias_id,
            tenant_id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=timestamp,
            updated_at=timestamp,
        )
    )
    return alias_id


def alias_target_values(tenant_id, alias_id, model_id, *, priority=1):
    return {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "alias_id": alias_id,
        "model_id": model_id,
        "priority": priority,
        "enabled": True,
        "created_at": now(),
    }


def test_alias_name_uniqueness_is_tenant_scoped(db_connection):
    tenant_a = add_tenant(db_connection, name="alias-a")
    tenant_b = add_tenant(db_connection, name="alias-b")
    add_alias(db_connection, tenant_a, name="smart")
    add_alias(db_connection, tenant_b, name="smart")
    with pytest.raises(IntegrityError):
        add_alias(db_connection, tenant_a, name="smart")


def test_alias_target_uniqueness_and_priority(db_connection):
    tenant_id, _, _, model_a = registry_graph(db_connection, suffix="alias-map")
    provider = add_provider(db_connection, tenant_id, name="alias-map-provider-2")
    target = add_target(db_connection, tenant_id, provider, name="alias-map-target-2")
    model_b = add_model(db_connection, tenant_id, target, provider_model_name="other")
    alias_id = add_alias(db_connection, tenant_id)
    mappings = Base.metadata.tables["llm_alias_targets"]
    db_connection.execute(
        insert(mappings).values(**alias_target_values(tenant_id, alias_id, model_a))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(mappings).values(
                **alias_target_values(tenant_id, alias_id, model_b, priority=1)
            )
        )


def test_duplicate_alias_model_mapping_is_rejected(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="alias-model")
    alias_id = add_alias(db_connection, tenant_id)
    mappings = Base.metadata.tables["llm_alias_targets"]
    db_connection.execute(
        insert(mappings).values(**alias_target_values(tenant_id, alias_id, model_id))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(mappings).values(
                **alias_target_values(tenant_id, alias_id, model_id, priority=2)
            )
        )


def test_alias_target_cross_tenant_and_nonpositive_priority_rejected(db_connection):
    tenant_a, _, _, model_a = registry_graph(db_connection, suffix="alias-cross-a")
    tenant_b, _, _, _ = registry_graph(db_connection, suffix="alias-cross-b")
    alias_b = add_alias(db_connection, tenant_b)
    mappings = Base.metadata.tables["llm_alias_targets"]
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(mappings).values(**alias_target_values(tenant_a, alias_b, model_a))
        )


def test_alias_target_nonpositive_priority_rejected(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="alias-priority")
    alias_id = add_alias(db_connection, tenant_id)
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_alias_targets"]).values(
                **alias_target_values(tenant_id, alias_id, model_id, priority=0)
            )
        )


def test_alias_schema_has_no_speculative_columns(db_connection):
    names = {c["name"] for c in inspect(db_connection).get_columns("llm_aliases")}
    assert "required_capabilities" not in names
    assert "gateway_name" not in names


def price_values(model_id, tenant_id, start, end, **overrides):
    values = {
        "id": uuid4(),
        "tenant_id": tenant_id,
        "model_id": model_id,
        "input_price_per_unit": Decimal("0.001"),
        "output_price_per_unit": Decimal("0.002"),
        "unit_tokens": 1000,
        "currency": "USD",
        "effective_from": start,
        "effective_to": end,
        "created_at": now(),
    }
    values.update(overrides)
    return values


def test_same_model_overlap_is_rejected_even_with_different_currency(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="price-overlap")
    prices = Base.metadata.tables["model_prices"]
    start = now()
    db_connection.execute(
        insert(prices).values(
            **price_values(model_id, tenant_id, start, start + timedelta(days=10))
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(prices).values(
                **price_values(
                    model_id,
                    tenant_id,
                    start + timedelta(days=5),
                    start + timedelta(days=12),
                    currency="EUR",
                )
            )
        )


def test_open_ended_price_blocks_later_window(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="price-open")
    prices = Base.metadata.tables["model_prices"]
    start = now()
    db_connection.execute(
        insert(prices).values(**price_values(model_id, tenant_id, start, None))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(prices).values(
                **price_values(model_id, tenant_id, start + timedelta(days=1), None)
            )
        )


def test_adjacent_and_separated_price_windows_are_allowed(db_connection):
    tenant_id, _, _, model_id = registry_graph(db_connection, suffix="price-adjacent")
    prices = Base.metadata.tables["model_prices"]
    start = now()
    end = start + timedelta(days=10)
    db_connection.execute(
        insert(prices).values(**price_values(model_id, tenant_id, start, end))
    )
    db_connection.execute(
        insert(prices).values(
            **price_values(model_id, tenant_id, end, end + timedelta(days=5))
        )
    )
    db_connection.execute(
        insert(prices).values(
            **price_values(
                model_id,
                tenant_id,
                end + timedelta(days=10),
                end + timedelta(days=15),
            )
        )
    )


def test_identical_price_windows_on_different_models_are_allowed(db_connection):
    tenant_id = add_tenant(db_connection, name="price-models")
    provider_id = add_provider(db_connection, tenant_id)
    target_id = add_target(db_connection, tenant_id, provider_id)
    model_a = add_model(db_connection, tenant_id, target_id, provider_model_name="a")
    model_b = add_model(db_connection, tenant_id, target_id, provider_model_name="b")
    prices = Base.metadata.tables["model_prices"]
    start, end = now(), now() + timedelta(days=1)
    db_connection.execute(
        insert(prices),
        [
            price_values(model_a, tenant_id, start, end),
            price_values(model_b, tenant_id, start, end),
        ],
    )


@pytest.mark.parametrize(
    ("overrides", "zero_length"),
    [
        ({"input_price_per_unit": Decimal("-0.1")}, False),
        ({"output_price_per_unit": Decimal("-0.1")}, False),
        ({"unit_tokens": 0}, False),
        ({"currency": "usd"}, False),
        ({"currency": "US"}, False),
        ({}, True),
    ],
)
def test_invalid_model_price_is_rejected(db_connection, overrides, zero_length):
    tenant_id, _, _, model_id = registry_graph(
        db_connection, suffix=f"price-invalid-{uuid4()}"
    )
    timestamp = now()
    end = timestamp if zero_length else timestamp + timedelta(days=1)
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["model_prices"]).values(
                **price_values(model_id, tenant_id, timestamp, end, **overrides)
            )
        )
