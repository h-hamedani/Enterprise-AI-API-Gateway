from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, insert, select

from app.control_plane.llm_registry import (
    LlmRegistryConflictError,
    LlmRegistryNotFoundError,
    create_llm_registry_service,
)
from app.core.config import get_settings
from app.persistence.models import Base
from app.schemas.control_plane import (
    AliasTargetWrite,
    LlmAliasCreate,
    LlmModelCreate,
    LlmModelPatch,
    LlmProviderCreate,
    LlmTargetCreate,
    ModelPriceCreate,
    ModelPricePatch,
)


def test_registry_relationships_replacements_pricing_and_tenant_isolation():
    engine = create_engine(get_settings().postgres_migration_dsn)
    connection = engine.connect()
    transaction = connection.begin()
    now = datetime.now(UTC)
    tenant_a, tenant_b = uuid4(), uuid4()
    connection.execute(
        insert(Base.metadata.tables["tenants"]),
        [
            {
                "id": t,
                "name": f"m27-{t}",
                "status": "ACTIVE",
                "created_at": now,
                "updated_at": now,
            }
            for t in (tenant_a, tenant_b)
        ],
    )
    service = create_llm_registry_service(b"I" * 32)
    try:
        provider = service.create_provider(
            connection,
            tenant_a,
            LlmProviderCreate(name="openai", provider_type="OPENAI"),
        )
        target = service.create_target(
            connection,
            tenant_a,
            LlmTargetCreate(
                provider_id=provider.id,
                name="primary",
                base_url="https://provider.example",
            ),
        )
        model = service.create_model(
            connection,
            tenant_a,
            LlmModelCreate(
                provider_target_id=target.id, provider_model_name="gpt-main"
            ),
        )
        service.replace_capabilities(
            connection, tenant_a, model.id, ["CHAT", "STREAMING"]
        )
        assert service.get_model(connection, tenant_a, model.id).capabilities == [
            "CHAT",
            "STREAMING",
        ]
        alias = service.create_alias(
            connection, tenant_a, LlmAliasCreate(name="assistant")
        )
        service.replace_alias_targets(
            connection,
            tenant_a,
            alias.id,
            [AliasTargetWrite(model_id=model.id, priority=1)],
        )
        assert (
            service.get_alias(connection, tenant_a, alias.id).targets[0].model_id
            == model.id
        )
        with pytest.raises(LlmRegistryNotFoundError):
            service.get_model(connection, tenant_b, model.id)
        assert (
            service.patch_model(
                connection, tenant_a, model.id, LlmModelPatch(display_name="Main")
            ).id
            == model.id
        )

        first = service.create_price(
            connection,
            tenant_a,
            model.id,
            ModelPriceCreate(
                input_price_per_unit=Decimal("1.25"),
                output_price_per_unit=Decimal("2.50"),
                unit_tokens=1000,
                currency="EUR",
                effective_from=now,
                effective_to=now + timedelta(days=1),
            ),
        )
        second = service.create_price(
            connection,
            tenant_a,
            model.id,
            ModelPriceCreate(
                input_price_per_unit=Decimal(0),
                output_price_per_unit=Decimal(3),
                unit_tokens=7,
                currency="JPY",
                effective_from=now + timedelta(days=1),
                effective_to=None,
            ),
        )
        assert first.currency == "EUR" and second.unit_tokens == 7
        with pytest.raises(LlmRegistryConflictError):
            service.patch_price(
                connection,
                tenant_a,
                second.id,
                ModelPricePatch(effective_from=now + timedelta(hours=12)),
            )
        rows = (
            connection.execute(
                select(Base.metadata.tables["model_prices"]).where(
                    Base.metadata.tables["model_prices"].c.model_id == model.id
                )
            )
            .mappings()
            .all()
        )
        assert len(rows) == 2 and {row["currency"].strip() for row in rows} == {
            "EUR",
            "JPY",
        }
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()
