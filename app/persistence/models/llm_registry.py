from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import CHAR, JSONB, ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.persistence.models.enums import (
    CertificationStatus,
    ModelCapability,
    ProviderType,
    ResourceStatus,
)


class LlmProvider(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "llm_providers"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    provider_type: Mapped[ProviderType] = mapped_column(
        Enum(ProviderType, name="provider_type"), nullable=False
    )
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        UniqueConstraint("tenant_id", "id", name="uq_llm_providers_tenant_id"),
        UniqueConstraint("tenant_id", "name", name="uq_llm_providers_tenant_name"),
        Index("ix_llm_providers_tenant_status", "tenant_id", "status"),
        Index("ix_llm_providers_tenant_type", "tenant_id", "provider_type"),
    )


class LlmProviderTarget(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "llm_provider_targets"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    provider_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=30000)
    pre_output_idle_timeout_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=20000
    )
    pre_output_budget_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=30000
    )
    post_output_idle_timeout_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=60000
    )
    max_concurrent_requests: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )
    certification_status: Mapped[CertificationStatus] = mapped_column(
        Enum(CertificationStatus, name="certification_status"),
        nullable=False,
        default=CertificationStatus.UNVERIFIED,
        server_default=CertificationStatus.UNVERIFIED.value,
    )
    allow_uncertified_runtime: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    certified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    certification_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "provider_id"],
            ["llm_providers.tenant_id", "llm_providers.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_llm_provider_targets_tenant_id"),
        UniqueConstraint(
            "tenant_id",
            "provider_id",
            "name",
            name="uq_llm_provider_targets_provider_name",
        ),
        CheckConstraint("timeout_ms > 0", name="llm_target_timeout_positive"),
        CheckConstraint(
            "pre_output_idle_timeout_ms >= 1000",
            name="llm_target_pre_output_idle_minimum",
        ),
        CheckConstraint(
            "pre_output_budget_ms > pre_output_idle_timeout_ms",
            name="llm_target_pre_output_budget_after_idle",
        ),
        CheckConstraint(
            "pre_output_budget_ms <= 120000",
            name="llm_target_pre_output_budget_maximum",
        ),
        CheckConstraint(
            "post_output_idle_timeout_ms >= 1000",
            name="llm_target_post_output_idle_minimum",
        ),
        CheckConstraint(
            "max_concurrent_requests IS NULL OR max_concurrent_requests > 0",
            name="llm_target_concurrency_positive",
        ),
        Index(
            "ix_llm_targets_runtime_eligibility",
            "tenant_id",
            "status",
            "certification_status",
            "allow_uncertified_runtime",
        ),
        Index(
            "ix_llm_targets_tenant_provider_status",
            "tenant_id",
            "provider_id",
            "status",
        ),
    )


class LlmProviderCredential(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "llm_provider_credentials"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    provider_target_id: Mapped[UUID] = mapped_column(nullable=False)
    secret_ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    key_version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"), nullable=False
    )
    rotated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            ondelete="RESTRICT",
        ),
        Index(
            "uq_llm_provider_credentials_one_active",
            "tenant_id",
            "provider_target_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
        Index(
            "ix_llm_provider_credentials_target_created",
            "tenant_id",
            "provider_target_id",
            "created_at",
        ),
    )


class LlmModel(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "llm_models"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    provider_target_id: Mapped[UUID] = mapped_column(nullable=False)
    provider_model_name: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_llm_models_tenant_id"),
        UniqueConstraint(
            "tenant_id",
            "provider_target_id",
            "id",
            name="uq_llm_models_tenant_target_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "provider_target_id",
            "provider_model_name",
            name="uq_llm_models_target_provider_name",
        ),
        Index(
            "ix_llm_models_tenant_target_status",
            "tenant_id",
            "provider_target_id",
            "status",
        ),
        Index("ix_llm_models_tenant_status", "tenant_id", "status"),
    )


class LlmModelCapability(Base):
    __tablename__ = "llm_model_capabilities"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    model_id: Mapped[UUID] = mapped_column(primary_key=True)
    capability: Mapped[ModelCapability] = mapped_column(
        Enum(ModelCapability, name="model_capability"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            ondelete="CASCADE",
        ),
    )


class LlmAlias(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "llm_aliases"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        UniqueConstraint("tenant_id", "id", name="uq_llm_aliases_tenant_id"),
        UniqueConstraint("tenant_id", "name", name="uq_llm_aliases_tenant_name"),
        Index("ix_llm_aliases_tenant_status", "tenant_id", "status"),
    )


class LlmAliasTarget(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_alias_targets"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    alias_id: Mapped[UUID] = mapped_column(nullable=False)
    model_id: Mapped[UUID] = mapped_column(nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "alias_id"],
            ["llm_aliases.tenant_id", "llm_aliases.id"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "alias_id",
            "priority",
            name="uq_llm_alias_targets_alias_priority",
        ),
        UniqueConstraint(
            "tenant_id",
            "alias_id",
            "model_id",
            name="uq_llm_alias_targets_alias_model",
        ),
        CheckConstraint("priority > 0", name="llm_alias_target_priority_positive"),
        Index(
            "ix_llm_alias_targets_resolution",
            "tenant_id",
            "alias_id",
            "enabled",
            "priority",
        ),
    )


class ModelPrice(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "model_prices"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    model_id: Mapped[UUID] = mapped_column(nullable=False)
    input_price_per_unit: Mapped[Decimal] = mapped_column(
        Numeric(20, 10), nullable=False
    )
    output_price_per_unit: Mapped[Decimal] = mapped_column(
        Numeric(20, 10), nullable=False
    )
    unit_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    effective_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    effective_to: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "input_price_per_unit >= 0", name="model_price_input_nonnegative"
        ),
        CheckConstraint(
            "output_price_per_unit >= 0", name="model_price_output_nonnegative"
        ),
        CheckConstraint("unit_tokens > 0", name="model_price_unit_tokens_positive"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="model_price_currency_format"),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="model_price_effective_window_valid",
        ),
        ExcludeConstraint(
            ("model_id", "="),
            (func.tstzrange(effective_from, effective_to, "[)"), "&&"),
            name="ex_model_prices_model_effective_window",
            using="gist",
        ),
        Index(
            "ix_model_prices_model_effective_from",
            "model_id",
            effective_from.desc(),
        ),
    )
