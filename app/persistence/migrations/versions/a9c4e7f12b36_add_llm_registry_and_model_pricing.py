"""add llm registry and model pricing

Revision ID: a9c4e7f12b36
Revises: d1e0310898aa
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a9c4e7f12b36"
down_revision: str | Sequence[str] | None = "d1e0310898aa"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

resource_status_enum = postgresql.ENUM(
    "ACTIVE", "DISABLED", name="resource_status", create_type=False
)
provider_type_enum = postgresql.ENUM(
    "OPENAI",
    "ANTHROPIC",
    "VLLM",
    "GENERIC_OPENAI_COMPAT",
    name="provider_type",
    create_type=False,
)
certification_status_enum = postgresql.ENUM(
    "UNVERIFIED",
    "CERTIFIED",
    "FAILED",
    name="certification_status",
    create_type=False,
)
model_capability_enum = postgresql.ENUM(
    "CHAT",
    "STREAMING",
    "TOOLS",
    "EMBEDDINGS",
    name="model_capability",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    provider_type_enum.create(bind, checkfirst=True)
    certification_status_enum.create(bind, checkfirst=True)
    model_capability_enum.create(bind, checkfirst=True)

    op.create_table(
        "llm_providers",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("provider_type", provider_type_enum, nullable=False),
        sa.Column("status", resource_status_enum, nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_llm_providers_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_providers")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_llm_providers_tenant_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_llm_providers_tenant_name"),
    )
    op.create_index(
        "ix_llm_providers_tenant_status",
        "llm_providers",
        ["tenant_id", "status"],
    )
    op.create_index(
        "ix_llm_providers_tenant_type",
        "llm_providers",
        ["tenant_id", "provider_type"],
    )

    op.create_table(
        "llm_provider_targets",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("base_url", sa.String(length=2048), nullable=False),
        sa.Column("timeout_ms", sa.Integer(), nullable=False),
        sa.Column("pre_output_idle_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("pre_output_budget_ms", sa.Integer(), nullable=False),
        sa.Column("post_output_idle_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("max_concurrent_requests", sa.Integer(), nullable=True),
        sa.Column("status", resource_status_enum, nullable=False),
        sa.Column(
            "certification_status",
            certification_status_enum,
            server_default="UNVERIFIED",
            nullable=False,
        ),
        sa.Column(
            "allow_uncertified_runtime",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("certified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "certification_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "timeout_ms > 0",
            name=op.f("ck_llm_provider_targets_llm_target_timeout_positive"),
        ),
        sa.CheckConstraint(
            "pre_output_idle_timeout_ms >= 1000",
            name=op.f("ck_llm_provider_targets_llm_target_pre_output_idle_minimum"),
        ),
        sa.CheckConstraint(
            "pre_output_budget_ms > pre_output_idle_timeout_ms",
            name=op.f(
                "ck_llm_provider_targets_llm_target_pre_output_budget_after_idle"
            ),
        ),
        sa.CheckConstraint(
            "pre_output_budget_ms <= 120000",
            name=op.f("ck_llm_provider_targets_llm_target_pre_output_budget_maximum"),
        ),
        sa.CheckConstraint(
            "post_output_idle_timeout_ms >= 1000",
            name=op.f("ck_llm_provider_targets_llm_target_post_output_idle_minimum"),
        ),
        sa.CheckConstraint(
            "max_concurrent_requests IS NULL OR max_concurrent_requests > 0",
            name=op.f("ck_llm_provider_targets_llm_target_concurrency_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider_id"],
            ["llm_providers.tenant_id", "llm_providers.id"],
            name=op.f("fk_llm_provider_targets_tenant_id_llm_providers"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_provider_targets")),
        sa.UniqueConstraint(
            "tenant_id", "id", name="uq_llm_provider_targets_tenant_id"
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "provider_id",
            "name",
            name="uq_llm_provider_targets_provider_name",
        ),
    )
    op.create_index(
        "ix_llm_targets_runtime_eligibility",
        "llm_provider_targets",
        [
            "tenant_id",
            "status",
            "certification_status",
            "allow_uncertified_runtime",
        ],
    )
    op.create_index(
        "ix_llm_targets_tenant_provider_status",
        "llm_provider_targets",
        ["tenant_id", "provider_id", "status"],
    )

    op.create_table(
        "llm_provider_credentials",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("provider_target_id", sa.Uuid(), nullable=False),
        sa.Column("secret_ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("key_version", sa.String(length=64), nullable=False),
        sa.Column("status", resource_status_enum, nullable=False),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            name=op.f("fk_llm_provider_credentials_tenant_id_llm_provider_targets"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_provider_credentials")),
    )
    op.create_index(
        "ix_llm_provider_credentials_target_created",
        "llm_provider_credentials",
        ["tenant_id", "provider_target_id", "created_at"],
    )
    op.create_index(
        "uq_llm_provider_credentials_one_active",
        "llm_provider_credentials",
        ["tenant_id", "provider_target_id"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )

    op.create_table(
        "llm_models",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("provider_target_id", sa.Uuid(), nullable=False),
        sa.Column("provider_model_name", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=200), nullable=True),
        sa.Column("status", resource_status_enum, nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            name=op.f("fk_llm_models_tenant_id_llm_provider_targets"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_models")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_llm_models_tenant_id"),
        sa.UniqueConstraint(
            "tenant_id",
            "provider_target_id",
            "provider_model_name",
            name="uq_llm_models_target_provider_name",
        ),
    )
    op.create_index(
        "ix_llm_models_tenant_status",
        "llm_models",
        ["tenant_id", "status"],
    )
    op.create_index(
        "ix_llm_models_tenant_target_status",
        "llm_models",
        ["tenant_id", "provider_target_id", "status"],
    )

    op.create_table(
        "llm_model_capabilities",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.Uuid(), nullable=False),
        sa.Column("capability", model_capability_enum, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            name=op.f("fk_llm_model_capabilities_tenant_id_llm_models"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "model_id", "capability", name=op.f("pk_llm_model_capabilities")
        ),
    )

    op.create_table(
        "llm_aliases",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("status", resource_status_enum, nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_llm_aliases_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_aliases")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_llm_aliases_tenant_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_llm_aliases_tenant_name"),
    )
    op.create_index(
        "ix_llm_aliases_tenant_status", "llm_aliases", ["tenant_id", "status"]
    )

    op.create_table(
        "llm_alias_targets",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("alias_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.Uuid(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "priority > 0",
            name=op.f("ck_llm_alias_targets_llm_alias_target_priority_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "alias_id"],
            ["llm_aliases.tenant_id", "llm_aliases.id"],
            name=op.f("fk_llm_alias_targets_tenant_id_llm_aliases"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            name=op.f("fk_llm_alias_targets_tenant_id_llm_models"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_alias_targets")),
        sa.UniqueConstraint(
            "tenant_id",
            "alias_id",
            "model_id",
            name="uq_llm_alias_targets_alias_model",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "alias_id",
            "priority",
            name="uq_llm_alias_targets_alias_priority",
        ),
    )
    op.create_index(
        "ix_llm_alias_targets_resolution",
        "llm_alias_targets",
        ["tenant_id", "alias_id", "enabled", "priority"],
    )

    op.create_table(
        "model_prices",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.Uuid(), nullable=False),
        sa.Column("input_price_per_unit", sa.Numeric(20, 10), nullable=False),
        sa.Column("output_price_per_unit", sa.Numeric(20, 10), nullable=False),
        sa.Column("unit_tokens", sa.Integer(), nullable=False),
        sa.Column("currency", postgresql.CHAR(length=3), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "input_price_per_unit >= 0",
            name=op.f("ck_model_prices_model_price_input_nonnegative"),
        ),
        sa.CheckConstraint(
            "output_price_per_unit >= 0",
            name=op.f("ck_model_prices_model_price_output_nonnegative"),
        ),
        sa.CheckConstraint(
            "unit_tokens > 0",
            name=op.f("ck_model_prices_model_price_unit_tokens_positive"),
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_model_prices_model_price_currency_format"),
        ),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name=op.f("ck_model_prices_model_price_effective_window_valid"),
        ),
        postgresql.ExcludeConstraint(
            ("model_id", "="),
            (
                sa.func.tstzrange(
                    sa.column("effective_from"),
                    sa.column("effective_to"),
                    "[)",
                ),
                "&&",
            ),
            name="ex_model_prices_model_effective_window",
            using="gist",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            name=op.f("fk_model_prices_tenant_id_llm_models"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_prices")),
    )
    op.create_index(
        "ix_model_prices_model_effective_from",
        "model_prices",
        ["model_id", sa.text("effective_from DESC")],
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_index("ix_model_prices_model_effective_from", table_name="model_prices")
    op.drop_table("model_prices")
    op.drop_index("ix_llm_alias_targets_resolution", table_name="llm_alias_targets")
    op.drop_table("llm_alias_targets")
    op.drop_index("ix_llm_aliases_tenant_status", table_name="llm_aliases")
    op.drop_table("llm_aliases")
    op.drop_table("llm_model_capabilities")
    op.drop_index("ix_llm_models_tenant_target_status", table_name="llm_models")
    op.drop_index("ix_llm_models_tenant_status", table_name="llm_models")
    op.drop_table("llm_models")
    op.drop_index(
        "uq_llm_provider_credentials_one_active",
        table_name="llm_provider_credentials",
    )
    op.drop_index(
        "ix_llm_provider_credentials_target_created",
        table_name="llm_provider_credentials",
    )
    op.drop_table("llm_provider_credentials")
    op.drop_index(
        "ix_llm_targets_tenant_provider_status", table_name="llm_provider_targets"
    )
    op.drop_index(
        "ix_llm_targets_runtime_eligibility", table_name="llm_provider_targets"
    )
    op.drop_table("llm_provider_targets")
    op.drop_index("ix_llm_providers_tenant_type", table_name="llm_providers")
    op.drop_index("ix_llm_providers_tenant_status", table_name="llm_providers")
    op.drop_table("llm_providers")

    model_capability_enum.drop(bind, checkfirst=True)
    certification_status_enum.drop(bind, checkfirst=True)
    provider_type_enum.drop(bind, checkfirst=True)
