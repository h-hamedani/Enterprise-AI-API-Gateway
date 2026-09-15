"""add rate limits telemetry audit and retention

Revision ID: c7d8e9f0a1b2
Revises: a9c4e7f12b36
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c7d8e9f0a1b2"
down_revision: str | Sequence[str] | None = "a9c4e7f12b36"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

rate_scope_type = postgresql.ENUM(
    "API_KEY",
    "ROUTE",
    "SERVICE",
    "LLM_ALIAS",
    "LLM_MODEL",
    "PROVIDER_TARGET",
    "ADMIN_TOKEN",
    name="rate_scope_type",
    create_type=False,
)
workload_type = postgresql.ENUM(
    "NORMAL", "LLM", name="workload_type", create_type=False
)
llm_final_status = postgresql.ENUM(
    "SUCCESS",
    "FAILED",
    "CLIENT_CANCELLED",
    name="llm_final_status",
    create_type=False,
)
attempt_status = postgresql.ENUM(
    "SUCCESS", "FAILED", "CANCELLED", name="attempt_status", create_type=False
)
audit_result = postgresql.ENUM(
    "SUCCESS", "FAILED", name="audit_result", create_type=False
)
retention_state = postgresql.ENUM(
    "STARTED",
    "ROLLED_UP",
    "VERIFIED",
    "PURGING",
    "COMPLETED",
    "FAILED",
    name="retention_state",
    create_type=False,
)
retention_job_type = postgresql.ENUM(
    "API_RETENTION", "LLM_RETENTION", name="retention_job_type", create_type=False
)


def upgrade() -> None:
    bind = op.get_bind()
    for enum in (
        rate_scope_type,
        workload_type,
        llm_final_status,
        attempt_status,
        audit_result,
        retention_state,
        retention_job_type,
    ):
        enum.create(bind, checkfirst=True)

    op.create_unique_constraint(
        "uq_api_keys_tenant_id", "api_keys", ["tenant_id", "id"]
    )
    op.create_unique_constraint(
        "uq_admin_tokens_tenant_id", "admin_tokens", ["tenant_id", "id"]
    )
    op.create_unique_constraint(
        "uq_normal_api_routes_tenant_id", "normal_api_routes", ["tenant_id", "id"]
    )

    op.create_table(
        "rate_limit_policies",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("scope_type", rate_scope_type, nullable=False),
        sa.Column("scope_id", sa.Uuid(), nullable=False),
        sa.Column("requests_per_window", sa.Integer(), nullable=True),
        sa.Column("window_seconds", sa.Integer(), nullable=True),
        sa.Column("max_concurrency", sa.Integer(), nullable=True),
        sa.Column("degraded_factor", sa.Numeric(5, 4), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.CheckConstraint(
            "requests_per_window IS NULL OR requests_per_window > 0",
            name=op.f("ck_rate_limit_policies_rate_policy_requests_positive"),
        ),
        sa.CheckConstraint(
            "window_seconds IS NULL OR window_seconds > 0",
            name=op.f("ck_rate_limit_policies_rate_policy_window_positive"),
        ),
        sa.CheckConstraint(
            "max_concurrency IS NULL OR max_concurrency > 0",
            name=op.f("ck_rate_limit_policies_rate_policy_concurrency_positive"),
        ),
        sa.CheckConstraint(
            "degraded_factor > 0 AND degraded_factor <= 1",
            name=op.f("ck_rate_limit_policies_rate_policy_degraded_factor_range"),
        ),
        sa.CheckConstraint(
            "(requests_per_window IS NULL) = (window_seconds IS NULL)",
            name=op.f("ck_rate_limit_policies_rate_policy_request_window_paired"),
        ),
        sa.CheckConstraint(
            "(requests_per_window IS NOT NULL AND window_seconds IS NOT NULL) "
            "OR max_concurrency IS NOT NULL",
            name=op.f("ck_rate_limit_policies_rate_policy_meaningful"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_rate_limit_policies_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rate_limit_policies")),
    )
    op.create_index(
        "ix_rate_limit_policies_scope",
        "rate_limit_policies",
        ["tenant_id", "scope_type", "scope_id"],
    )
    op.create_index(
        "uq_rate_limit_policy_one_enabled",
        "rate_limit_policies",
        ["tenant_id", "scope_type", "scope_id"],
        unique=True,
        postgresql_where=sa.text("enabled = true"),
    )

    op.create_table(
        "requests",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=True),
        sa.Column("api_key_id", sa.Uuid(), nullable=True),
        sa.Column("workload_type", workload_type, nullable=False),
        sa.Column("route_id", sa.Uuid(), nullable=True),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("degraded_mode", sa.Boolean(), nullable=False),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name=op.f("ck_requests_request_latency_nonnegative"),
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name=op.f("ck_requests_request_completion_after_start"),
        ),
        sa.CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name=op.f("ck_requests_request_status_code_valid"),
        ),
        sa.CheckConstraint(
            "(workload_type = 'NORMAL' AND route_id IS NOT NULL) OR "
            "(workload_type = 'LLM' AND route_id IS NULL)",
            name=op.f("ck_requests_request_workload_attribution"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_requests_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "application_id"],
            ["applications.tenant_id", "applications.id"],
            name=op.f("fk_requests_tenant_id_applications"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.id"],
            name=op.f("fk_requests_tenant_id_api_keys"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "route_id"],
            ["normal_api_routes.tenant_id", "normal_api_routes.id"],
            name=op.f("fk_requests_tenant_id_normal_api_routes"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_requests")),
        sa.UniqueConstraint("request_id", name=op.f("uq_requests_request_id")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_requests_tenant_id"),
    )
    op.create_index("ix_requests_api_key_id", "requests", ["api_key_id"])
    op.create_index("ix_requests_route_id", "requests", ["route_id"])
    op.create_index(
        "ix_requests_tenant_started_at", "requests", ["tenant_id", "started_at"]
    )
    op.create_index(
        "ix_requests_tenant_workload_started",
        "requests",
        ["tenant_id", "workload_type", "started_at"],
    )

    op.create_table(
        "llm_requests",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("request_id_fk", sa.Uuid(), nullable=False),
        sa.Column("requested_model", sa.String(160), nullable=False),
        sa.Column("resolved_alias_id", sa.Uuid(), nullable=True),
        sa.Column("final_model_id", sa.Uuid(), nullable=True),
        sa.Column("stream", sa.Boolean(), nullable=False),
        sa.Column("tool_calling", sa.Boolean(), nullable=False),
        sa.Column("input_tokens_total", sa.BigInteger(), nullable=True),
        sa.Column("output_tokens_total", sa.BigInteger(), nullable=True),
        sa.Column("known_cost_total", sa.Numeric(20, 8), nullable=True),
        sa.Column("ttft_ms", sa.Integer(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("final_status", llm_final_status, nullable=True),
        sa.Column("cost_complete", sa.Boolean(), nullable=False),
        sa.Column("output_committed", sa.Boolean(), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=op.f("ck_llm_requests_llm_request_attempt_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "input_tokens_total IS NULL OR input_tokens_total >= 0",
            name=op.f("ck_llm_requests_llm_request_input_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "output_tokens_total IS NULL OR output_tokens_total >= 0",
            name=op.f("ck_llm_requests_llm_request_output_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "known_cost_total IS NULL OR known_cost_total >= 0",
            name=op.f("ck_llm_requests_llm_request_known_cost_nonnegative"),
        ),
        sa.CheckConstraint(
            "ttft_ms IS NULL OR ttft_ms >= 0",
            name=op.f("ck_llm_requests_llm_request_ttft_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "request_id_fk"],
            ["requests.tenant_id", "requests.id"],
            name=op.f("fk_llm_requests_tenant_id_requests"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "resolved_alias_id"],
            ["llm_aliases.tenant_id", "llm_aliases.id"],
            name=op.f("fk_llm_requests_tenant_id_llm_aliases"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "final_model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            name=op.f("fk_llm_requests_tenant_id_llm_models"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_requests")),
        sa.UniqueConstraint(
            "request_id_fk", name=op.f("uq_llm_requests_request_id_fk")
        ),
        sa.UniqueConstraint("tenant_id", "id", name="uq_llm_requests_tenant_id"),
    )
    op.create_index(
        "ix_llm_requests_tenant_alias",
        "llm_requests",
        ["tenant_id", "resolved_alias_id"],
    )
    op.create_index(
        "ix_llm_requests_tenant_final_model",
        "llm_requests",
        ["tenant_id", "final_model_id"],
    )

    op.create_table(
        "llm_attempts",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("llm_request_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("provider_target_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", attempt_status, nullable=False),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("input_tokens", sa.BigInteger(), nullable=True),
        sa.Column("output_tokens", sa.BigInteger(), nullable=True),
        sa.Column("known_cost", sa.Numeric(20, 8), nullable=True),
        sa.Column("first_output_committed", sa.Boolean(), nullable=False),
        sa.Column("provider_midstream_failure", sa.Boolean(), nullable=False),
        sa.Column("provider_ttft_ms", sa.Integer(), nullable=True),
        sa.Column("retry_reason", sa.String(64), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.CheckConstraint(
            "attempt_no > 0", name=op.f("ck_llm_attempts_llm_attempt_number_positive")
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name=op.f("ck_llm_attempts_llm_attempt_input_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name=op.f("ck_llm_attempts_llm_attempt_output_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "known_cost IS NULL OR known_cost >= 0",
            name=op.f("ck_llm_attempts_llm_attempt_known_cost_nonnegative"),
        ),
        sa.CheckConstraint(
            "provider_ttft_ms IS NULL OR provider_ttft_ms >= 0",
            name=op.f("ck_llm_attempts_llm_attempt_provider_ttft_nonnegative"),
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name=op.f("ck_llm_attempts_llm_attempt_completion_after_start"),
        ),
        sa.CheckConstraint(
            "provider_midstream_failure = false OR status = 'FAILED'",
            name=op.f("ck_llm_attempts_llm_attempt_midstream_failure_status"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "llm_request_id"],
            ["llm_requests.tenant_id", "llm_requests.id"],
            name=op.f("fk_llm_attempts_tenant_id_llm_requests"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            name=op.f("fk_llm_attempts_tenant_id_llm_provider_targets"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            name=op.f("fk_llm_attempts_tenant_id_llm_models"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_attempts")),
        sa.UniqueConstraint(
            "llm_request_id", "attempt_no", name="uq_llm_attempts_request_attempt"
        ),
    )
    op.create_index(
        "ix_llm_attempts_target_model_started",
        "llm_attempts",
        ["provider_target_id", "model_id", "started_at"],
    )
    op.create_index(
        "ix_llm_attempts_tenant_request_attempt",
        "llm_attempts",
        ["tenant_id", "llm_request_id", "attempt_no"],
    )
    op.create_index(
        "ix_llm_attempts_tenant_started_at", "llm_attempts", ["tenant_id", "started_at"]
    )

    op.create_table(
        "audit_logs",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("actor_admin_user_id", sa.Uuid(), nullable=True),
        sa.Column("actor_admin_token_id", sa.Uuid(), nullable=True),
        sa.Column("request_id", sa.Uuid(), nullable=True),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=True),
        sa.Column("result", audit_result, nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_audit_logs_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "actor_admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            name=op.f("fk_audit_logs_tenant_id_admin_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "actor_admin_token_id"],
            ["admin_tokens.tenant_id", "admin_tokens.id"],
            name=op.f("fk_audit_logs_tenant_id_admin_tokens"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_logs")),
    )
    op.create_index("ix_audit_logs_actor_token", "audit_logs", ["actor_admin_token_id"])
    op.create_index("ix_audit_logs_actor_user", "audit_logs", ["actor_admin_user_id"])
    op.create_index("ix_audit_logs_request_id", "audit_logs", ["request_id"])
    op.create_index(
        "ix_audit_logs_tenant_created_id",
        "audit_logs",
        ["tenant_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_audit_logs_tenant_resource",
        "audit_logs",
        ["tenant_id", "resource_type", "resource_id"],
    )

    op.create_table(
        "config_versions",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by_admin_user_id", sa.Uuid(), nullable=True),
        sa.CheckConstraint(
            "version >= 0", name=op.f("ck_config_versions_config_version_nonnegative")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_config_versions_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "updated_by_admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            name=op.f("fk_config_versions_tenant_id_admin_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tenant_id", name=op.f("pk_config_versions")),
    )

    op.create_table(
        "retention_checkpoints",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("job_type", retention_job_type, nullable=False),
        sa.Column("scope_date", sa.Date(), nullable=True),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", retention_state, nullable=False),
        sa.Column("source_row_count", sa.BigInteger(), nullable=True),
        sa.Column("aggregate_row_count", sa.BigInteger(), nullable=True),
        sa.Column("last_purged_id", sa.Uuid(), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.CheckConstraint(
            "source_row_count IS NULL OR source_row_count >= 0",
            name=op.f("ck_retention_checkpoints_retention_source_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "aggregate_row_count IS NULL OR aggregate_row_count >= 0",
            name=op.f("ck_retention_checkpoints_retention_aggregate_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR state = 'COMPLETED'",
            name=op.f("ck_retention_checkpoints_retention_completed_state"),
        ),
        sa.CheckConstraint(
            "updated_at >= started_at",
            name=op.f("ck_retention_checkpoints_retention_update_after_start"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_retention_checkpoints_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_retention_checkpoints")),
        sa.UniqueConstraint(
            "tenant_id",
            "job_type",
            "scope_date",
            "cutoff_at",
            name="uq_retention_checkpoint_scope",
            postgresql_nulls_not_distinct=True,
        ),
    )
    op.create_index(
        "ix_retention_checkpoints_cutoff",
        "retention_checkpoints",
        ["tenant_id", "job_type", "cutoff_at"],
    )
    op.create_index(
        "ix_retention_checkpoints_resume",
        "retention_checkpoints",
        ["tenant_id", "job_type", "state", "updated_at"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_table("retention_checkpoints")
    op.drop_table("config_versions")
    op.drop_table("audit_logs")
    op.drop_table("llm_attempts")
    op.drop_table("llm_requests")
    op.drop_table("requests")
    op.drop_table("rate_limit_policies")
    op.drop_constraint(
        "uq_normal_api_routes_tenant_id", "normal_api_routes", type_="unique"
    )
    op.drop_constraint("uq_admin_tokens_tenant_id", "admin_tokens", type_="unique")
    op.drop_constraint("uq_api_keys_tenant_id", "api_keys", type_="unique")

    for enum in (
        retention_job_type,
        retention_state,
        audit_result,
        attempt_status,
        llm_final_status,
        workload_type,
        rate_scope_type,
    ):
        enum.drop(bind, checkfirst=True)
