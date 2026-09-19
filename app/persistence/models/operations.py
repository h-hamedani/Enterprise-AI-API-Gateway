from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import Base, UUIDPrimaryKeyMixin
from app.persistence.models.enums import (
    AttemptStatus,
    AuditResult,
    LlmFinalStatus,
    RateScopeType,
    RetentionJobType,
    RetentionState,
    WorkloadType,
)


class RateLimitPolicy(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "rate_limit_policies"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    scope_type: Mapped[RateScopeType] = mapped_column(
        Enum(RateScopeType, name="rate_scope_type"), nullable=False
    )
    scope_id: Mapped[UUID] = mapped_column(nullable=False)
    requests_per_window: Mapped[int | None] = mapped_column(Integer, nullable=True)
    window_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_concurrency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    degraded_factor: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), nullable=False, default=Decimal("0.25")
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        CheckConstraint(
            "requests_per_window IS NULL OR requests_per_window > 0",
            name="rate_policy_requests_positive",
        ),
        CheckConstraint(
            "window_seconds IS NULL OR window_seconds > 0",
            name="rate_policy_window_positive",
        ),
        CheckConstraint(
            "max_concurrency IS NULL OR max_concurrency > 0",
            name="rate_policy_concurrency_positive",
        ),
        CheckConstraint(
            "degraded_factor > 0 AND degraded_factor <= 1",
            name="rate_policy_degraded_factor_range",
        ),
        CheckConstraint(
            "(requests_per_window IS NULL) = (window_seconds IS NULL)",
            name="rate_policy_request_window_paired",
        ),
        CheckConstraint(
            "(requests_per_window IS NOT NULL AND window_seconds IS NOT NULL) "
            "OR max_concurrency IS NOT NULL",
            name="rate_policy_meaningful",
        ),
        Index(
            "uq_rate_limit_policy_one_enabled",
            "tenant_id",
            "scope_type",
            "scope_id",
            unique=True,
            postgresql_where=text("enabled = true"),
        ),
        Index(
            "ix_rate_limit_policies_scope",
            "tenant_id",
            "scope_type",
            "scope_id",
        ),
    )


class Request(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "requests"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    request_id: Mapped[UUID] = mapped_column(nullable=False, unique=True)
    application_id: Mapped[UUID | None] = mapped_column(nullable=True)
    api_key_id: Mapped[UUID | None] = mapped_column(nullable=True)
    workload_type: Mapped[WorkloadType] = mapped_column(
        Enum(WorkloadType, name="workload_type"), nullable=False
    )
    route_id: Mapped[UUID | None] = mapped_column(nullable=True)
    method: Mapped[str] = mapped_column(String(16), nullable=False)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    degraded_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(
            ["tenant_id", "application_id"],
            ["applications.tenant_id", "applications.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "application_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.application_id", "api_keys.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "route_id"],
            ["normal_api_routes.tenant_id", "normal_api_routes.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_requests_tenant_id"),
        UniqueConstraint(
            "tenant_id",
            "id",
            "workload_type",
            name="uq_requests_tenant_id_workload",
        ),
        CheckConstraint(
            "api_key_id IS NULL OR application_id IS NOT NULL",
            name="request_api_key_requires_application",
        ),
        CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0", name="request_latency_nonnegative"
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="request_completion_after_start",
        ),
        CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name="request_status_code_valid",
        ),
        CheckConstraint(
            "(workload_type = 'NORMAL' AND route_id IS NOT NULL) OR "
            "(workload_type = 'LLM' AND route_id IS NULL)",
            name="request_workload_attribution",
        ),
        Index("ix_requests_tenant_started_at", "tenant_id", "started_at"),
        Index(
            "ix_requests_tenant_workload_started",
            "tenant_id",
            "workload_type",
            "started_at",
        ),
        Index("ix_requests_api_key_id", "api_key_id"),
        Index("ix_requests_route_id", "route_id"),
    )


class LlmRequest(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_requests"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    request_id_fk: Mapped[UUID] = mapped_column(nullable=False, unique=True)
    request_workload_type: Mapped[WorkloadType] = mapped_column(
        Enum(WorkloadType, name="workload_type", create_type=False),
        nullable=False,
        default=WorkloadType.LLM,
        server_default=text("'LLM'::workload_type"),
    )
    requested_model: Mapped[str] = mapped_column(String(160), nullable=False)
    resolved_alias_id: Mapped[UUID | None] = mapped_column(nullable=True)
    final_model_id: Mapped[UUID | None] = mapped_column(nullable=True)
    stream: Mapped[bool] = mapped_column(Boolean, nullable=False)
    tool_calling: Mapped[bool] = mapped_column(Boolean, nullable=False)
    input_tokens_total: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_tokens_total: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    known_cost_total: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    ttft_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    final_status: Mapped[LlmFinalStatus | None] = mapped_column(
        Enum(LlmFinalStatus, name="llm_final_status"), nullable=True
    )
    cost_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    output_committed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "request_id_fk", "request_workload_type"],
            ["requests.tenant_id", "requests.id", "requests.workload_type"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "resolved_alias_id"],
            ["llm_aliases.tenant_id", "llm_aliases.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "final_model_id"],
            ["llm_models.tenant_id", "llm_models.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_llm_requests_tenant_id"),
        CheckConstraint(
            "attempt_count >= 0", name="llm_request_attempt_count_nonnegative"
        ),
        CheckConstraint(
            "request_workload_type = 'LLM'",
            name="llm_request_workload_type_llm",
        ),
        CheckConstraint(
            "input_tokens_total IS NULL OR input_tokens_total >= 0",
            name="llm_request_input_tokens_nonnegative",
        ),
        CheckConstraint(
            "output_tokens_total IS NULL OR output_tokens_total >= 0",
            name="llm_request_output_tokens_nonnegative",
        ),
        CheckConstraint(
            "known_cost_total IS NULL OR known_cost_total >= 0",
            name="llm_request_known_cost_nonnegative",
        ),
        CheckConstraint(
            "ttft_ms IS NULL OR ttft_ms >= 0", name="llm_request_ttft_nonnegative"
        ),
        Index("ix_llm_requests_tenant_final_model", "tenant_id", "final_model_id"),
        Index("ix_llm_requests_tenant_alias", "tenant_id", "resolved_alias_id"),
    )


class LlmAttempt(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_attempts"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    llm_request_id: Mapped[UUID] = mapped_column(nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_target_id: Mapped[UUID] = mapped_column(nullable=False)
    model_id: Mapped[UUID] = mapped_column(nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[AttemptStatus] = mapped_column(
        Enum(AttemptStatus, name="attempt_status"), nullable=False
    )
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    known_cost: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    first_output_committed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    provider_midstream_failure: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    provider_ttft_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    retry_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "llm_request_id"],
            ["llm_requests.tenant_id", "llm_requests.id"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "provider_target_id"],
            ["llm_provider_targets.tenant_id", "llm_provider_targets.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "provider_target_id", "model_id"],
            [
                "llm_models.tenant_id",
                "llm_models.provider_target_id",
                "llm_models.id",
            ],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "llm_request_id", "attempt_no", name="uq_llm_attempts_request_attempt"
        ),
        CheckConstraint("attempt_no > 0", name="llm_attempt_number_positive"),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="llm_attempt_input_tokens_nonnegative",
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="llm_attempt_output_tokens_nonnegative",
        ),
        CheckConstraint(
            "known_cost IS NULL OR known_cost >= 0",
            name="llm_attempt_known_cost_nonnegative",
        ),
        CheckConstraint(
            "provider_ttft_ms IS NULL OR provider_ttft_ms >= 0",
            name="llm_attempt_provider_ttft_nonnegative",
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="llm_attempt_completion_after_start",
        ),
        CheckConstraint(
            "provider_midstream_failure = false OR status = 'FAILED'",
            name="llm_attempt_midstream_failure_status",
        ),
        Index("ix_llm_attempts_tenant_started_at", "tenant_id", "started_at"),
        Index(
            "ix_llm_attempts_target_model_started",
            "provider_target_id",
            "model_id",
            "started_at",
        ),
        Index(
            "ix_llm_attempts_tenant_request_attempt",
            "tenant_id",
            "llm_request_id",
            "attempt_no",
        ),
    )


class AuditLog(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "audit_logs"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    actor_admin_user_id: Mapped[UUID | None] = mapped_column(nullable=True)
    actor_admin_token_id: Mapped[UUID | None] = mapped_column(nullable=True)
    request_id: Mapped[UUID | None] = mapped_column(nullable=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[UUID | None] = mapped_column(nullable=True)
    result: Mapped[AuditResult] = mapped_column(
        Enum(AuditResult, name="audit_result"), nullable=False
    )
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(
            ["tenant_id", "actor_admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "actor_admin_token_id"],
            ["admin_tokens.tenant_id", "admin_tokens.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "actor_admin_user_id", "actor_admin_token_id"],
            [
                "admin_tokens.tenant_id",
                "admin_tokens.admin_user_id",
                "admin_tokens.id",
            ],
            ondelete="RESTRICT",
        ),
        Index(
            "ix_audit_logs_tenant_created_id",
            "tenant_id",
            text("created_at DESC"),
            text("id DESC"),
        ),
        Index("ix_audit_logs_actor_user", "actor_admin_user_id"),
        Index("ix_audit_logs_actor_token", "actor_admin_token_id"),
        Index(
            "ix_audit_logs_tenant_resource",
            "tenant_id",
            "resource_type",
            "resource_id",
        ),
        Index("ix_audit_logs_request_id", "request_id"),
    )


class ConfigVersion(Base):
    __tablename__ = "config_versions"

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_by_admin_user_id: Mapped[UUID | None] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(
            ["tenant_id", "updated_by_admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("version >= 0", name="config_version_nonnegative"),
    )


class RetentionCheckpoint(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "retention_checkpoints"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    job_type: Mapped[RetentionJobType] = mapped_column(
        Enum(RetentionJobType, name="retention_job_type"), nullable=False
    )
    scope_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[RetentionState] = mapped_column(
        Enum(RetentionState, name="retention_state"), nullable=False
    )
    source_row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    aggregate_row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_purged_id: Mapped[UUID | None] = mapped_column(nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="RESTRICT"),
        UniqueConstraint(
            "tenant_id",
            "job_type",
            "scope_date",
            "cutoff_at",
            name="uq_retention_checkpoint_scope",
            postgresql_nulls_not_distinct=True,
        ),
        CheckConstraint(
            "source_row_count IS NULL OR source_row_count >= 0",
            name="retention_source_count_nonnegative",
        ),
        CheckConstraint(
            "aggregate_row_count IS NULL OR aggregate_row_count >= 0",
            name="retention_aggregate_count_nonnegative",
        ),
        CheckConstraint(
            "completed_at IS NULL OR state = 'COMPLETED'",
            name="retention_completed_state",
        ),
        CheckConstraint(
            "updated_at >= started_at", name="retention_update_after_start"
        ),
        Index(
            "ix_retention_checkpoints_resume",
            "tenant_id",
            "job_type",
            "state",
            "updated_at",
        ),
        Index(
            "ix_retention_checkpoints_cutoff",
            "tenant_id",
            "job_type",
            "cutoff_at",
        ),
    )
