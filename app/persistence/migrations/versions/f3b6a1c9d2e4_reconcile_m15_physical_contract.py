"""reconcile M1.5 physical database contract

Revision ID: f3b6a1c9d2e4
Revises: c7d8e9f0a1b2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f3b6a1c9d2e4"
down_revision: str | None = "c7d8e9f0a1b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    principal_status = postgresql.ENUM(
        "ACTIVE", "DISABLED", "REVOKED", name="principal_status"
    )
    permission_resource_type = postgresql.ENUM(
        "SERVICE",
        "ROUTE",
        "LLM_ALIAS",
        "LLM_MODEL",
        name="permission_resource_type",
    )
    permission_action = postgresql.ENUM("INVOKE", name="permission_action")
    service_auth_type = postgresql.ENUM(
        "NONE", "STATIC_BEARER", "STATIC_HEADER", name="service_auth_type"
    )
    principal_status.create(bind, checkfirst=True)
    permission_resource_type.create(bind, checkfirst=True)
    permission_action.create(bind, checkfirst=True)
    service_auth_type.create(bind, checkfirst=True)

    for table_name in ("admin_tokens", "api_keys"):
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name} ALTER COLUMN status TYPE principal_status "
                "USING status::text::principal_status"
            )
        )

    op.add_column("admin_tokens", sa.Column("token_prefix", sa.String(32)))
    op.add_column("admin_tokens", sa.Column("expires_at", sa.DateTime(timezone=True)))
    op.execute(
        "UPDATE admin_tokens SET token_prefix = left(token_hash, 12) "
        "WHERE token_prefix IS NULL"
    )
    op.alter_column("admin_tokens", "token_prefix", nullable=False)
    op.create_unique_constraint(
        "uq_admin_tokens_tenant_user_id",
        "admin_tokens",
        ["tenant_id", "admin_user_id", "id"],
    )
    op.create_check_constraint(
        "ck_admin_tokens_admin_token_revoked_status_consistent",
        "admin_tokens",
        "(status = 'REVOKED') = (revoked_at IS NOT NULL)",
    )

    op.add_column("api_keys", sa.Column("name", sa.String(200)))
    op.add_column("api_keys", sa.Column("key_prefix", sa.String(32)))
    op.add_column("api_keys", sa.Column("expires_at", sa.DateTime(timezone=True)))
    op.execute(
        "UPDATE api_keys SET name = 'legacy-' || left(id::text, 8), "
        "key_prefix = left(key_hash, 12) WHERE name IS NULL OR key_prefix IS NULL"
    )
    op.alter_column("api_keys", "name", nullable=False)
    op.alter_column("api_keys", "key_prefix", nullable=False)
    op.create_unique_constraint(
        "uq_api_keys_tenant_application_id",
        "api_keys",
        ["tenant_id", "application_id", "id"],
    )
    op.create_check_constraint(
        "ck_api_keys_api_key_revoked_status_consistent",
        "api_keys",
        "(status = 'REVOKED') = (revoked_at IS NOT NULL)",
    )

    op.create_table(
        "api_key_permissions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("api_key_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "resource_type",
            postgresql.ENUM(name="permission_resource_type", create_type=False),
            nullable=False,
        ),
        sa.Column("resource_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "action",
            postgresql.ENUM(name="permission_action", create_type=False),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_api_key_permissions"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.id"],
            name="fk_api_key_permissions_tenant_id_api_keys",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "api_key_id",
            "resource_type",
            "resource_id",
            "action",
            name="uq_api_key_permissions_scope",
        ),
    )
    op.create_index(
        "ix_api_key_permissions_authorization_lookup",
        "api_key_permissions",
        ["tenant_id", "api_key_id", "resource_type", "resource_id", "action"],
    )

    op.add_column(
        "normal_api_routes", sa.Column("upstream_path_template", sa.String(1024))
    )
    op.add_column("normal_api_routes", sa.Column("priority", sa.Integer()))
    op.add_column("normal_api_routes", sa.Column("timeout_ms", sa.Integer()))
    op.add_column("normal_api_routes", sa.Column("header_policy", postgresql.JSONB()))
    op.add_column(
        "normal_api_routes",
        sa.Column(
            "status",
            postgresql.ENUM(name="resource_status", create_type=False),
        ),
    )
    op.execute(
        "UPDATE normal_api_routes SET upstream_path_template = path_pattern, "
        "priority = 0, status = CASE WHEN enabled THEN 'ACTIVE'::resource_status "
        "ELSE 'DISABLED'::resource_status END"
    )
    op.alter_column("normal_api_routes", "upstream_path_template", nullable=False)
    op.alter_column("normal_api_routes", "priority", nullable=False)
    op.alter_column("normal_api_routes", "status", nullable=False)
    op.drop_index(
        "ix_normal_api_routes_tenant_service_enabled", table_name="normal_api_routes"
    )
    op.drop_column("normal_api_routes", "enabled")
    op.create_index(
        "ix_normal_api_routes_tenant_service_status",
        "normal_api_routes",
        ["tenant_id", "service_id", "status"],
    )
    op.create_check_constraint(
        "ck_normal_api_routes_normal_api_route_priority_nonnegative",
        "normal_api_routes",
        "priority >= 0",
    )
    op.create_check_constraint(
        "ck_normal_api_routes_normal_api_route_timeout_positive",
        "normal_api_routes",
        "timeout_ms IS NULL OR timeout_ms > 0",
    )
    op.drop_constraint(
        "fk_normal_api_routes_tenant_id_normal_api_services",
        "normal_api_routes",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_normal_api_routes_tenant_id_normal_api_services",
        "normal_api_routes",
        "normal_api_services",
        ["tenant_id", "service_id"],
        ["tenant_id", "id"],
        ondelete="RESTRICT",
    )

    op.create_table(
        "service_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("service_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "auth_type",
            postgresql.ENUM(name="service_auth_type", create_type=False),
            nullable=False,
        ),
        sa.Column("header_name", sa.String(255)),
        sa.Column("secret_ciphertext", sa.LargeBinary()),
        sa.Column("key_version", sa.Integer()),
        sa.Column(
            "status",
            postgresql.ENUM(name="resource_status", create_type=False),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rotated_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("id", name="pk_service_credentials"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "service_id"],
            ["normal_api_services.tenant_id", "normal_api_services.id"],
            name="fk_service_credentials_tenant_id_normal_api_services",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "(auth_type = 'NONE' AND header_name IS NULL AND "
            "secret_ciphertext IS NULL AND key_version IS NULL) OR "
            "(auth_type = 'STATIC_BEARER' AND header_name IS NULL AND "
            "secret_ciphertext IS NOT NULL AND key_version IS NOT NULL) OR "
            "(auth_type = 'STATIC_HEADER' AND header_name IS NOT NULL AND "
            "secret_ciphertext IS NOT NULL AND key_version IS NOT NULL)",
            name="ck_service_credentials_service_credential_auth_fields_consistent",
        ),
        sa.CheckConstraint(
            "key_version IS NULL OR key_version > 0",
            name="ck_service_credentials_service_credential_key_version_positive",
        ),
    )
    op.create_index(
        "uq_service_credentials_one_active",
        "service_credentials",
        ["tenant_id", "service_id"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )
    op.create_index(
        "ix_service_credentials_tenant_service",
        "service_credentials",
        ["tenant_id", "service_id"],
    )

    op.create_unique_constraint(
        "uq_llm_models_tenant_target_id",
        "llm_models",
        ["tenant_id", "provider_target_id", "id"],
    )
    op.drop_constraint("fk_requests_tenant_id_api_keys", "requests", type_="foreignkey")
    op.create_foreign_key(
        "fk_requests_tenant_application_api_key",
        "requests",
        "api_keys",
        ["tenant_id", "application_id", "api_key_id"],
        ["tenant_id", "application_id", "id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_requests_request_api_key_requires_application",
        "requests",
        "api_key_id IS NULL OR application_id IS NOT NULL",
    )
    op.create_unique_constraint(
        "uq_requests_tenant_id_workload",
        "requests",
        ["tenant_id", "id", "workload_type"],
    )

    op.add_column(
        "llm_requests",
        sa.Column(
            "request_workload_type",
            postgresql.ENUM(name="workload_type", create_type=False),
            nullable=False,
            server_default=sa.text("'LLM'::workload_type"),
        ),
    )
    op.drop_constraint(
        "fk_llm_requests_tenant_id_requests", "llm_requests", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_llm_requests_tenant_request_workload",
        "llm_requests",
        "requests",
        ["tenant_id", "request_id_fk", "request_workload_type"],
        ["tenant_id", "id", "workload_type"],
        ondelete="CASCADE",
    )
    op.create_check_constraint(
        "ck_llm_requests_llm_request_workload_type_llm",
        "llm_requests",
        "request_workload_type = 'LLM'",
    )

    op.drop_constraint(
        "fk_llm_attempts_tenant_id_llm_models", "llm_attempts", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_llm_attempts_tenant_target_model",
        "llm_attempts",
        "llm_models",
        ["tenant_id", "provider_target_id", "model_id"],
        ["tenant_id", "provider_target_id", "id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_audit_logs_tenant_user_token",
        "audit_logs",
        "admin_tokens",
        ["tenant_id", "actor_admin_user_id", "actor_admin_token_id"],
        ["tenant_id", "admin_user_id", "id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_audit_logs_tenant_user_token", "audit_logs", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_llm_attempts_tenant_target_model", "llm_attempts", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_llm_attempts_tenant_id_llm_models",
        "llm_attempts",
        "llm_models",
        ["tenant_id", "model_id"],
        ["tenant_id", "id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint(
        "ck_llm_requests_llm_request_workload_type_llm",
        "llm_requests",
        type_="check",
    )
    op.drop_constraint(
        "fk_llm_requests_tenant_request_workload",
        "llm_requests",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_llm_requests_tenant_id_requests",
        "llm_requests",
        "requests",
        ["tenant_id", "request_id_fk"],
        ["tenant_id", "id"],
        ondelete="CASCADE",
    )
    op.drop_column("llm_requests", "request_workload_type")
    op.drop_constraint("uq_requests_tenant_id_workload", "requests", type_="unique")
    op.drop_constraint(
        "ck_requests_request_api_key_requires_application", "requests", type_="check"
    )
    op.drop_constraint(
        "fk_requests_tenant_application_api_key", "requests", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_requests_tenant_id_api_keys",
        "requests",
        "api_keys",
        ["tenant_id", "api_key_id"],
        ["tenant_id", "id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("uq_llm_models_tenant_target_id", "llm_models", type_="unique")

    op.drop_index(
        "ix_service_credentials_tenant_service", table_name="service_credentials"
    )
    op.drop_index("uq_service_credentials_one_active", table_name="service_credentials")
    op.drop_table("service_credentials")

    op.drop_constraint(
        "fk_normal_api_routes_tenant_id_normal_api_services",
        "normal_api_routes",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_normal_api_routes_tenant_id_normal_api_services",
        "normal_api_routes",
        "normal_api_services",
        ["tenant_id", "service_id"],
        ["tenant_id", "id"],
        ondelete="CASCADE",
    )
    op.drop_constraint(
        "ck_normal_api_routes_normal_api_route_timeout_positive",
        "normal_api_routes",
        type_="check",
    )
    op.drop_constraint(
        "ck_normal_api_routes_normal_api_route_priority_nonnegative",
        "normal_api_routes",
        type_="check",
    )
    op.add_column("normal_api_routes", sa.Column("enabled", sa.Boolean()))
    op.execute(
        "UPDATE normal_api_routes SET enabled = (status = 'ACTIVE'::resource_status)"
    )
    op.alter_column("normal_api_routes", "enabled", nullable=False)
    op.drop_index(
        "ix_normal_api_routes_tenant_service_status", table_name="normal_api_routes"
    )
    op.drop_column("normal_api_routes", "status")
    op.create_index(
        "ix_normal_api_routes_tenant_service_enabled",
        "normal_api_routes",
        ["tenant_id", "service_id", "enabled"],
    )
    op.drop_column("normal_api_routes", "header_policy")
    op.drop_column("normal_api_routes", "timeout_ms")
    op.drop_column("normal_api_routes", "priority")
    op.drop_column("normal_api_routes", "upstream_path_template")

    op.drop_index(
        "ix_api_key_permissions_authorization_lookup",
        table_name="api_key_permissions",
    )
    op.drop_table("api_key_permissions")

    op.drop_constraint(
        "ck_api_keys_api_key_revoked_status_consistent", "api_keys", type_="check"
    )
    op.drop_constraint("uq_api_keys_tenant_application_id", "api_keys", type_="unique")
    op.drop_column("api_keys", "expires_at")
    op.drop_column("api_keys", "key_prefix")
    op.drop_column("api_keys", "name")
    op.drop_constraint(
        "ck_admin_tokens_admin_token_revoked_status_consistent",
        "admin_tokens",
        type_="check",
    )
    op.drop_constraint("uq_admin_tokens_tenant_user_id", "admin_tokens", type_="unique")
    op.drop_column("admin_tokens", "expires_at")
    op.drop_column("admin_tokens", "token_prefix")

    for table_name in ("admin_tokens", "api_keys"):
        op.execute(
            sa.text(
                f"UPDATE {table_name} SET status = 'DISABLED' "
                "WHERE status::text = 'REVOKED'"
            )
        )
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name} ALTER COLUMN status TYPE resource_status "
                "USING status::text::resource_status"
            )
        )

    bind = op.get_bind()
    postgresql.ENUM(name="service_auth_type").drop(bind, checkfirst=True)
    postgresql.ENUM(name="permission_action").drop(bind, checkfirst=True)
    postgresql.ENUM(name="permission_resource_type").drop(bind, checkfirst=True)
    postgresql.ENUM(name="principal_status").drop(bind, checkfirst=True)
