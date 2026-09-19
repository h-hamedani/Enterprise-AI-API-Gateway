"""complete M1.6 security core and idempotency digest contract

Revision ID: b7e2c4d6f8a0
Revises: f3b6a1c9d2e4
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b7e2c4d6f8a0"
down_revision: str | None = "f3b6a1c9d2e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Historical values may be raw keys. Without the original HMAC secret there is no
    # safe deterministic backfill, so coordinated cleanup/reset is required first.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM idempotency_records LIMIT 1) THEN
            RAISE EXCEPTION
              'M1.6 requires idempotency_records to be empty; raw historical keys cannot be safely backfilled without their original HMAC secret';
          END IF;
        END $$
        """
    )
    op.drop_constraint(
        "uq_idempotency_scope_key", "idempotency_records", type_="unique"
    )
    op.alter_column(
        "idempotency_records",
        "idempotency_key",
        new_column_name="idempotency_key_hash",
    )
    op.execute(
        "ALTER TABLE idempotency_records "
        "ALTER COLUMN idempotency_key_hash TYPE varchar(64)"
    )
    op.create_unique_constraint(
        "uq_idempotency_scope_key_hash",
        "idempotency_records",
        ["tenant_id", "admin_user_id", "endpoint_key", "idempotency_key_hash"],
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'migration_owner') THEN
            CREATE ROLE migration_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway_runtime') THEN
            CREATE ROLE gateway_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'retention_worker') THEN
            CREATE ROLE retention_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'security_operations') THEN
            CREATE ROLE security_operations NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
          END IF;
        END $$;

        GRANT USAGE, CREATE ON SCHEMA public TO migration_owner;
        GRANT USAGE ON SCHEMA public
          TO gateway_runtime, retention_worker, security_operations;
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO gateway_runtime;
        REVOKE DELETE ON requests, llm_requests, llm_attempts, audit_logs
          FROM gateway_runtime;
        REVOKE UPDATE ON audit_logs FROM gateway_runtime;

        GRANT SELECT ON requests, llm_requests, llm_attempts,
          retention_checkpoints, idempotency_records TO retention_worker;
        GRANT INSERT, UPDATE ON retention_checkpoints TO retention_worker;
        GRANT DELETE ON requests, llm_requests, llm_attempts,
          idempotency_records TO retention_worker;
        REVOKE ALL ON admin_tokens, api_keys, llm_provider_credentials,
          service_credentials FROM retention_worker;

        GRANT SELECT, INSERT, UPDATE ON tenants, admin_users, admin_tokens,
          audit_logs TO security_operations;
        REVOKE DELETE ON tenants, admin_users, admin_tokens, audit_logs
          FROM security_operations;

        ALTER DEFAULT PRIVILEGES IN SCHEMA public
          GRANT SELECT, INSERT, UPDATE ON TABLES TO gateway_runtime;
        """
    )


def downgrade() -> None:
    op.execute("REVOKE USAGE, CREATE ON SCHEMA public FROM migration_owner")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE SELECT, INSERT, UPDATE ON TABLES FROM gateway_runtime"
    )
    op.execute(
        "REVOKE ALL ON ALL TABLES IN SCHEMA public "
        "FROM gateway_runtime, retention_worker, security_operations"
    )
    op.execute(
        "REVOKE USAGE ON SCHEMA public "
        "FROM gateway_runtime, retention_worker, security_operations"
    )
    op.drop_constraint(
        "uq_idempotency_scope_key_hash", "idempotency_records", type_="unique"
    )
    op.execute(
        "ALTER TABLE idempotency_records "
        "ALTER COLUMN idempotency_key_hash TYPE varchar(255)"
    )
    op.alter_column(
        "idempotency_records",
        "idempotency_key_hash",
        new_column_name="idempotency_key",
    )
    op.create_unique_constraint(
        "uq_idempotency_scope_key",
        "idempotency_records",
        ["tenant_id", "admin_user_id", "endpoint_key", "idempotency_key"],
    )
