"""enforce audit immutability for security operations

Revision ID: d4a7c9e2f1b6
Revises: b7e2c4d6f8a0
"""

from collections.abc import Sequence

from alembic import op

revision: str = "d4a7c9e2f1b6"
down_revision: str | None = "b7e2c4d6f8a0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("REVOKE UPDATE ON audit_logs FROM security_operations")


def downgrade() -> None:
    op.execute("GRANT UPDATE ON audit_logs TO security_operations")
