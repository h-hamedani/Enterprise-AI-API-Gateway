"""add normal api services and routes

Revision ID: d1e0310898aa
Revises: e54a7134967b
Create Date: 2026-09-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d1e0310898aa"
down_revision: str | Sequence[str] | None = "e54a7134967b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


resource_status_enum = postgresql.ENUM(
    "ACTIVE",
    "DISABLED",
    name="resource_status",
    create_type=False,
)


def upgrade() -> None:
    op.create_table(
        "normal_api_services",
        sa.Column(
            "tenant_id",
            sa.Uuid(),
            nullable=False,
        ),
        sa.Column(
            "slug",
            sa.String(length=63),
            nullable=False,
        ),
        sa.Column(
            "display_name",
            sa.String(length=200),
            nullable=False,
        ),
        sa.Column(
            "upstream_base_url",
            sa.String(length=2048),
            nullable=False,
        ),
        sa.Column(
            "status",
            resource_status_enum,
            nullable=False,
        ),
        sa.Column(
            "request_body_limit_bytes",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "connect_timeout_seconds",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "pool_timeout_seconds",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "write_timeout_seconds",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "read_idle_timeout_seconds",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "pre_response_timeout_seconds",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.CheckConstraint(
            "slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'",
            name=op.f("ck_normal_api_services_normal_api_service_slug_format"),
        ),
        sa.CheckConstraint(
            "connect_timeout_seconds > 0",
            name=op.f(
                "ck_normal_api_services_normal_api_service_connect_timeout_positive"
            ),
        ),
        sa.CheckConstraint(
            "pool_timeout_seconds > 0",
            name=op.f(
                "ck_normal_api_services_normal_api_service_pool_timeout_positive"
            ),
        ),
        sa.CheckConstraint(
            "pre_response_timeout_seconds > 0",
            name=op.f(
                "ck_normal_api_services_"
                "normal_api_service_pre_response_timeout_positive"
            ),
        ),
        sa.CheckConstraint(
            "read_idle_timeout_seconds > 0",
            name=op.f(
                "ck_normal_api_services_normal_api_service_read_idle_timeout_positive"
            ),
        ),
        sa.CheckConstraint(
            "request_body_limit_bytes > 0",
            name=op.f("ck_normal_api_services_normal_api_service_body_limit_positive"),
        ),
        sa.CheckConstraint(
            "write_timeout_seconds > 0",
            name=op.f(
                "ck_normal_api_services_normal_api_service_write_timeout_positive"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_normal_api_services_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_normal_api_services"),
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "id",
            name="uq_normal_api_services_tenant_id",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "slug",
            name="uq_normal_api_services_tenant_slug",
        ),
    )

    op.create_index(
        "ix_normal_api_services_tenant_status",
        "normal_api_services",
        ["tenant_id", "status"],
        unique=False,
    )

    op.create_table(
        "normal_api_routes",
        sa.Column(
            "tenant_id",
            sa.Uuid(),
            nullable=False,
        ),
        sa.Column(
            "service_id",
            sa.Uuid(),
            nullable=False,
        ),
        sa.Column(
            "path_pattern",
            sa.String(length=2048),
            nullable=False,
        ),
        sa.Column(
            "method",
            sa.String(length=10),
            nullable=False,
        ),
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
        ),
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.CheckConstraint(
            "method IN ('GET','POST','PUT','PATCH','DELETE')",
            name=op.f("ck_normal_api_routes_normal_api_route_method_allowed"),
        ),
        sa.CheckConstraint(
            "path_pattern LIKE '/%'",
            name=op.f("ck_normal_api_routes_normal_api_route_path_starts_with_slash"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "service_id"],
            [
                "normal_api_services.tenant_id",
                "normal_api_services.id",
            ],
            name=op.f("fk_normal_api_routes_tenant_id_normal_api_services"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_normal_api_routes"),
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "service_id",
            "method",
            "path_pattern",
            name="uq_normal_api_routes_service_method_path",
        ),
    )

    op.create_index(
        "ix_normal_api_routes_tenant_service_enabled",
        "normal_api_routes",
        ["tenant_id", "service_id", "enabled"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_normal_api_routes_tenant_service_enabled",
        table_name="normal_api_routes",
    )
    op.drop_table("normal_api_routes")

    op.drop_index(
        "ix_normal_api_services_tenant_status",
        table_name="normal_api_services",
    )
    op.drop_table("normal_api_services")
