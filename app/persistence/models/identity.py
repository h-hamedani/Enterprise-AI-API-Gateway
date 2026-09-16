from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from app.persistence.models.enums import PrincipalStatus, ResourceStatus


class Tenant(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "tenants"

    name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        unique=True,
    )

    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )


class AdminUser(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "admin_users"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)

    name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )

    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "id",
            name="uq_admin_users_tenant_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "name",
            name="uq_admin_users_tenant_name",
        ),
        Index(
            "ix_admin_users_tenant_status",
            "tenant_id",
            "status",
        ),
    )


class AdminToken(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "admin_tokens"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    admin_user_id: Mapped[UUID] = mapped_column(nullable=False)

    token_prefix: Mapped[str] = mapped_column(String(32), nullable=False)

    token_hash: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
    )

    status: Mapped[PrincipalStatus] = mapped_column(
        Enum(PrincipalStatus, name="principal_status"),
        nullable=False,
        default=PrincipalStatus.ACTIVE,
    )

    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "id",
            name="uq_admin_tokens_tenant_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "admin_user_id",
            "id",
            name="uq_admin_tokens_tenant_user_id",
        ),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            ondelete="RESTRICT",
        ),
        Index(
            "ix_admin_tokens_tenant_user_status",
            "tenant_id",
            "admin_user_id",
            "status",
        ),
        CheckConstraint(
            "(status = 'REVOKED') = (revoked_at IS NOT NULL)",
            name="admin_token_revoked_status_consistent",
        ),
    )
