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
from app.persistence.models.enums import (
    PermissionAction,
    PermissionResourceType,
    PrincipalStatus,
    ResourceStatus,
)


class Application(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "applications"

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
            name="uq_applications_tenant_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "name",
            name="uq_applications_tenant_name",
        ),
        Index(
            "ix_applications_tenant_status",
            "tenant_id",
            "status",
        ),
    )


class ApiKey(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "api_keys"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    application_id: Mapped[UUID] = mapped_column(nullable=False)

    name: Mapped[str] = mapped_column(String(200), nullable=False)

    key_prefix: Mapped[str] = mapped_column(String(32), nullable=False)

    key_hash: Mapped[str] = mapped_column(
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
            name="uq_api_keys_tenant_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "application_id",
            "id",
            name="uq_api_keys_tenant_application_id",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "application_id"],
            ["applications.tenant_id", "applications.id"],
            ondelete="RESTRICT",
        ),
        Index(
            "ix_api_keys_tenant_application_status",
            "tenant_id",
            "application_id",
            "status",
        ),
        CheckConstraint(
            "(status = 'REVOKED') = (revoked_at IS NOT NULL)",
            name="api_key_revoked_status_consistent",
        ),
    )


class ApiKeyPermission(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "api_key_permissions"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    api_key_id: Mapped[UUID] = mapped_column(nullable=False)
    resource_type: Mapped[PermissionResourceType] = mapped_column(
        Enum(PermissionResourceType, name="permission_resource_type"), nullable=False
    )
    resource_id: Mapped[UUID] = mapped_column(nullable=False)
    action: Mapped[PermissionAction] = mapped_column(
        Enum(PermissionAction, name="permission_action"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "tenant_id",
            "api_key_id",
            "resource_type",
            "resource_id",
            "action",
            name="uq_api_key_permissions_scope",
        ),
        Index(
            "ix_api_key_permissions_authorization_lookup",
            "tenant_id",
            "api_key_id",
            "resource_type",
            "resource_id",
            "action",
        ),
    )
