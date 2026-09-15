from __future__ import annotations

from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from app.persistence.models.enums import ResourceStatus


class NormalApiService(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "normal_api_services"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)

    slug: Mapped[str] = mapped_column(
        String(63),
        nullable=False,
    )

    display_name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )

    upstream_base_url: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
    )

    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    request_body_limit_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=10 * 1024 * 1024,
    )

    connect_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=5,
    )

    pool_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=5,
    )

    write_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=30,
    )

    read_idle_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=60,
    )

    pre_response_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=120,
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
            name="uq_normal_api_services_tenant_id",
        ),
        UniqueConstraint(
            "tenant_id",
            "slug",
            name="uq_normal_api_services_tenant_slug",
        ),
        CheckConstraint(
            "slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'",
            name="normal_api_service_slug_format",
        ),
        CheckConstraint(
            "request_body_limit_bytes > 0",
            name="normal_api_service_body_limit_positive",
        ),
        CheckConstraint(
            "connect_timeout_seconds > 0",
            name="normal_api_service_connect_timeout_positive",
        ),
        CheckConstraint(
            "pool_timeout_seconds > 0",
            name="normal_api_service_pool_timeout_positive",
        ),
        CheckConstraint(
            "write_timeout_seconds > 0",
            name="normal_api_service_write_timeout_positive",
        ),
        CheckConstraint(
            "read_idle_timeout_seconds > 0",
            name="normal_api_service_read_idle_timeout_positive",
        ),
        CheckConstraint(
            "pre_response_timeout_seconds > 0",
            name="normal_api_service_pre_response_timeout_positive",
        ),
        Index(
            "ix_normal_api_services_tenant_status",
            "tenant_id",
            "status",
        ),
    )


class NormalApiRoute(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "normal_api_routes"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    service_id: Mapped[UUID] = mapped_column(nullable=False)

    path_pattern: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
    )

    method: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
    )

    enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "service_id"],
            [
                "normal_api_services.tenant_id",
                "normal_api_services.id",
            ],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "tenant_id",
            "service_id",
            "method",
            "path_pattern",
            name="uq_normal_api_routes_service_method_path",
        ),
        CheckConstraint(
            "method IN ('GET','POST','PUT','PATCH','DELETE')",
            name="normal_api_route_method_allowed",
        ),
        CheckConstraint(
            "path_pattern LIKE '/%'",
            name="normal_api_route_path_starts_with_slash",
        ),
        Index(
            "ix_normal_api_routes_tenant_service_enabled",
            "tenant_id",
            "service_id",
            "enabled",
        ),
    )
