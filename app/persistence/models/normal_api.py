from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from app.persistence.models.enums import ResourceStatus, ServiceAuthType


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

    upstream_path_template: Mapped[str] = mapped_column(String(1024), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    timeout_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    header_policy: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"),
        nullable=False,
        default=ResourceStatus.ACTIVE,
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "id",
            name="uq_normal_api_routes_tenant_id",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "service_id"],
            [
                "normal_api_services.tenant_id",
                "normal_api_services.id",
            ],
            ondelete="RESTRICT",
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
        CheckConstraint("priority >= 0", name="normal_api_route_priority_nonnegative"),
        CheckConstraint(
            "timeout_ms IS NULL OR timeout_ms > 0",
            name="normal_api_route_timeout_positive",
        ),
        Index(
            "ix_normal_api_routes_tenant_service_status",
            "tenant_id",
            "service_id",
            "status",
        ),
    )


class ServiceCredential(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "service_credentials"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    service_id: Mapped[UUID] = mapped_column(nullable=False)
    auth_type: Mapped[ServiceAuthType] = mapped_column(
        Enum(ServiceAuthType, name="service_auth_type"), nullable=False
    )
    header_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    secret_ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    key_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[ResourceStatus] = mapped_column(
        Enum(ResourceStatus, name="resource_status"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    rotated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "service_id"],
            ["normal_api_services.tenant_id", "normal_api_services.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "(auth_type = 'NONE' AND header_name IS NULL "
            "AND secret_ciphertext IS NULL AND key_version IS NULL) OR "
            "(auth_type = 'STATIC_BEARER' AND header_name IS NULL "
            "AND secret_ciphertext IS NOT NULL AND key_version IS NOT NULL) OR "
            "(auth_type = 'STATIC_HEADER' AND header_name IS NOT NULL "
            "AND secret_ciphertext IS NOT NULL AND key_version IS NOT NULL)",
            name="service_credential_auth_fields_consistent",
        ),
        CheckConstraint(
            "key_version IS NULL OR key_version > 0",
            name="service_credential_key_version_positive",
        ),
        Index(
            "uq_service_credentials_one_active",
            "tenant_id",
            "service_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
        Index("ix_service_credentials_tenant_service", "tenant_id", "service_id"),
    )
