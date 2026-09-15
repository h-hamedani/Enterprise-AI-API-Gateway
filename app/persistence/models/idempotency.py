from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.persistence.models.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from app.persistence.models.enums import IdempotencyState


class IdempotencyRecord(
    UUIDPrimaryKeyMixin,
    TimestampMixin,
    Base,
):
    __tablename__ = "idempotency_records"

    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    admin_user_id: Mapped[UUID] = mapped_column(nullable=False)

    endpoint_key: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )

    idempotency_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    request_fingerprint: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )

    state: Mapped[IdempotencyState] = mapped_column(
        Enum(
            IdempotencyState,
            name="idempotency_state",
        ),
        nullable=False,
    )

    response_status: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    response_body_ciphertext: Mapped[bytes | None] = mapped_column(
        LargeBinary,
        nullable=True,
    )

    response_metadata: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "admin_user_id"],
            ["admin_users.tenant_id", "admin_users.id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "admin_user_id",
            "endpoint_key",
            "idempotency_key",
            name="uq_idempotency_scope_key",
        ),
        Index(
            "ix_idempotency_expires_at",
            "expires_at",
        ),
    )
