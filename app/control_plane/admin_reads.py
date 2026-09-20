from __future__ import annotations

import hmac
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import and_, desc, or_, select
from sqlalchemy.engine import Connection

from app.core.pagination import CursorPosition, PaginationCursorCodec
from app.persistence.models import Base
from app.schemas.control_plane import AuditLogResponse, AuditPage


@dataclass(frozen=True, slots=True)
class ConfigVersionResult:
    config_version: int


class AdminReadService:
    def __init__(self, cursor_codec: PaginationCursorCodec) -> None:
        self._cursor_codec = cursor_codec
        self._audit_logs = Base.metadata.tables["audit_logs"]
        self._config_versions = Base.metadata.tables["config_versions"]

    def list_audit_logs(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        limit: int,
        cursor: str | None,
    ) -> AuditPage:
        statement = select(self._audit_logs).where(
            self._audit_logs.c.tenant_id == tenant_id
        )
        if cursor is not None:
            position = self._cursor_codec.decode(cursor, tenant_id=tenant_id)
            statement = statement.where(
                or_(
                    self._audit_logs.c.created_at < position.created_at,
                    and_(
                        self._audit_logs.c.created_at == position.created_at,
                        self._audit_logs.c.id < position.resource_id,
                    ),
                )
            )
        rows = (
            connection.execute(
                statement.order_by(
                    desc(self._audit_logs.c.created_at),
                    desc(self._audit_logs.c.id),
                ).limit(limit + 1)
            )
            .mappings()
            .all()
        )
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        next_cursor = None
        if has_more:
            last = page_rows[-1]
            next_cursor = self._cursor_codec.encode(
                CursorPosition(tenant_id, last["created_at"], last["id"])
            )
        return AuditPage(
            data=[
                AuditLogResponse(
                    id=row["id"],
                    actor_admin_user_id=row["actor_admin_user_id"],
                    actor_admin_token_id=row["actor_admin_token_id"],
                    action=row["action"],
                    resource_type=row["resource_type"],
                    resource_id=row["resource_id"],
                    request_id=row["request_id"],
                    result=row["result"],
                    created_at=row["created_at"],
                )
                for row in page_rows
            ],
            next_cursor=next_cursor,
        )

    def get_config_version(
        self, connection: Connection, *, tenant_id: UUID
    ) -> ConfigVersionResult:
        version = connection.scalar(
            select(self._config_versions.c.version).where(
                self._config_versions.c.tenant_id == tenant_id
            )
        )
        return ConfigVersionResult(config_version=version or 0)


def create_admin_read_service(signing_key: bytes) -> AdminReadService:
    cursor_key = hmac.new(
        signing_key,
        b"control-plane-audit-pagination-cursor-v1",
        "sha256",
    ).digest()
    return AdminReadService(PaginationCursorCodec(cursor_key))
