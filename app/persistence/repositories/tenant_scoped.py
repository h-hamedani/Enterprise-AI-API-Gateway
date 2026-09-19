from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import Table, select, update
from sqlalchemy.engine import Connection, RowMapping


class TenantScopedRepository:
    def __init__(self, table: Table) -> None:
        if "tenant_id" not in table.c:
            raise ValueError("Tenant-scoped repository requires a tenant_id column.")
        self._table = table

    def get(
        self, connection: Connection, *, tenant_id: UUID, resource_id: UUID
    ) -> RowMapping | None:
        return (
            connection.execute(
                select(self._table).where(
                    self._table.c.tenant_id == tenant_id,
                    self._table.c.id == resource_id,
                )
            )
            .mappings()
            .one_or_none()
        )

    def update(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        resource_id: UUID,
        values: dict[str, Any],
    ) -> bool:
        result = connection.execute(
            update(self._table)
            .where(
                self._table.c.tenant_id == tenant_id,
                self._table.c.id == resource_id,
            )
            .values(**values)
        )
        return result.rowcount == 1
