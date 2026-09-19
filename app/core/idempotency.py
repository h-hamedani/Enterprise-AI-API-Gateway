from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from app.core.security.idempotency import IdempotencyDigester
from app.persistence.models import Base


class IdempotencyConflictError(RuntimeError):
    pass


class IdempotencyExpiredError(RuntimeError):
    pass


class ClaimState(StrEnum):
    CREATED = "CREATED"
    IN_PROGRESS = "IN_PROGRESS"
    REPLAY = "REPLAY"


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    state: ClaimState
    record_id: UUID
    response_status: int | None = None
    response_body_ciphertext: bytes | None = None


class IdempotencyRepository:
    def __init__(
        self, digester: IdempotencyDigester, *, ttl: timedelta = timedelta(hours=24)
    ) -> None:
        self._digester = digester
        self._ttl = ttl
        self._table = Base.metadata.tables["idempotency_records"]

    def claim(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        endpoint_key: str,
        raw_key: str,
        request_fingerprint: str,
        now: datetime | None = None,
    ) -> IdempotencyClaim:
        claimed_at = now or datetime.now(UTC)
        key_hash = self._digester.digest(raw_key)
        record_id = uuid4()
        inserted_id = connection.scalar(
            pg_insert(self._table)
            .values(
                id=record_id,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                endpoint_key=endpoint_key,
                idempotency_key_hash=key_hash,
                request_fingerprint=request_fingerprint,
                state="IN_PROGRESS",
                expires_at=claimed_at + self._ttl,
                created_at=claimed_at,
                updated_at=claimed_at,
            )
            .on_conflict_do_nothing(constraint="uq_idempotency_scope_key_hash")
            .returning(self._table.c.id)
        )
        if inserted_id is not None:
            return IdempotencyClaim(ClaimState.CREATED, record_id)

        row = (
            connection.execute(
                select(self._table).where(
                    self._table.c.tenant_id == tenant_id,
                    self._table.c.admin_user_id == admin_user_id,
                    self._table.c.endpoint_key == endpoint_key,
                    self._table.c.idempotency_key_hash == key_hash,
                )
            )
            .mappings()
            .one()
        )
        if row["expires_at"] <= claimed_at:
            raise IdempotencyExpiredError(
                "Idempotency record expired and must be removed by retention."
            )
        if row["request_fingerprint"] != request_fingerprint:
            raise IdempotencyConflictError(
                "Idempotency key is bound to another request."
            )
        if row["state"] == "IN_PROGRESS":
            return IdempotencyClaim(ClaimState.IN_PROGRESS, row["id"])
        return IdempotencyClaim(
            ClaimState.REPLAY,
            row["id"],
            row["response_status"],
            row["response_body_ciphertext"],
        )

    def complete(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        record_id: UUID,
        response_status: int,
        response_body_ciphertext: bytes | None,
        now: datetime | None = None,
    ) -> None:
        completed_at = now or datetime.now(UTC)
        result = connection.execute(
            update(self._table)
            .where(
                self._table.c.tenant_id == tenant_id,
                self._table.c.id == record_id,
                self._table.c.state == "IN_PROGRESS",
            )
            .values(
                state="COMPLETED",
                response_status=response_status,
                response_body_ciphertext=response_body_ciphertext,
                completed_at=completed_at,
                updated_at=completed_at,
            )
        )
        if result.rowcount != 1:
            raise RuntimeError("Idempotency record is not completable.")
