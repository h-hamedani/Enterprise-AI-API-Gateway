from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, ClassVar
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from app.core.security.idempotency import IdempotencyDigester
from app.core.security.secrets import (
    EncryptedSecret,
    SecretDecryptionError,
    SecretService,
)
from app.persistence.models import Base


class IdempotencyConflictError(RuntimeError):
    pass


class IdempotencyExpiredError(RuntimeError):
    pass


class IdempotencyInProgressError(RuntimeError):
    pass


class IdempotencyReplayError(RuntimeError):
    pass


class ClaimState(StrEnum):
    CREATED = "CREATED"
    IN_PROGRESS = "IN_PROGRESS"
    REPLAY = "REPLAY"


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    state: ClaimState
    record_id: UUID | None
    response_status: int | None = None
    response_body_ciphertext: bytes | None = None
    response_metadata: dict[str, Any] | None = None


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
        lock_material = "\x1f".join(
            (str(tenant_id), str(admin_user_id), endpoint_key, key_hash)
        ).encode()
        advisory_key = int.from_bytes(
            hashlib.sha256(lock_material).digest()[:8], "big", signed=True
        )
        if not connection.scalar(func.pg_try_advisory_xact_lock(advisory_key)):
            return IdempotencyClaim(ClaimState.IN_PROGRESS, None)
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
        if row["state"] != "COMPLETED":
            raise IdempotencyReplayError("Idempotency replay is unavailable.")
        return IdempotencyClaim(
            ClaimState.REPLAY,
            row["id"],
            row["response_status"],
            row["response_body_ciphertext"],
            row["response_metadata"],
        )

    def complete(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        record_id: UUID,
        response_status: int,
        response_body_ciphertext: bytes | None,
        response_metadata: dict[str, Any] | None = None,
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
                response_metadata=response_metadata,
                completed_at=completed_at,
                updated_at=completed_at,
            )
        )
        if result.rowcount != 1:
            raise RuntimeError("Idempotency record is not completable.")


@dataclass(frozen=True, slots=True)
class StoredHttpResponse:
    status_code: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class CoordinatorClaim:
    state: ClaimState
    record_id: UUID | None
    response: StoredHttpResponse | None = None


class IdempotencyCoordinator:
    _REPLAY_FORMAT = "encrypted-http-response-v1"
    _FORBIDDEN_REPLAY_HEADERS: ClassVar[frozenset[str]] = frozenset(
        {
            "authorization",
            "cookie",
            "idempotency-key",
            "set-cookie",
            "x-request-id",
        }
    )

    def __init__(
        self,
        repository: IdempotencyRepository,
        secret_service: SecretService,
    ) -> None:
        self._repository = repository
        self._secret_service = secret_service

    @staticmethod
    def fingerprint(
        *,
        method: str,
        endpoint_key: str,
        body: Any,
        path_parameters: Mapping[str, Any] | None = None,
    ) -> str:
        if not endpoint_key:
            raise ValueError("Endpoint identity must not be empty.")
        canonical = json.dumps(
            {
                "body": body,
                "endpoint": endpoint_key,
                "method": method.upper(),
                "path": {
                    str(key): str(value)
                    for key, value in sorted((path_parameters or {}).items())
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

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
    ) -> CoordinatorClaim:
        claim = self._repository.claim(
            connection,
            tenant_id=tenant_id,
            admin_user_id=admin_user_id,
            endpoint_key=endpoint_key,
            raw_key=raw_key,
            request_fingerprint=request_fingerprint,
            now=now,
        )
        if claim.state is ClaimState.IN_PROGRESS:
            raise IdempotencyInProgressError("Idempotency operation is in progress.")
        if claim.state is ClaimState.CREATED:
            return CoordinatorClaim(ClaimState.CREATED, claim.record_id)
        if claim.record_id is None:
            raise IdempotencyReplayError("Idempotency replay is unavailable.")
        response = self._decrypt_response(
            claim,
            tenant_id=tenant_id,
            admin_user_id=admin_user_id,
            endpoint_key=endpoint_key,
        )
        return CoordinatorClaim(ClaimState.REPLAY, claim.record_id, response)

    def complete(
        self,
        connection: Connection,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        endpoint_key: str,
        record_id: UUID,
        response: StoredHttpResponse,
        now: datetime | None = None,
    ) -> None:
        headers = self._validated_headers(response.headers)
        plaintext = json.dumps(
            {
                "body": base64.b64encode(response.body).decode(),
                "headers": headers,
                "status": response.status_code,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        envelope = self._secret_service.encrypt(
            plaintext,
            aad=self._aad(tenant_id, admin_user_id, endpoint_key, record_id),
        )
        self._repository.complete(
            connection,
            tenant_id=tenant_id,
            record_id=record_id,
            response_status=response.status_code,
            response_body_ciphertext=envelope.ciphertext,
            response_metadata={
                "format": self._REPLAY_FORMAT,
                "key_version": envelope.key_version,
            },
            now=now,
        )

    def _decrypt_response(
        self,
        claim: IdempotencyClaim,
        *,
        tenant_id: UUID,
        admin_user_id: UUID,
        endpoint_key: str,
    ) -> StoredHttpResponse:
        metadata = claim.response_metadata
        try:
            if (
                claim.response_status is None
                or claim.response_body_ciphertext is None
                or not isinstance(metadata, dict)
                or metadata.get("format") != self._REPLAY_FORMAT
                or not isinstance(metadata.get("key_version"), int)
                or claim.record_id is None
            ):
                raise ValueError
            plaintext = self._secret_service.decrypt(
                EncryptedSecret(
                    metadata["key_version"], claim.response_body_ciphertext
                ),
                aad=self._aad(tenant_id, admin_user_id, endpoint_key, claim.record_id),
            )
            values = json.loads(plaintext)
            if set(values) != {"body", "headers", "status"}:
                raise ValueError
            status = values["status"]
            if not isinstance(status, int) or status != claim.response_status:
                raise ValueError
            body = base64.b64decode(values["body"], validate=True)
            headers = self._validated_headers(tuple(map(tuple, values["headers"])))
            return StoredHttpResponse(status, body, tuple(map(tuple, headers)))
        except (
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
            SecretDecryptionError,
        ) as exc:
            raise IdempotencyReplayError("Idempotency replay is unavailable.") from exc

    @classmethod
    def _validated_headers(
        cls, headers: tuple[tuple[str, str], ...]
    ) -> list[list[str]]:
        validated: list[list[str]] = []
        for name, value in headers:
            if name.lower() in cls._FORBIDDEN_REPLAY_HEADERS:
                raise ValueError("Unsafe response header cannot be replayed.")
            if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
                raise ValueError("Invalid response header.")
            validated.append([name, value])
        return validated

    @staticmethod
    def _aad(
        tenant_id: UUID,
        admin_user_id: UUID,
        endpoint_key: str,
        record_id: UUID,
    ) -> bytes:
        return (
            f"idempotency-replay:v1:{tenant_id}:{admin_user_id}:"
            f"{endpoint_key}:{record_id}"
        ).encode()
