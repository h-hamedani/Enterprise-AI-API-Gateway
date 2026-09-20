from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, Field

from app.core.errors import invalid_request

PAGINATION_ORDER = ("created_at DESC", "id DESC")


class PaginationParameters(BaseModel):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = None


@dataclass(frozen=True, slots=True)
class CursorPosition:
    tenant_id: UUID
    created_at: datetime
    resource_id: UUID


class PaginationCursorCodec:
    """Signed opaque cursor for deterministic created_at DESC, id DESC pages."""

    def __init__(self, signing_key: bytes) -> None:
        if len(signing_key) < 32:
            raise ValueError("Pagination cursor signing key must be at least 32 bytes.")
        self._signing_key = signing_key

    def encode(self, position: CursorPosition) -> str:
        created_at = position.created_at
        if created_at.tzinfo is None:
            raise ValueError("Cursor timestamp must be timezone-aware.")
        payload = json.dumps(
            {
                "v": 1,
                "t": str(position.tenant_id),
                "c": created_at.astimezone(UTC).isoformat(),
                "i": str(position.resource_id),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        signature = hmac.new(self._signing_key, payload, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(payload + signature).rstrip(b"=").decode()

    def decode(self, cursor: str, *, tenant_id: UUID) -> CursorPosition:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            packed = base64.b64decode(padded, altchars=b"-_", validate=True)
            payload, signature = packed[:-32], packed[-32:]
            expected = hmac.new(self._signing_key, payload, hashlib.sha256).digest()
            if len(signature) != 32 or not hmac.compare_digest(signature, expected):
                raise ValueError
            values = json.loads(payload)
            if set(values) != {"v", "t", "c", "i"} or values["v"] != 1:
                raise ValueError
            position = CursorPosition(
                tenant_id=UUID(values["t"]),
                created_at=datetime.fromisoformat(values["c"]),
                resource_id=UUID(values["i"]),
            )
            if position.created_at.tzinfo is None or position.tenant_id != tenant_id:
                raise ValueError
            return position
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid pagination cursor.") from exc


def decode_cursor(
    codec: PaginationCursorCodec,
    cursor: str,
    *,
    tenant_id: UUID,
) -> CursorPosition:
    try:
        return codec.decode(cursor, tenant_id=tenant_id)
    except ValueError:
        invalid_request(param="cursor")
