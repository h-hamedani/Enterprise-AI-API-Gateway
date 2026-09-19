from __future__ import annotations

import hashlib
import hmac


class IdempotencyDigester:
    def __init__(self, secret: bytes) -> None:
        if len(secret) < 32:
            raise ValueError("Idempotency HMAC secret must be at least 32 bytes.")
        self._secret = secret

    def digest(self, raw_key: str) -> str:
        if not raw_key:
            raise ValueError("Idempotency-Key must not be empty.")
        return hmac.new(
            self._secret,
            raw_key.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def matches(candidate: str, expected: str) -> bool:
        return hmac.compare_digest(candidate, expected)
