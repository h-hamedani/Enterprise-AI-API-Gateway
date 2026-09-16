from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class IssuedCredential:
    raw: str
    safe_prefix: str
    verifier: str


class CredentialHasher:
    def __init__(self, secret: bytes) -> None:
        if len(secret) < 32:
            raise ValueError("Credential HMAC secret must be at least 32 bytes.")
        self._secret = secret

    def issue(self, prefix: str) -> IssuedCredential:
        if prefix not in {"adm_", "gw_"}:
            raise ValueError("Unsupported credential prefix.")
        raw = prefix + secrets.token_urlsafe(32)
        return IssuedCredential(
            raw=raw,
            safe_prefix=raw[:12],
            verifier=self.hash(raw),
        )

    def hash(self, raw: str) -> str:
        return hmac.new(
            self._secret,
            raw.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def verify(self, raw: str, verifier: str) -> bool:
        return hmac.compare_digest(self.hash(raw), verifier)


class CredentialAuthenticator:
    def __init__(self, hasher: CredentialHasher) -> None:
        self._hasher = hasher

    def authenticate(
        self,
        raw: str,
        *,
        verifier: str,
        status: str,
        expires_at: datetime | None,
        now: datetime | None = None,
    ) -> bool:
        verifier_matches = self._hasher.verify(raw, verifier)
        checked_at = now or datetime.now(UTC)
        usable = status == "ACTIVE" and (expires_at is None or expires_at > checked_at)
        return verifier_matches and usable
