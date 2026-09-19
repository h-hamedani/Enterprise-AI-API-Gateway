from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest

from app.control_plane import break_glass
from app.core.security.credentials import CredentialAuthenticator, CredentialHasher
from app.core.security.idempotency import IdempotencyDigester
from app.core.security.secrets import SecretDecryptionError, SecretService


def test_idempotency_digest_is_deterministic_and_never_logs_raw_key(caplog):
    service = IdempotencyDigester(b"i" * 32)
    raw = "customer-supplied-idempotency-key"

    with caplog.at_level(logging.DEBUG):
        first = service.digest(raw)
        second = service.digest(raw)

    assert first == second
    assert len(first) == 64
    assert service.digest("different") != first
    assert raw not in caplog.text


@pytest.mark.parametrize("prefix", ["adm_", "gw_"])
def test_credentials_are_show_once_and_verify_without_storing_raw(prefix):
    hasher = CredentialHasher(b"c" * 32)
    issued = hasher.issue(prefix)

    assert issued.raw.startswith(prefix)
    assert issued.safe_prefix == issued.raw[:12]
    assert issued.raw not in issued.verifier
    assert hasher.verify(issued.raw, issued.verifier)
    assert not hasher.verify(issued.raw + "wrong", issued.verifier)


@pytest.mark.parametrize("status", ["DISABLED", "REVOKED"])
def test_credential_authentication_rejects_inactive_credentials(status):
    hasher = CredentialHasher(b"c" * 32)
    issued = hasher.issue("adm_")
    assert not CredentialAuthenticator(hasher).authenticate(
        issued.raw,
        verifier=issued.verifier,
        status=status,
        expires_at=None,
    )


def test_credential_authentication_rejects_expired_credentials():
    hasher = CredentialHasher(b"c" * 32)
    issued = hasher.issue("gw_")
    now = datetime.now(UTC)
    assert not CredentialAuthenticator(hasher).authenticate(
        issued.raw,
        verifier=issued.verifier,
        status="ACTIVE",
        expires_at=now - timedelta(seconds=1),
        now=now,
    )


def test_secret_service_authenticated_round_trip_and_tamper_detection():
    service = SecretService({1: b"k" * 32}, current_key_version=1)
    plaintext = b"provider-secret-value"
    envelope = service.encrypt(plaintext, aad=b"tenant:resource:provider")

    assert envelope.key_version == 1
    assert plaintext not in envelope.ciphertext
    assert service.decrypt(envelope, aad=b"tenant:resource:provider") == plaintext

    tampered = envelope.with_ciphertext(envelope.ciphertext[:-1] + b"x")
    with pytest.raises(SecretDecryptionError):
        service.decrypt(tampered, aad=b"tenant:resource:provider")


def test_secret_service_rejects_wrong_key_version_and_wrong_key():
    service = SecretService({1: b"a" * 32}, current_key_version=1)
    envelope = service.encrypt(b"secret", aad=b"scope")

    with pytest.raises(SecretDecryptionError):
        SecretService({2: b"b" * 32}, current_key_version=2).decrypt(
            envelope, aad=b"scope"
        )
    with pytest.raises(SecretDecryptionError):
        SecretService({1: b"b" * 32}, current_key_version=1).decrypt(
            envelope, aad=b"scope"
        )


def test_break_glass_wrong_secret_raises_classified_non_sensitive_error():
    service = break_glass.BreakGlassRecoveryService(
        CredentialHasher(b"c" * 32),
        recovery_secret_hash="0" * 64,
    )
    raw_secret = "never-disclose-this-recovery-secret"

    with pytest.raises(break_glass.BreakGlassDeniedError) as raised:
        service.recover(
            None,
            tenant_id=None,
            admin_user_id=None,
            recovery_secret=raw_secret,
            recovery_mechanism="hidden_prompt",
        )

    assert (
        raised.value.failure_category
        is break_glass.BreakGlassFailureCategory.INVALID_RECOVERY_SECRET
    )
    assert raw_secret not in str(raised.value)
