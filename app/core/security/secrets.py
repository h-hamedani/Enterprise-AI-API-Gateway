from __future__ import annotations

import os
from dataclasses import dataclass, replace

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class SecretDecryptionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class EncryptedSecret:
    key_version: int
    ciphertext: bytes

    def with_ciphertext(self, ciphertext: bytes) -> EncryptedSecret:
        return replace(self, ciphertext=ciphertext)


class SecretService:
    _NONCE_SIZE = 12

    def __init__(self, keys: dict[int, bytes], *, current_key_version: int) -> None:
        if current_key_version not in keys:
            raise ValueError("Current encryption key version is unavailable.")
        if any(len(key) != 32 for key in keys.values()):
            raise ValueError("AES-256-GCM keys must be exactly 32 bytes.")
        self._keys = dict(keys)
        self._current_key_version = current_key_version

    def encrypt(self, plaintext: bytes, *, aad: bytes) -> EncryptedSecret:
        nonce = os.urandom(self._NONCE_SIZE)
        ciphertext = AESGCM(self._keys[self._current_key_version]).encrypt(
            nonce, plaintext, aad
        )
        return EncryptedSecret(
            key_version=self._current_key_version,
            ciphertext=nonce + ciphertext,
        )

    def decrypt(self, envelope: EncryptedSecret, *, aad: bytes) -> bytes:
        key = self._keys.get(envelope.key_version)
        if key is None or len(envelope.ciphertext) <= self._NONCE_SIZE:
            raise SecretDecryptionError("Secret cannot be decrypted.")
        nonce = envelope.ciphertext[: self._NONCE_SIZE]
        ciphertext = envelope.ciphertext[self._NONCE_SIZE :]
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, aad)
        except InvalidTag as exc:
            raise SecretDecryptionError("Secret cannot be decrypted.") from exc
