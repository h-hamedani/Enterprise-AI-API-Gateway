from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass

from fastapi import Request

from app.control_plane.auth import (
    AdminAuthenticationError,
    AdminAuthenticator,
    AdminContext,
)
from app.core.config import get_settings
from app.core.errors import authentication_error
from app.core.security.credentials import CredentialHasher

_ADMIN_TOKEN_PATTERN = re.compile(r"^adm_[A-Za-z0-9_-]{43}$")


@dataclass(frozen=True, slots=True)
class AdminPreAuthContext:
    client_ip: str
    route_class: str = "ADMIN_AUTH_PROTECTED"


def admin_pre_auth_context(request: Request) -> AdminPreAuthContext:
    client_ip = request.client.host if request.client is not None else "unknown"
    return AdminPreAuthContext(client_ip=client_ip)


def _extract_admin_token(request: Request) -> str:
    authorization = request.headers.get("Authorization")
    if authorization is None:
        authentication_error()
    scheme, separator, raw_token = authorization.partition(" ")
    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not _ADMIN_TOKEN_PATTERN.fullmatch(raw_token)
    ):
        authentication_error()
    return raw_token


def _admin_authenticator(request: Request) -> AdminAuthenticator:
    configured = getattr(request.app.state, "admin_authenticator", None)
    if configured is not None:
        return configured
    encoded = get_settings().credential_hmac_secret
    if encoded is None:
        authentication_error()
    try:
        secret = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        authentication_error()
    return AdminAuthenticator(CredentialHasher(secret))


async def require_admin_context(request: Request) -> AdminContext:
    raw_token = _extract_admin_token(request)
    request_id = request.state.request_id
    try:
        return await _admin_authenticator(request).authenticate(
            request.app.state.db_engine,
            raw_token=raw_token,
            request_id=request_id,
        )
    except AdminAuthenticationError:
        authentication_error()
