from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass

from fastapi import Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.control_plane.auth import (
    AdminAuthenticationError,
    AdminAuthenticator,
    AdminContext,
)
from app.control_plane.protection import ControlPlaneProtection
from app.core.config import get_settings
from app.core.errors import authentication_error
from app.core.security.credentials import CredentialHasher

_ADMIN_TOKEN_PATTERN = re.compile(r"^adm_[A-Za-z0-9_-]{43}$")
_ADMIN_BEARER = HTTPBearer(auto_error=False, scheme_name="AdminToken")
_ADMIN_CREDENTIALS_DEPENDENCY = Security(_ADMIN_BEARER)


@dataclass(frozen=True, slots=True)
class AdminPreAuthContext:
    client_ip: str
    route_class: str = "ADMIN_AUTH_PROTECTED"


def admin_pre_auth_context(request: Request) -> AdminPreAuthContext:
    client_ip = request.client.host if request.client is not None else "unknown"
    return AdminPreAuthContext(client_ip=client_ip)


def _extract_admin_token(
    credentials: HTTPAuthorizationCredentials | None,
) -> str:
    if credentials is None:
        authentication_error()
    if credentials.scheme.lower() != "bearer" or not _ADMIN_TOKEN_PATTERN.fullmatch(
        credentials.credentials
    ):
        authentication_error()
    return credentials.credentials


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


async def require_admin_context(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = _ADMIN_CREDENTIALS_DEPENDENCY,
) -> AdminContext:
    protection = getattr(request.app.state, "control_plane_protection", None)
    if isinstance(protection, ControlPlaneProtection):
        await protection.pre_auth(request)
    raw_token = _extract_admin_token(credentials)
    request_id = request.state.request_id
    try:
        context = await _admin_authenticator(request).authenticate(
            request.app.state.db_engine,
            raw_token=raw_token,
            request_id=request_id,
        )
        if isinstance(protection, ControlPlaneProtection):
            await protection.post_auth(request, context)
        return context
    except AdminAuthenticationError:
        authentication_error()
