"""Internal resolution only. No public route, body read, protection or upstream."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import ValidationError

from app.core.errors import GatewayHttpError, authorization_error, resource_not_found
from app.normal_api.auth import GatewayAuthenticator, GatewayIdentity
from app.normal_api.body_limit_constants import (
    DEFAULT_REQUEST_BODY_LIMIT_BYTES,
    MAX_REQUEST_BODY_LIMIT_BYTES,
)
from app.normal_api.current_state import ConfigurationSnapshot, NormalApiCurrentState
from app.normal_api.ingress import (
    CanonicalIngress,
    canonical_ingress,
    pattern_matches,
    unavailable,
)
from app.schemas.control_plane import RouteHeaderPolicy

METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")


@dataclass(frozen=True, slots=True, repr=False)
class ResolvedNormalRequest:
    request_id: UUID
    identity: GatewayIdentity
    snapshot: ConfigurationSnapshot
    ingress: CanonicalIngress
    service: Mapping
    route: Mapping
    effective_request_body_limit_bytes: int


def validate_configuration(service: Mapping, route: Mapping):
    if service.get("slug") in {"v1", "admin", "health"}:
        unavailable()
    required = (
        "connect_timeout_seconds",
        "pool_timeout_seconds",
        "write_timeout_seconds",
        "read_idle_timeout_seconds",
        "pre_response_timeout_seconds",
        "request_body_limit_bytes",
    )
    if any(
        type(service.get(name)) is not int or service[name] <= 0 for name in required
    ):
        unavailable()
    if service["request_body_limit_bytes"] > MAX_REQUEST_BODY_LIMIT_BYTES:
        unavailable()
    if (
        not isinstance(service.get("upstream_base_url"), str)
        or not service["upstream_base_url"]
    ):
        unavailable()
    try:
        parsed = urlsplit(service["upstream_base_url"])
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or "?" in service["upstream_base_url"]
            or "#" in service["upstream_base_url"]
        ):
            unavailable()
        _port = parsed.port  # Validate URL port syntax without DNS or destination I/O.
    except ValueError:
        unavailable()
    if type(route.get("priority")) is not int or route["priority"] < 0:
        unavailable()
    timeout = route.get("timeout_ms")
    if timeout is not None and (type(timeout) is not int or timeout <= 0):
        unavailable()
    try:
        if route.get("header_policy") is not None:
            RouteHeaderPolicy.model_validate(route["header_policy"])
    except ValidationError:
        unavailable()


class NormalApiResolver:
    def __init__(
        self,
        authenticator: GatewayAuthenticator,
        current_state: NormalApiCurrentState,
        *,
        deployment_body_limit_bytes: int = DEFAULT_REQUEST_BODY_LIMIT_BYTES,
    ):
        if (
            type(deployment_body_limit_bytes) is not int
            or not 1 <= deployment_body_limit_bytes <= MAX_REQUEST_BODY_LIMIT_BYTES
        ):
            raise ValueError("Invalid deployment body limit.")
        self.authenticator = authenticator
        self.current_state = current_state
        self.deployment_body_limit_bytes = deployment_body_limit_bytes

    async def resolve(self, scope: dict, *, request_id: UUID) -> ResolvedNormalRequest:
        """request_id is trusted middleware correlation, never a caller header."""
        identity = await self.authenticator.authenticate(scope)
        ingress = canonical_ingress(scope)
        try:
            # A try-now boundary, not a reconciliation timeout/cadence. These
            # operations do no persistence I/O; a contended lock fails closed.
            async with asyncio.timeout(0):
                await self.current_state.register(identity.tenant_id)
                snapshot = await self.current_state.capture(identity.tenant_id)
        except TimeoutError:
            unavailable()
        if (
            snapshot.tenant_id != identity.tenant_id
            or snapshot.projection.tenant_id != identity.tenant_id
        ):
            unavailable()
        services = [
            row
            for row in snapshot.projection.services
            if row.get("tenant_id") == identity.tenant_id
            and row.get("slug") == ingress.service_slug
            and row.get("status") == "ACTIVE"
        ]
        if not services:
            resource_not_found()
        if len(services) != 1:
            unavailable()
        service = services[0]
        paths = [
            row
            for row in snapshot.projection.routes
            if row.get("tenant_id") == identity.tenant_id
            and row.get("service_id") == service["id"]
            and row.get("status") == "ACTIVE"
            and row.get("method") in METHODS
            and pattern_matches(row.get("path_pattern"), ingress.remaining_segments)
        ]
        matches = [row for row in paths if row["method"] == scope.get("method")]
        if not paths:
            resource_not_found()
        if not matches:
            # R19 discloses only executable method inventory. Invalid required
            # configuration must not become an apparently usable Allow entry.
            for candidate in paths:
                validate_configuration(service, candidate)
            allow = ", ".join(
                method
                for method in METHODS
                if any(row["method"] == method for row in paths)
            )
            raise GatewayHttpError(
                405, "invalid_request", "Invalid request.", headers={"Allow": allow}
            )
        if len(matches) != 1:
            unavailable()
        route = matches[0]
        if not (
            {("SERVICE", service["id"]), ("ROUTE", route["id"])} & identity.permissions
        ):
            authorization_error()
        validate_configuration(service, route)
        return ResolvedNormalRequest(
            request_id,
            identity,
            snapshot,
            ingress,
            service,
            route,
            min(self.deployment_body_limit_bytes, service["request_body_limit_bytes"]),
        )
