"""Backend-neutral runtime events with finite, identifier-free dimensions."""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

_FAMILIES = frozenset(
    {
        "redis",
        "rate",
        "concurrency",
        "circuit",
        "local_degraded",
        "recovery",
        "config_publication",
        "config_subscriber",
        "config_reconciliation",
        "pre_auth",
        "traffic",
    }
)
RESOURCE_KINDS = frozenset(
    {
        "application",
        "api_key",
        "service",
        "route",
        "llm_provider",
        "llm_target",
        "llm_model",
        "llm_alias",
        "model_price",
    }
)
_DIMENSIONS = {
    "operation": frozenset(
        {
            "ping",
            "acquire",
            "renew",
            "release",
            "eligibility",
            "success",
            "failure",
            "publish",
            "subscribe",
            "reconcile",
            "evaluate",
            "token_bucket",
            "other",
        }
    ),
    "outcome": frozenset(
        {
            "success",
            "failure",
            "allowed",
            "rejected",
            "error",
            "acquired",
            "renewed",
            "released",
            "missing",
            "closed",
            "probe_granted",
            "probe_denied",
            "applied",
            "stale",
            "dependency_error",
            "local_capacity_exhausted",
            "recovery_started",
            "redis_not_ready",
            "subscriber_not_ready",
            "config_not_reconciled",
            "final_redis_check_failed",
            "certificate_invalidated",
            "recovery_succeeded",
            "recovery_failed",
            "config_reconciliation_timeout",
            "subscriber_reconnected",
            "recovery_interrupted",
            "not_degraded",
            "recovery_in_progress",
            "pass_success",
            "pass_error",
            "empty_membership",
            "published",
            "partially_published",
            "publish_failed",
            "connected",
            "received",
            "reconnecting",
            "callback_failed",
            "other",
        }
    ),
    "target_kind": frozenset({"route", "provider_target", "other"}),
    "resource_kind": RESOURCE_KINDS | {"other"},
    "store": frozenset({"rate", "pre_auth", "concurrency", "circuit"}),
    "mode": frozenset({"NORMAL", "DEGRADED_REDIS", "RECOVERING_REDIS"}),
    "state": frozenset(
        {"closed", "open", "half_open", "degraded_half_open", "error", "other"}
    ),
    "scope_type": frozenset(
        {
            "API_KEY",
            "ADMIN_TOKEN",
            "ROUTE",
            "LLM_ALIAS",
            "LLM_MODEL",
            "SERVICE",
            "PROVIDER_TARGET",
            "other",
        }
    ),
}


def normalize_category(value: object, allowed: frozenset[str] | set[str]) -> str:
    return value if isinstance(value, str) and value in allowed else "other"


def bounded_dimension(name: str, value: object) -> str:
    allowed = _DIMENSIONS.get(name)
    if allowed is None:
        raise ValueError("Telemetry dimension is invalid.")
    normalized = normalize_category(value, allowed)
    if normalized not in allowed:
        raise ValueError("Telemetry dimension is invalid.")
    return normalized


@dataclass(frozen=True, slots=True)
class BoundedEvent:
    family: str
    dimensions: Mapping[str, str]
    latency_ms: float | None = None

    def __post_init__(self) -> None:
        if self.family not in _FAMILIES or any(
            name not in _DIMENSIONS or value not in _DIMENSIONS[name]
            for name, value in self.dimensions.items()
        ):
            raise ValueError("Telemetry dimensions are not bounded.")
        if self.latency_ms is not None and (
            type(self.latency_ms) not in (int, float)
            or not math.isfinite(self.latency_ms)
            or self.latency_ms < 0
        ):
            raise ValueError("Telemetry latency is invalid.")
        object.__setattr__(self, "dimensions", MappingProxyType(dict(self.dimensions)))


class BoundedTelemetry(Protocol):
    def record(self, event: BoundedEvent) -> None: ...


class LoggingBoundedTelemetry:
    def record(self, event: BoundedEvent) -> None:
        logging.getLogger("app.redis.telemetry").info(
            "Runtime telemetry event",
            extra={
                "event_family": event.family,
                **event.dimensions,
                **(
                    {"latency_ms": event.latency_ms}
                    if event.latency_ms is not None
                    else {}
                ),
            },
        )


class RecordingTelemetry:
    """Minimal in-memory semantic recorder for tests and adapters."""

    def __init__(self) -> None:
        self.events: list[BoundedEvent] = []

    def record(self, event: BoundedEvent) -> None:
        self.events.append(event)
