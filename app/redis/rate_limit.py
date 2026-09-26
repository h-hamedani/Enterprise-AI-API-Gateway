from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol
from uuid import UUID

from redis.exceptions import NoScriptError

from app.persistence.models.enums import RateScopeType
from app.redis.namespace import REDIS_NAMESPACE_PREFIX
from app.redis.redis_failure import is_redis_availability_failure
from app.redis.runtime import RedisRuntime

logger = logging.getLogger(__name__)

MAX_EXACT_LUA_INTEGER = 2**53 - 1
MAX_POSTGRES_INTEGER = 2**31 - 1
_SCRIPT = (Path(__file__).with_name("scripts") / "token_bucket_v1.lua").read_text(
    encoding="utf-8"
)

_LAYER_ORDER = {
    RateScopeType.API_KEY: 0,
    RateScopeType.ADMIN_TOKEN: 0,
    RateScopeType.ROUTE: 1,
    RateScopeType.LLM_ALIAS: 1,
    RateScopeType.LLM_MODEL: 1,
    RateScopeType.SERVICE: 2,
    RateScopeType.PROVIDER_TARGET: 2,
}


class RateLimitPolicyError(ValueError):
    """A resolved policy cannot be evaluated safely."""


class RateLimitDependencyError(RuntimeError):
    """Redis could not complete the shared rate-limit decision."""

    def __init__(self) -> None:
        super().__init__("Rate-limit dependency is unavailable.")


class RateLimitProtocolError(RuntimeError):
    """The Redis result did not match the frozen token-bucket protocol."""

    def __init__(self) -> None:
        super().__init__("Rate-limit response is invalid.")


@dataclass(frozen=True, slots=True)
class ResolvedRatePolicy:
    policy_id: UUID
    tenant_id: UUID
    scope_type: RateScopeType
    scope_id: UUID
    requests_per_window: int
    window_seconds: int
    key_override: str | None = None
    degraded_factor: Decimal | None = None

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, UUID)
            for value in (self.policy_id, self.tenant_id, self.scope_id)
        ):
            raise RateLimitPolicyError("Policy identifiers must be UUID values.")
        if not isinstance(self.scope_type, RateScopeType):
            raise RateLimitPolicyError("Policy scope type is invalid.")
        if (
            type(self.requests_per_window) is not int
            or self.requests_per_window <= 0
            or self.requests_per_window > MAX_POSTGRES_INTEGER
            or type(self.window_seconds) is not int
            or self.window_seconds <= 0
            or self.window_seconds > MAX_POSTGRES_INTEGER
        ):
            raise RateLimitPolicyError("Policy rate values must be positive integers.")
        if self.capacity_units > MAX_EXACT_LUA_INTEGER:
            raise RateLimitPolicyError(
                "Policy rate values exceed the safe runtime bound."
            )
        if self.degraded_factor is not None:
            try:
                factor = Decimal(str(self.degraded_factor))
            except (InvalidOperation, ValueError) as exc:
                raise RateLimitPolicyError(
                    "Policy degraded factor is invalid."
                ) from exc
            if not factor.is_finite() or not 0 < factor <= 1:
                raise RateLimitPolicyError("Policy degraded factor is invalid.")

    @property
    def window_ms(self) -> int:
        return self.window_seconds * 1000

    @property
    def capacity_units(self) -> int:
        return self.requests_per_window * self.window_ms


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    retry_after_ms: int


@dataclass(frozen=True, slots=True)
class RateLimitTelemetryEvent:
    outcome: str
    scope_types: tuple[str, ...]


class RateLimitTelemetry(Protocol):
    def record(self, event: RateLimitTelemetryEvent) -> None: ...


class LoggingRateLimitTelemetry:
    def record(self, event: RateLimitTelemetryEvent) -> None:
        logger.info(
            "Redis rate-limit evaluation",
            extra={
                "limiter_operation": "token_bucket",
                "limiter_outcome": event.outcome,
                "scope_types": event.scope_types,
            },
        )


def rate_limit_key(policy: ResolvedRatePolicy) -> str:
    if policy.key_override is not None:
        return policy.key_override
    return (
        f"{REDIS_NAMESPACE_PREFIX}rl:{{{policy.tenant_id}}}:"
        f"{policy.scope_type.value}:{policy.scope_id}"
    )


class RedisTokenBucket:
    def __init__(
        self,
        runtime: RedisRuntime,
        *,
        telemetry: RateLimitTelemetry | None = None,
    ) -> None:
        self._runtime = runtime
        self._telemetry = telemetry or LoggingRateLimitTelemetry()
        self._script_sha: str | None = None
        self._script_lock = asyncio.Lock()

    async def evaluate(self, policies: Sequence[ResolvedRatePolicy]) -> RateLimitResult:
        ordered = self._validate_and_order(policies)
        if not ordered:
            return RateLimitResult(True, 0)

        keys = [rate_limit_key(policy) for policy in ordered]
        arguments: list[int] = []
        for policy in ordered:
            arguments.extend((policy.requests_per_window, policy.window_ms))

        try:
            sha = await self._load_script()
            try:
                response = await self._runtime.client.evalsha(
                    sha, len(keys), *keys, *arguments
                )
            except NoScriptError:
                sha = await self._load_script(force=True)
                response = await self._runtime.client.evalsha(
                    sha, len(keys), *keys, *arguments
                )
            result = self._parse_result(response)
        except Exception as exc:
            self._record("error", ordered)
            if is_redis_availability_failure(exc):
                raise RateLimitDependencyError() from None
            raise

        self._record("allowed" if result.allowed else "rejected", ordered)
        return result

    async def _load_script(self, *, force: bool = False) -> str:
        async with self._script_lock:
            if self._script_sha is None or force:
                self._script_sha = await self._runtime.client.script_load(_SCRIPT)
            return self._script_sha

    @staticmethod
    def _validate_and_order(
        policies: Sequence[ResolvedRatePolicy],
    ) -> tuple[ResolvedRatePolicy, ...]:
        if not policies:
            return ()
        tenant_id = policies[0].tenant_id
        if any(policy.tenant_id != tenant_id for policy in policies):
            raise RateLimitPolicyError("All policies must belong to one tenant.")
        keys = [rate_limit_key(policy) for policy in policies]
        if len(keys) != len(set(keys)):
            raise RateLimitPolicyError("Duplicate policy scopes are invalid.")
        return tuple(
            sorted(
                policies,
                key=lambda policy: (
                    _LAYER_ORDER[policy.scope_type],
                    policy.policy_id.int,
                ),
            )
        )

    @staticmethod
    def _parse_result(response) -> RateLimitResult:
        if not isinstance(response, (list, tuple)) or len(response) != 2:
            raise RateLimitProtocolError()
        if any(type(value) is not int for value in response):
            raise RateLimitProtocolError()
        allowed, retry_after_ms = response
        if allowed not in (0, 1) or retry_after_ms < 0:
            raise RateLimitProtocolError()
        return RateLimitResult(bool(allowed), retry_after_ms)

    def _record(self, outcome: str, policies: Sequence[ResolvedRatePolicy]) -> None:
        self._telemetry.record(
            RateLimitTelemetryEvent(
                outcome=outcome,
                scope_types=tuple(
                    sorted({policy.scope_type.value for policy in policies})
                ),
            )
        )
