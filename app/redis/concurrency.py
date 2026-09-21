from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

import uuid_utils.compat as uuid_utils
from redis.exceptions import NoScriptError

from app.persistence.models.enums import RateScopeType
from app.redis.namespace import REDIS_NAMESPACE_PREFIX
from app.redis.runtime import RedisRuntime

logger = logging.getLogger(__name__)
_MAX_PG_INTEGER = 2**31 - 1
_SCRIPTS = {
    action: (
        Path(__file__).with_name("scripts") / f"semaphore_{action}_v1.lua"
    ).read_text(encoding="utf-8")
    for action in ("acquire", "renew", "release")
}
_ORDER = {
    RateScopeType.API_KEY: 0,
    RateScopeType.ADMIN_TOKEN: 0,
    RateScopeType.ROUTE: 1,
    RateScopeType.LLM_ALIAS: 1,
    RateScopeType.LLM_MODEL: 1,
    RateScopeType.SERVICE: 2,
    RateScopeType.PROVIDER_TARGET: 2,
}


class ConcurrencyPolicyError(ValueError):
    """A resolved concurrency policy is invalid."""


class ConcurrencyDependencyError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Concurrency dependency is unavailable.")


@dataclass(frozen=True, slots=True)
class ResolvedConcurrencyPolicy:
    policy_id: UUID
    tenant_id: UUID
    scope_type: RateScopeType
    scope_id: UUID
    max_concurrency: int

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, UUID)
            for value in (self.policy_id, self.tenant_id, self.scope_id)
        ):
            raise ConcurrencyPolicyError("Policy identifiers must be UUID values.")
        if not isinstance(self.scope_type, RateScopeType):
            raise ConcurrencyPolicyError("Policy scope type is invalid.")
        if (
            type(self.max_concurrency) is not int
            or not 0 < self.max_concurrency <= _MAX_PG_INTEGER
        ):
            raise ConcurrencyPolicyError(
                "Policy concurrency must be a positive PostgreSQL integer."
            )


@dataclass(frozen=True, slots=True)
class AcquireResult:
    acquired: bool
    lease_id: UUID | None


@dataclass(frozen=True, slots=True)
class RenewResult:
    renewed: bool


@dataclass(frozen=True, slots=True)
class ReleaseResult:
    released: bool


class ConcurrencyTelemetry(Protocol):
    def record(
        self, operation: str, outcome: str, scope_types: tuple[str, ...]
    ) -> None: ...


class LoggingConcurrencyTelemetry:
    def record(
        self, operation: str, outcome: str, scope_types: tuple[str, ...]
    ) -> None:
        logger.info(
            "Redis concurrency operation",
            extra={
                "operation_class": operation,
                "operation_outcome": outcome,
                "scope_types": scope_types,
            },
        )


def semaphore_key(policy: ResolvedConcurrencyPolicy) -> str:
    return f"{REDIS_NAMESPACE_PREFIX}sem:{{{policy.tenant_id}}}:{policy.scope_type.value}:{policy.scope_id}"


class RedisConcurrencySemaphore:
    def __init__(
        self,
        runtime: RedisRuntime,
        lease_duration_ms: int,
        *,
        telemetry: ConcurrencyTelemetry | None = None,
    ) -> None:
        if (
            type(lease_duration_ms) is not int
            or not 5000 <= lease_duration_ms <= 120000
        ):
            raise ConcurrencyPolicyError("Lease duration is outside the frozen range.")
        self._runtime = runtime
        self._duration = lease_duration_ms
        self._telemetry = telemetry or LoggingConcurrencyTelemetry()
        self._sha: dict[str, str] = {}
        self._script_lock = asyncio.Lock()

    async def acquire(
        self, policies: Sequence[ResolvedConcurrencyPolicy]
    ) -> AcquireResult:
        ordered = self._ordered(policies)
        if not ordered:
            return AcquireResult(True, None)
        lease_id = uuid_utils.uuid7()
        result = await self._execute(
            "acquire", ordered, lease_id, [p.max_concurrency for p in ordered]
        )
        return AcquireResult(bool(result), lease_id if result else None)

    async def renew(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> RenewResult:
        ordered = self._ordered(policies)
        self._check_lease_id(lease_id)
        if not ordered:
            return RenewResult(False)
        return RenewResult(bool(await self._execute("renew", ordered, lease_id)))

    async def release(
        self, policies: Sequence[ResolvedConcurrencyPolicy], lease_id: UUID
    ) -> ReleaseResult:
        ordered = self._ordered(policies)
        self._check_lease_id(lease_id)
        if not ordered:
            return ReleaseResult(False)
        return ReleaseResult(bool(await self._execute("release", ordered, lease_id)))

    async def _execute(
        self,
        action: str,
        ordered: tuple[ResolvedConcurrencyPolicy, ...],
        lease_id: UUID,
        extra: list[int] | None = None,
    ) -> int:
        keys = [semaphore_key(policy) for policy in ordered]
        arguments = [self._duration, str(lease_id), *(extra or [])]
        try:
            sha = await self._load(action)
            try:
                result = await self._runtime.client.evalsha(
                    sha, len(keys), *keys, *arguments
                )
            except NoScriptError:
                sha = await self._load(action, force=True)
                result = await self._runtime.client.evalsha(
                    sha, len(keys), *keys, *arguments
                )
            if type(result) is not int or result not in (0, 1):
                raise ConcurrencyDependencyError()
        except Exception as exc:
            self._record(action, "error", ordered)
            raise ConcurrencyDependencyError() from exc
        outcomes = {
            "acquire": ("rejected", "acquired"),
            "renew": ("missing", "renewed"),
            "release": ("missing", "released"),
        }
        self._record(action, outcomes[action][result], ordered)
        return result

    async def _load(self, action: str, *, force: bool = False) -> str:
        async with self._script_lock:
            if force or action not in self._sha:
                self._sha[action] = await self._runtime.client.script_load(
                    _SCRIPTS[action]
                )
            return self._sha[action]

    @staticmethod
    def _ordered(
        policies: Sequence[ResolvedConcurrencyPolicy],
    ) -> tuple[ResolvedConcurrencyPolicy, ...]:
        if not policies:
            return ()
        if any(
            not isinstance(policy, ResolvedConcurrencyPolicy) for policy in policies
        ):
            raise ConcurrencyPolicyError(
                "Policies must be resolved concurrency policies."
            )
        tenant = policies[0].tenant_id
        if any(policy.tenant_id != tenant for policy in policies):
            raise ConcurrencyPolicyError("Policies must belong to one tenant.")
        keys = [semaphore_key(policy) for policy in policies]
        if len(keys) != len(set(keys)):
            raise ConcurrencyPolicyError("Duplicate semaphore scopes are invalid.")
        return tuple(
            sorted(
                policies,
                key=lambda policy: (_ORDER[policy.scope_type], policy.policy_id.int),
            )
        )

    @staticmethod
    def _check_lease_id(lease_id: UUID) -> None:
        if not isinstance(lease_id, UUID):
            raise ConcurrencyPolicyError("Lease ID must be a UUID.")

    def _record(
        self, action: str, outcome: str, policies: tuple[ResolvedConcurrencyPolicy, ...]
    ) -> None:
        self._telemetry.record(
            action, outcome, tuple(sorted({p.scope_type.value for p in policies}))
        )
