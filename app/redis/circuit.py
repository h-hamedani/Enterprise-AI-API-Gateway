"""Distributed, Redis-owned circuit coordination; no provider invocation."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import UUID

import uuid_utils.compat as uuid_utils
from redis.exceptions import NoScriptError

from app.core.config import Settings
from app.persistence.models.llm_registry import LlmModel, LlmProviderTarget
from app.persistence.models.normal_api import NormalApiRoute, NormalApiService
from app.redis.namespace import REDIS_NAMESPACE_PREFIX
from app.redis.redis_failure import is_redis_availability_failure
from app.redis.runtime import RedisRuntime

logger = logging.getLogger(__name__)
_MAX_SAFE = 2**53 - 1
_SCRIPT = (Path(__file__).with_name("scripts") / "circuit_v1.lua").read_text(
    encoding="utf-8"
)


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"
    DEGRADED_HALF_OPEN = "DEGRADED_HALF_OPEN"


class CircuitContractError(ValueError):
    """Invalid circuit identity, configuration, or outcome token."""


class CircuitDependencyError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Circuit dependency is unavailable.")


class CircuitStateError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Circuit state cannot transition safely.")


class CircuitProtocolError(RuntimeError):
    """The Redis result did not match the frozen circuit protocol."""

    def __init__(self) -> None:
        super().__init__("Circuit response is invalid.")


@dataclass(frozen=True, slots=True)
class CircuitConfig:
    failure_threshold: int
    failure_window_ms: int
    open_duration_ms: int
    half_open_probe_limit: int
    successes_to_close: int
    probe_lease_duration_ms: int

    def __post_init__(self) -> None:
        for name in (
            "failure_threshold",
            "failure_window_ms",
            "open_duration_ms",
            "probe_lease_duration_ms",
        ):
            value = getattr(self, name)
            if type(value) is not int or not 0 < value <= _MAX_SAFE:
                raise CircuitContractError("Circuit configuration is invalid.")
        if (
            type(self.half_open_probe_limit) is not int
            or self.half_open_probe_limit != 1
            or type(self.successes_to_close) is not int
            or self.successes_to_close != 1
        ):
            raise CircuitContractError("V1 requires one probe and one close success.")

    @classmethod
    def from_settings(cls, settings: Settings) -> CircuitConfig:
        return cls(
            failure_threshold=settings.circuit_failure_threshold,
            failure_window_ms=settings.circuit_failure_window_ms,
            open_duration_ms=settings.circuit_open_duration_ms,
            half_open_probe_limit=settings.circuit_half_open_probe_limit,
            successes_to_close=settings.circuit_successes_to_close,
            probe_lease_duration_ms=settings.circuit_probe_lease_duration_ms,
        )


@dataclass(frozen=True, slots=True, init=False)
class CircuitIdentity:
    tenant_id: UUID
    target_kind: str
    target_id: UUID
    dimension_id: UUID

    @classmethod
    def normal(
        cls, route: NormalApiRoute, service: NormalApiService
    ) -> CircuitIdentity:
        if (
            not isinstance(route, NormalApiRoute)
            or not isinstance(service, NormalApiService)
            or route.tenant_id != service.tenant_id
            or route.service_id != service.id
        ):
            raise CircuitContractError("Normal circuit target relationship is invalid.")
        return cls._create(route.tenant_id, "route", route.id, service.id)

    @classmethod
    def llm(cls, target: LlmProviderTarget, model: LlmModel) -> CircuitIdentity:
        if (
            not isinstance(target, LlmProviderTarget)
            or not isinstance(model, LlmModel)
            or target.tenant_id != model.tenant_id
            or model.provider_target_id != target.id
        ):
            raise CircuitContractError("LLM circuit target relationship is invalid.")
        return cls._create(target.tenant_id, "provider_target", target.id, model.id)

    @classmethod
    def _create(
        cls, tenant_id: UUID, target_kind: str, target_id: UUID, dimension_id: UUID
    ) -> CircuitIdentity:
        if any(
            not isinstance(value, UUID)
            for value in (tenant_id, target_id, dimension_id)
        ):
            raise CircuitContractError("Circuit identity is invalid.")
        identity = object.__new__(cls)
        object.__setattr__(identity, "tenant_id", tenant_id)
        object.__setattr__(identity, "target_kind", target_kind)
        object.__setattr__(identity, "target_id", target_id)
        object.__setattr__(identity, "dimension_id", dimension_id)
        return identity


def circuit_key(identity: CircuitIdentity) -> str:
    if not isinstance(identity, CircuitIdentity):
        raise CircuitContractError("Circuit identity is invalid.")
    return (
        f"{REDIS_NAMESPACE_PREFIX}cb:{{{identity.tenant_id}}}:"
        f"{identity.target_kind}:{identity.target_id}:{identity.dimension_id}"
    )


@dataclass(frozen=True, slots=True)
class NormalEligibilityToken:
    identity: CircuitIdentity
    incarnation_id: UUID
    generation: int


@dataclass(frozen=True, slots=True)
class EligibilityResult:
    eligible: bool
    state: CircuitState
    normal_token: NormalEligibilityToken | None = None
    probe_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class OutcomeResult:
    applied: bool
    resulting_state: CircuitState


class CircuitTelemetry(Protocol):
    def record(
        self, operation: str, outcome: str, state: str, target_kind: str
    ) -> None: ...


class LoggingCircuitTelemetry:
    def record(
        self, operation: str, outcome: str, state: str, target_kind: str
    ) -> None:
        logger.info(
            "Redis circuit operation",
            extra={
                "operation_class": operation,
                "operation_outcome": outcome,
                "circuit_state": state,
                "target_kind": target_kind,
            },
        )


class RedisCircuitStore:
    def __init__(
        self,
        runtime: RedisRuntime,
        config: CircuitConfig,
        *,
        telemetry: CircuitTelemetry | None = None,
    ) -> None:
        if not isinstance(config, CircuitConfig):
            raise CircuitContractError("Circuit configuration is invalid.")
        self._runtime = runtime
        self._config = config
        self._telemetry = telemetry or LoggingCircuitTelemetry()
        self._sha: str | None = None
        self._script_lock = asyncio.Lock()

    async def check_or_claim_eligibility(
        self, identity: CircuitIdentity
    ) -> EligibilityResult:
        candidate = uuid_utils.uuid7()
        probe = uuid_utils.uuid7()
        row = await self._execute("eligibility", identity, "", "", 0, candidate, probe)
        state = CircuitState(row[1])
        if row[2] == "normal":
            result = EligibilityResult(
                True,
                state,
                normal_token=NormalEligibilityToken(
                    identity, UUID(row[3]), int(row[4])
                ),
            )
        elif row[2] == "probe":
            result = EligibilityResult(True, state, probe_id=UUID(row[3]))
        else:
            result = EligibilityResult(False, state)
        self._telemetry.record(
            "eligibility",
            "probe_granted"
            if result.probe_id
            else "closed"
            if result.eligible
            else "probe_denied",
            state.value.lower(),
            identity.target_kind,
        )
        return result

    async def record_success(
        self, identity: CircuitIdentity, token: NormalEligibilityToken | UUID
    ) -> OutcomeResult:
        return await self._outcome("success", identity, token)

    async def record_failure(
        self, identity: CircuitIdentity, token: NormalEligibilityToken | UUID
    ) -> OutcomeResult:
        return await self._outcome("failure", identity, token)

    async def _outcome(
        self,
        action: str,
        identity: CircuitIdentity,
        token: NormalEligibilityToken | UUID,
    ) -> OutcomeResult:
        if isinstance(token, NormalEligibilityToken):
            if token.identity != identity or not 1 <= token.generation <= _MAX_SAFE:
                raise CircuitContractError("Circuit token identity is invalid.")
            mode, token_id, generation = (
                "normal",
                str(token.incarnation_id),
                token.generation,
            )
        elif isinstance(token, UUID):
            mode, token_id, generation = "probe", str(token), 0
        else:
            raise CircuitContractError("Circuit outcome token is invalid.")
        row = await self._execute(
            action,
            identity,
            mode,
            token_id,
            generation,
            uuid_utils.uuid7(),
            uuid_utils.uuid7(),
        )
        result = OutcomeResult(row[2] == "applied", CircuitState(row[1]))
        self._telemetry.record(
            action,
            "applied" if result.applied else "stale",
            result.resulting_state.value.lower(),
            identity.target_kind,
        )
        return result

    async def _execute(
        self,
        action: str,
        identity: CircuitIdentity,
        mode: str,
        token_id: str,
        generation: int,
        candidate: UUID,
        probe: UUID,
    ) -> list[str]:
        key = circuit_key(identity)
        args = (
            action,
            mode,
            token_id,
            generation,
            str(candidate),
            str(probe),
            self._config.failure_threshold,
            self._config.failure_window_ms,
            self._config.open_duration_ms,
            self._config.probe_lease_duration_ms,
        )
        try:
            sha = await self._load()
            try:
                row = await self._runtime.client.evalsha(
                    sha, 2, key, f"{key}:failures", *args
                )
            except NoScriptError:
                sha = await self._load(force=True)
                row = await self._runtime.client.evalsha(
                    sha, 2, key, f"{key}:failures", *args
                )
            if not isinstance(row, list) or len(row) < 3:
                raise CircuitProtocolError()
            if row[0] == "overflow":
                raise CircuitStateError()
            if row[0] == "invalid":
                raise CircuitStateError()
            if row[0] != "ok":
                raise CircuitProtocolError()
            self._validate_row(action, row)
            return row
        except CircuitStateError:
            self._telemetry.record(action, "error", "half_open", identity.target_kind)
            raise
        except Exception as exc:
            self._telemetry.record(action, "error", "error", identity.target_kind)
            if is_redis_availability_failure(exc):
                raise CircuitDependencyError() from None
            raise

    async def _load(self, *, force: bool = False) -> str:
        async with self._script_lock:
            if force or self._sha is None:
                self._sha = await self._runtime.client.script_load(_SCRIPT)
            return self._sha

    @staticmethod
    def _validate_row(action: str, row: list[str]) -> None:
        if any(type(value) is not str for value in row):
            raise CircuitProtocolError()
        if row[1] not in ("CLOSED", "OPEN", "HALF_OPEN"):
            raise CircuitProtocolError()
        if action == "eligibility":
            if row[2] == "normal":
                if len(row) != 5 or row[1] != "CLOSED":
                    raise CircuitProtocolError()
                try:
                    parsed = UUID(row[3])
                    generation = int(row[4])
                except (ValueError, TypeError):
                    raise CircuitProtocolError() from None
                if str(parsed) != row[3]:
                    raise CircuitProtocolError()
                if str(generation) != row[4] or not 1 <= generation <= _MAX_SAFE:
                    raise CircuitProtocolError()
            elif row[2] == "probe":
                if len(row) != 4 or row[1] != "HALF_OPEN":
                    raise CircuitProtocolError()
                try:
                    parsed = UUID(row[3])
                except (ValueError, TypeError):
                    raise CircuitProtocolError() from None
                if str(parsed) != row[3]:
                    raise CircuitProtocolError()
            elif row[2] != "denied" or len(row) != 3:
                raise CircuitProtocolError()
        elif len(row) != 3 or row[2] not in ("applied", "stale"):
            raise CircuitProtocolError()
