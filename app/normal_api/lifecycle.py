"""Internal R7A/R8/R9 ownership; no HTTP transport or public dispatch endpoint.

Hooks are required integration boundaries, not default forwarding implementations.
All synchronous observations execute on the request's event loop (no await at the
decision fence). The request owns its children; at cutoff the supervisor retains
unfinished work rather than claiming every child has terminated.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from app.core.errors import GatewayHttpError
from app.normal_api.supervisor import LifecycleScope
from app.persistence.models.enums import RateScopeType
from app.persistence.models.normal_api import NormalApiRoute, NormalApiService
from app.redis.circuit import CircuitIdentity
from app.redis.concurrency import ConcurrencyDependencyError
from app.redis.guarded_renewal import (
    GuardedRenewalOutcome,
    RenewalAttemptStatus,
    RenewalDisposition,
    RenewalFailure,
    managed_renewal_cleanup,
)

logger = logging.getLogger(__name__)
_RECENT_RENEWAL_EVIDENCE = 2  # Representation bound, never an execution limit.
# Existing guarded backend values; None preserves absence of established phase.
_RENEWAL_PHASES = ("preparation", "write", "drain", "response", "local")


class End(StrEnum):
    DELIVERED = "delivered"
    REJECTED = "rejected"
    DISCONNECT = "disconnect"
    CANCELLED = "cancelled"
    SHUTDOWN = "shutdown"
    OWNERSHIP_LOST = "ownership_lost"
    OWNERSHIP_UNCERTAIN = "ownership_uncertain"
    INTERNAL = "internal"
    TIMEOUT = "timeout"


class CircuitClass(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    NO_COMPLETION = "no_completion"


class TimeoutKind(StrEnum):
    POOL = "pool"
    CONNECT = "connect"
    WRITE = "write"
    READ = "read"
    PRE_RESPONSE = "pre_response"


@dataclass(frozen=True, slots=True)
class UpstreamOutcome:
    """Only a definitive result supplied by the eventual invocation boundary."""

    classification: CircuitClass
    status: int | None = None
    error: GatewayHttpError | None = None

    def __post_init__(self):
        if not isinstance(self.classification, CircuitClass):
            raise TypeError("Invalid upstream classification.")
        if self.status is not None:
            expected = (
                CircuitClass.FAILURE
                if self.status in {500, 502, 503, 504}
                else CircuitClass.SUCCESS
            )
            if (
                type(self.status) is not int
                or not 200 <= self.status <= 599
                or self.classification != expected
                or self.error is not None
            ):
                raise ValueError("Invalid complete HTTP outcome.")
        elif self.classification == CircuitClass.SUCCESS:
            raise ValueError("HTTP success requires a complete final status.")

    @classmethod
    def complete_http(cls, status: int):
        if type(status) is not int or not 200 <= status <= 599:
            raise ValueError("Not a complete final HTTP response.")
        return cls(
            CircuitClass.FAILURE
            if status in {500, 502, 503, 504}
            else CircuitClass.SUCCESS,
            status,
        )

    @classmethod
    def transport_failure(cls):
        # Caller must establish an R2 upstream transport/protocol failure, never
        # a local exception, TLS trust failure, or downstream cancellation.
        return cls(
            CircuitClass.FAILURE,
            error=GatewayHttpError(
                503, "upstream_unavailable", "Upstream unavailable."
            ),
        )


@dataclass(frozen=True, slots=True, repr=False)
class Policies:
    rate: tuple
    concurrency: tuple

    def for_resolution(self, resolved):
        ids = {
            RateScopeType.API_KEY: resolved.identity.api_key_id,
            RateScopeType.ROUTE: resolved.route["id"],
            RateScopeType.SERVICE: resolved.service["id"],
        }
        for batch in (self.rate, self.concurrency):
            scopes = set()
            for policy in batch:
                if (
                    policy.tenant_id != resolved.identity.tenant_id
                    or policy.scope_type not in ids
                    or policy.scope_id != ids[policy.scope_type]
                    or policy.scope_type in scopes
                ):
                    raise ValueError("Protection policy ownership is invalid.")
                scopes.add(policy.scope_type)
        order = {kind: index for index, kind in enumerate(ids)}
        return Policies(
            tuple(sorted(self.rate, key=lambda p: order[p.scope_type])),
            tuple(sorted(self.concurrency, key=lambda p: order[p.scope_type])),
        )


@dataclass(frozen=True, slots=True)
class Hooks:
    policies: Callable[[Any], Awaitable[Policies]]
    body: Callable[[Any, Any], Awaitable[Any]]
    prepare: Callable[[Any, Any], Awaitable[Any]]
    execute: Callable[[Any, Any], Awaitable[None]]
    deliver: Callable[[Any], Awaitable[None]]


@dataclass(frozen=True, slots=True, repr=False)
class LifecycleResult:
    end: End
    committed: bool
    upstream: UpstreamOutcome | None
    error: GatewayHttpError | None
    cleanup_complete: bool
    outstanding_roles: tuple[str, ...]


@dataclass(frozen=True, slots=True, repr=False)
class RenewalEvidence:
    """Detached invocation evidence: no task, operation, socket or handle."""

    attempts: tuple
    outcome: GuardedRenewalOutcome | None
    cancelled: bool
    cleanup_pending: bool = False
    phase: str | None = None


@dataclass(frozen=True, slots=True, repr=False)
class RenewalEvidenceSummary:
    """Fixed-size history, independent of live work and physical obligations."""

    completed_invocations: int = 0
    uncertain_invocations: int = 0
    unknown_invocations: int = 0
    cancelled_invocations: int = 0
    cancellation_phase_counts: tuple = tuple(
        (phase, 0) for phase in (*_RENEWAL_PHASES, None)
    )
    failed_invocations: int = 0
    failure_counts: tuple = tuple((kind, 0) for kind in RenewalFailure)
    attempt_counts: tuple = tuple((status, 0) for status in RenewalAttemptStatus)
    disposition_counts: tuple = tuple((kind, 0) for kind in RenewalDisposition)
    first_uncertain_dispatch: float | None = None
    last_uncertain_dispatch: float | None = None
    first_dispatch: float | None = None
    last_dispatch: float | None = None
    recent: tuple[RenewalEvidence, ...] = ()
    issuing_backend: str | None = None
    issuing_generation: int | None = None
    live_invocations: int = 0
    physical_obligations: int = 0

    @property
    def unresolved(self):
        return bool(
            self.uncertain_invocations
            or self.live_invocations
            or self.physical_obligations
        )


class MonotonicClock:
    def now(self):
        return asyncio.get_running_loop().time()

    async def until(self, deadline):
        await asyncio.sleep(max(0, deadline - self.now()))


class ReceiveOwner:
    """One underlying receive caller, demand-fed during body, then disconnects.

    No read before the body stage. One message handoff, no unbounded body queue;
    the body hook remains responsible for R15 byte limits and full completion.
    """

    def __init__(self, receive, terminal):
        self._receive = receive
        self._terminal = terminal
        self._queue = asyncio.Queue(maxsize=1)
        self._consumed = asyncio.Event()
        self.task = None
        self.complete = False
        self._reading = False

    def start(self):
        if self.task is None:
            self.task = asyncio.create_task(self._pump())
            self.task.add_done_callback(self._finished)

    def _finished(self, task):
        if task.cancelled():
            self._terminal(End.INTERNAL)

    async def _pump(self):
        try:
            while True:
                message = await self._receive()
                if message["type"] == "http.disconnect":
                    self._terminal(End.DISCONNECT)
                    return
                if self.complete or message["type"] != "http.request":
                    self._terminal(End.INTERNAL)
                    return
                self._consumed.clear()
                await self._queue.put(message)
                await self._consumed.wait()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- receive boundary: no raw server errors
            self._terminal(End.INTERNAL)

    async def next_body_message(self):
        if self.complete or self._reading:
            raise RuntimeError("Body receive ownership violation.")
        self._reading = True
        try:
            message = await self._queue.get()
            self.complete = not message.get("more_body", False)
            self._consumed.set()
            return message
        finally:
            self._reading = False


class _RenewalGuard:
    """Read-only authorization on the request's owning event loop."""

    def __init__(self, owner):
        self.owner = owner

    def monotonic(self):
        value = self.owner.clock.now()
        if not math.isfinite(value) or (
            self.owner._last_time is not None and value < self.owner._last_time
        ):
            raise RuntimeError("Invalid monotonic clock.")
        return value

    def permit(self, sampled_at):
        owner = self.owner
        scope = owner.lifecycle_scope
        deadline = scope.deadline
        return bool(
            owner.end is None
            and not owner._stopping
            and owner.handle is not None
            and not scope._expired
            and not scope.notification.is_set()
            and owner._confidence_deadline is not None
            and sampled_at < owner._confidence_deadline
            and (deadline is None or sampled_at < deadline)
        )


class RequestLifecycle:
    """Single-use, internal orchestrator over the existing M3 facades.

    dependency_timeout_seconds is the existing deployment dependency bound, not
    a new cleanup setting. Required hooks cannot be mistaken for a public proxy.
    """

    def __init__(
        self,
        resolver,
        rate,
        concurrency,
        circuit,
        *,
        lease_duration_ms,
        dependency_timeout_seconds,
        clock=None,
    ):
        if (
            type(lease_duration_ms) is not int
            or not 5000 <= lease_duration_ms <= 120000
        ):
            raise ValueError("Invalid existing lease duration.")
        if (
            not math.isfinite(dependency_timeout_seconds)
            or dependency_timeout_seconds <= 0
        ):
            raise ValueError("Invalid existing dependency bound.")
        self.resolver, self.rate, self.concurrency, self.circuit = (
            resolver,
            rate,
            concurrency,
            circuit,
        )
        self.ttl = lease_duration_ms / 1000
        self.dependency_bound = dependency_timeout_seconds
        self.clock = clock or MonotonicClock()
        self.resolved = None
        self.policies = None
        self.handle = self.circuit_handle = self.circuit_identity = None
        self.upstream = None
        self.end = self.error = None
        self.committed = self.dispatched = False
        self._decision = None
        self._started = self._stopping = False
        self._work = self._renewal = self._cleanup_task = None
        self._completion = None
        self.receiver = None
        self.completion_result = None
        self.release_result = None
        self.release_attempted = False
        self.cleanup_failures = set()
        self._last_time = None
        self._confirmed_anchor = None
        self._confidence_deadline = None
        self._terminal_event = asyncio.Event()
        self.lifecycle_scope = None
        self._owned_tasks = {}
        self._renewal_obligations = {}
        self._renewal_resources = set()
        self._renewal_summary = RenewalEvidenceSummary()
        self.last_renewal_evidence = None
        self.last_renewal_outcome = None
        self._renewal_guard = _RenewalGuard(self)

    def _own(self, role, coroutine):
        task = asyncio.create_task(coroutine)
        self._owned_tasks[role] = task
        if self.lifecycle_scope is not None:
            self.lifecycle_scope.track(role, task)
        task.add_done_callback(self._consume_task_result)
        return task

    @staticmethod
    def _consume_task_result(task):
        if not task.cancelled():
            task.exception()

    @staticmethod
    def _renewal_snapshot(record, *, cancelled=False):
        outcome = (
            record
            if isinstance(record, GuardedRenewalOutcome)
            else getattr(record, "outcome", None)
        )
        phase = getattr(record, "phase", None)
        phase = phase if type(phase) is str and phase in _RENEWAL_PHASES else None
        return RenewalEvidence(
            tuple(getattr(record, "attempts", ())),
            outcome,
            cancelled,
            bool(getattr(record, "cleanup_pending", False)),
            phase,
        )

    def _retain_renewal(self, task):
        # A cancellation request is not completion. Keep actual live ownership.
        if not task.done():
            self._renewal_obligations[task] = None
            return
        if getattr(task, "_renewal_evidence_captured", False):
            return
        record = getattr(task, "guarded_renewal_operation", None)
        outcome = None
        failed = not task.cancelled() and task.exception() is not None
        if not task.cancelled() and task.exception() is None:
            candidate = task.result()
            if isinstance(candidate, GuardedRenewalOutcome):
                outcome = candidate
                self.last_renewal_outcome = outcome
                if record is None:
                    record = outcome
        evidence = self._renewal_snapshot(record, cancelled=task.cancelled())
        self.last_renewal_evidence = evidence
        outcome = evidence.outcome
        if evidence.cleanup_pending:
            # Retain real physical obligations independently of the completed Task.
            self._renewal_resources.add(record)
        previous = self._renewal_summary
        attempts = dict(previous.attempt_counts)
        dispositions = dict(previous.disposition_counts)
        failures = dict(previous.failure_counts)
        cancellation_phases = dict(previous.cancellation_phase_counts)
        if evidence.cancelled:
            cancellation_phases[evidence.phase] += 1
        for attempt in evidence.attempts:
            attempts[attempt.status] += 1
        if outcome is not None:
            dispositions[outcome.disposition] += 1
            if outcome.failure_category is not None:
                failures[outcome.failure_category] += 1
        uncertain = any(
            a.status is RenewalAttemptStatus.UNRESOLVED for a in evidence.attempts
        )
        unknown = (
            outcome is not None and outcome.disposition is RenewalDisposition.UNKNOWN
        )
        times = tuple(a.dispatched_at for a in evidence.attempts)
        uncertain_times = tuple(
            a.dispatched_at
            for a in evidence.attempts
            if a.status is RenewalAttemptStatus.UNRESOLVED
        )
        recent = previous.recent
        if uncertain or unknown:
            recent = (*recent, evidence)[-_RECENT_RENEWAL_EVIDENCE:]
        self._renewal_summary = replace(
            previous,
            completed_invocations=previous.completed_invocations + 1,
            uncertain_invocations=previous.uncertain_invocations + uncertain,
            unknown_invocations=previous.unknown_invocations + unknown,
            cancelled_invocations=previous.cancelled_invocations + task.cancelled(),
            cancellation_phase_counts=tuple(cancellation_phases.items()),
            failed_invocations=previous.failed_invocations + failed,
            failure_counts=tuple(failures.items()),
            first_uncertain_dispatch=previous.first_uncertain_dispatch
            if previous.first_uncertain_dispatch is not None
            else (min(uncertain_times) if uncertain_times else None),
            last_uncertain_dispatch=max(uncertain_times)
            if uncertain_times
            else previous.last_uncertain_dispatch,
            attempt_counts=tuple(attempts.items()),
            disposition_counts=tuple(dispositions.items()),
            first_dispatch=previous.first_dispatch
            if previous.first_dispatch is not None
            else (min(times) if times else None),
            last_dispatch=max(times) if times else previous.last_dispatch,
            recent=recent,
            issuing_backend=getattr(self.handle, "backend", None),
            issuing_generation=getattr(self.handle, "local_generation_id", None),
        )
        # Per-task marker makes repeated done callbacks idempotent without a Task archive.
        task._renewal_evidence_captured = True
        self._renewal_obligations.pop(task, None)
        for mapping in (
            self._owned_tasks,
            self.lifecycle_scope.tasks if self.lifecycle_scope else {},
        ):
            if mapping.get("renew_operation") is task:
                mapping.pop("renew_operation")

    @property
    def renewal_evidence(self):
        for task in tuple(self._renewal_obligations):
            self._retain_renewal(task)
        self._renewal_resources = {
            record for record in self._renewal_resources if record.cleanup_pending
        }
        return replace(
            self._renewal_summary,
            live_invocations=len(self._renewal_obligations),
            physical_obligations=len(self._renewal_resources),
        )

    @property
    def unresolved_renewals(self):
        summary = self.renewal_evidence
        resources = tuple(
            self._renewal_snapshot(record) for record in self._renewal_resources
        )
        return resources + tuple(
            e
            for e in summary.recent
            if any(a.status is RenewalAttemptStatus.UNRESOLVED for a in e.attempts)
        )

    @property
    def renewal_obligation(self):
        return "unresolved" if self.renewal_evidence.unresolved else "none"

    @property
    def release_obligation(self):
        if self.handle is None:
            return "none"
        if self.release_result is not None:
            return (
                "confirmed_success"
                if self.release_result.released
                else "confirmed_false"
            )
        if not self.release_attempted:
            return "not_attempted"
        return "attempted_unconfirmed"

    @property
    def circuit_obligation(self):
        if (
            self.upstream is None
            or self.upstream.classification == CircuitClass.NO_COMPLETION
        ):
            return "none"
        return "confirmed" if self.completion_result is not None else "unresolved"

    @property
    def outstanding_tasks(self):
        """Strongly retained, explicitly unfinished ownership at server cutoff."""
        return {
            role: task for role, task in self._owned_tasks.items() if not task.done()
        }

    @property
    def cleanup_complete(self):
        return (
            self._cleanup_task is not None
            and self._cleanup_task.done()
            and not self._cleanup_task.cancelled()
            and self._cleanup_task.exception() is None
            and not self.outstanding_tasks
            and "cleanup_failed" not in self.cleanup_failures
            and self.release_obligation
            not in {"not_attempted", "attempted_unconfirmed"}
            and self.circuit_obligation != "unresolved"
            and self.renewal_obligation != "unresolved"
            and "shutdown_incomplete" not in self.cleanup_failures
            and "release_not_attempted_shutdown" not in self.cleanup_failures
        )

    def _protected(self):
        if self.end is not None:
            return False
        if self.lifecycle_scope is not None:
            try:
                expired = self.lifecycle_scope.expired
            except RuntimeError:
                self.lifecycle_scope.expire()
                self.cancel(End.INTERNAL)
                return False
            if expired:
                self.cancel(End.SHUTDOWN)
                return False
        if self.handle is not None and self._confidence_deadline is not None:
            try:
                expired = self._now() >= self._confidence_deadline
            except Exception:  # noqa: BLE001 -- invalid clock is an internal terminal fault
                self.cancel(End.INTERNAL)
                return False
            if expired:
                self.cancel(End.OWNERSHIP_UNCERTAIN)
                return False
        return True

    def _now(self):
        value = self.clock.now()
        if not math.isfinite(value) or (
            self._last_time is not None and value < self._last_time
        ):
            raise RuntimeError("Invalid monotonic clock.")
        self._last_time = value
        return value

    def cancel(self, reason=End.CANCELLED):
        if reason not in {
            End.DISCONNECT,
            End.CANCELLED,
            End.SHUTDOWN,
            End.OWNERSHIP_LOST,
            End.OWNERSHIP_UNCERTAIN,
            End.INTERNAL,
        }:
            raise ValueError("Invalid cancellation cause.")
        return self._finish(
            reason,
            self._internal()
            if reason
            in {
                End.OWNERSHIP_LOST,
                End.OWNERSHIP_UNCERTAIN,
                End.INTERNAL,
            }
            else None,
        )

    @staticmethod
    def _internal():
        return GatewayHttpError(
            500, "gateway_internal_error", "Internal gateway error."
        )

    def _renewal_diagnostic(self, category):
        # R8 condition diagnostics, separate from the future R26 primary event.
        events = {
            "loss": "normal_api_renewal_ownership_lost",
            "unknown": "normal_api_renewal_unknown",
            "recovered": "normal_api_renewal_recovered",
            "expired": "normal_api_renewal_confidence_expired",
            "internal": "normal_api_renewal_internal_fault",
        }
        try:
            logger.info(
                "Normal API concurrency renewal condition",
                extra={
                    "event": events[category],
                    "plane": "DATA_NORMAL",
                    "request_id": str(self.resolved.request_id),
                },
            )
        except Exception:  # noqa: BLE001, S110 -- diagnostics cannot recurse or alter lifecycle truth
            pass

    def _finish(self, reason, error=None):
        if self.end is not None:
            return False
        self.end = reason
        if self.lifecycle_scope is not None:
            self.lifecycle_scope.begin_cleanup()
        self._terminal_event.set()
        if self._decision is None:
            self._decision = reason
            self.error = error
        # A definitive upstream result owns the public/circuit selection; a
        # later disconnect/loss still ends delivery but cannot rewrite it.
        if self._work is not None and self._work is not asyncio.current_task():
            self._work.cancel()
        if reason in {End.OWNERSHIP_LOST, End.OWNERSHIP_UNCERTAIN}:
            self._renewal_diagnostic(
                "loss" if reason is End.OWNERSHIP_LOST else "expired"
            )
        return True

    def observe_upstream(self, outcome: UpstreamOutcome):
        if not self._protected():
            return False
        if not self.dispatched or self.end is not None or self._decision is not None:
            return False
        if not isinstance(outcome, UpstreamOutcome):
            raise TypeError("Invalid upstream outcome.")
        self.upstream = outcome
        self._decision = "upstream"
        self.error = outcome.error
        self._start_completion()
        return True

    def _start_completion(self):
        if (
            self._completion is None
            and self.upstream is not None
            and self.upstream.classification != CircuitClass.NO_COMPLETION
        ):
            self._completion = self._own("circuit_completion", self._complete_circuit())

    def observe_timeouts(self, *kinds: TimeoutKind):
        """One R10 hook; no real transport timers are implemented here."""
        if not self._protected():
            return False
        if not self.dispatched or self.end is not None or self._decision is not None:
            return False
        active = set(kinds)
        if self.committed:
            active.discard(TimeoutKind.PRE_RESPONSE)
        selected = next((kind for kind in TimeoutKind if kind in active), None)
        if selected is None:
            return False
        error = GatewayHttpError(504, "upstream_timeout", "Upstream timeout.")
        if selected in {TimeoutKind.CONNECT, TimeoutKind.WRITE, TimeoutKind.READ}:
            self.upstream = UpstreamOutcome(CircuitClass.FAILURE, error=error)
        self._decision = End.TIMEOUT
        self.error = error
        self._start_completion()
        return self._finish(End.TIMEOUT)

    def before_downstream_commit(self):
        """Permission check before the adapter awaits response-start delivery."""
        if not self.dispatched or self.end is not None or self.committed:
            return False
        return self._protected()

    def committed_downstream(self):
        # Factual observation AFTER successful response-start. If confidence
        # expired during that await, stop delivery but never erase commitment.
        if not self.dispatched or self.committed:
            return False
        self.committed = True
        return self._protected()

    def _active(self):
        if not self._protected():
            raise asyncio.CancelledError

    def run(
        self,
        scope,
        *,
        request_id,
        receive,
        hooks: Hooks,
        lifecycle_scope: LifecycleScope,
    ):
        """Register ownership synchronously, including pre-first-step cancellation."""
        if self._started or self.lifecycle_scope is not None:
            raise RuntimeError("Lifecycle is single-use.")
        if not isinstance(lifecycle_scope, LifecycleScope):
            raise TypeError("A supervisor-owned scope is required.")
        self.clock = lifecycle_scope.supervisor.clock
        self.lifecycle_scope = lifecycle_scope
        return lifecycle_scope.launch(
            self,
            lambda: self._run(
                scope, request_id=request_id, receive=receive, hooks=hooks
            ),
        )

    async def _run(self, scope, *, request_id, receive, hooks):
        self._started = True
        self.receiver = ReceiveOwner(receive, self.cancel)
        self._shutdown_wait = asyncio.create_task(self.lifecycle_scope.wait_expired())
        terminal_wait = asyncio.create_task(self._terminal_event.wait())
        self._work = self._own("execution", self._pipeline(scope, request_id, hooks))
        externally_cancelled = False
        try:
            await asyncio.wait(
                (self._work, terminal_wait, self._shutdown_wait),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self.end is None:
                if self._shutdown_wait.cancelled():
                    self.lifecycle_scope.expire()
                    self.cleanup_failures.add("shutdown_signal_unavailable")
                    self.cancel(End.CANCELLED)
                else:
                    self.cancel(
                        End.SHUTDOWN if self.lifecycle_scope.expired else End.CANCELLED
                    )
        except asyncio.CancelledError:
            externally_cancelled = asyncio.current_task().cancelling() > 0
            self.cancel(End.CANCELLED)
        finally:
            self.lifecycle_scope.begin_cleanup()
            self._cleanup_task = asyncio.create_task(self._cleanup())
            self.lifecycle_scope.track("cleanup", self._cleanup_task)
            while not self._cleanup_task.done() and not self.lifecycle_scope.expired:
                try:
                    await self.lifecycle_scope.join(self._cleanup_task)
                except asyncio.CancelledError:
                    externally_cancelled = True
                    self.cancel(End.CANCELLED)
            if not self._cleanup_task.done() or self._cleanup_task.cancelled():
                self.cleanup_failures.add("shutdown_incomplete")
                if self.handle is not None and not self.release_attempted:
                    self.cleanup_failures.add("release_not_attempted_shutdown")
            else:
                if self._cleanup_task.exception() is not None:
                    self.cleanup_failures.add("cleanup_failed")
            for waiter in (terminal_wait, self._shutdown_wait):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(
                terminal_wait, self._shutdown_wait, return_exceptions=True
            )
        if externally_cancelled:
            raise asyncio.CancelledError
        return LifecycleResult(
            self.end,
            self.committed,
            self.upstream,
            None
            if self.committed
            or self.end
            in {
                End.DISCONNECT,
                End.CANCELLED,
                End.SHUTDOWN,
            }
            else self.error,
            self.cleanup_complete,
            tuple(self.outstanding_tasks),
        )

    async def _pipeline(self, scope, request_id, hooks):
        try:
            self._active()
            self.resolved = await self.resolver.resolve(scope, request_id=request_id)
            self._active()
            self.policies = (await hooks.policies(self.resolved)).for_resolution(
                self.resolved
            )
            self._active()
            decision = await self.rate.evaluate(self.policies.rate)
            self._active()
            if not decision.allowed:
                delay = max(1, math.ceil(decision.retry_after_ms / 1000))
                raise GatewayHttpError(
                    429,
                    "rate_limit_exceeded",
                    "Rate limit exceeded.",
                    headers={"Retry-After": str(delay)},
                )
            anchor = self._now()
            acquired = await self.concurrency.acquire(self.policies.concurrency)
            # Record exact issued ownership before checking cancellation/lateness.
            self.handle = acquired.handle if acquired.acquired else None
            if self.handle is not None:
                self._confirmed_anchor = anchor
                self._confidence_deadline = anchor + 2 * self.ttl / 3
            self._active()
            if not acquired.acquired:
                raise GatewayHttpError(
                    429, "rate_limit_exceeded", "Rate limit exceeded."
                )
            if self.handle is not None:
                if self._now() >= anchor + 2 * self.ttl / 3:
                    self.cancel(End.OWNERSHIP_UNCERTAIN)
                    self._active()
                self._renewal = self._own("renewal", self._renew(anchor))
                self._renewal.add_done_callback(self._renewal_finished)
            self.receiver.start()
            self._owned_tasks["receive"] = self.receiver.task
            self.lifecycle_scope.track("receive", self.receiver.task)
            body = await hooks.body(self.resolved, self.receiver)
            self._active()
            if not self.receiver.complete:
                raise RuntimeError("Body hook did not establish normal completion.")
            prepared = await hooks.prepare(self.resolved, body)
            self._active()
            self.circuit_identity = CircuitIdentity.normal(
                NormalApiRoute(
                    id=self.resolved.route["id"],
                    tenant_id=self.resolved.identity.tenant_id,
                    service_id=self.resolved.service["id"],
                ),
                NormalApiService(
                    id=self.resolved.service["id"],
                    tenant_id=self.resolved.identity.tenant_id,
                ),
            )
            admission = await self.circuit.check_or_claim_eligibility(
                self.circuit_identity
            )
            self.circuit_handle = admission.completion_handle
            self._active()
            if not admission.eligible:
                raise GatewayHttpError(
                    503, "upstream_unavailable", "Upstream unavailable."
                )
            self.dispatched = True
            await hooks.execute(self, prepared)
            self._active()
            if self.upstream is None:
                raise RuntimeError("Executor ended without a definitive outcome.")
            await hooks.deliver(self)
            self._active()
            if not self.committed:
                raise RuntimeError("Delivery ended without commitment.")
            self._finish(End.DELIVERED)
        except asyncio.CancelledError:
            if self.end is None:
                self.cancel(End.CANCELLED)
        except GatewayHttpError as error:
            self._finish(End.REJECTED, error)
        except Exception:  # noqa: BLE001 -- unfinished stage boundary fails closed
            self.cancel(End.INTERNAL)

    async def _guarded_renewal(self):
        # Internal authority propagation only; the facade's frozen API is intact.
        with managed_renewal_cleanup(self.lifecycle_scope):
            return await self.concurrency.renew_guarded(
                self.policies.concurrency, self.handle, guard=self._renewal_guard
            )

    async def _renew(self, anchor):
        try:
            while self.end is None:
                deadline = anchor + 2 * self.ttl / 3
                await self.clock.until(anchor + self.ttl / 3)
                for attempt in range(2):
                    self._active()
                    if self._now() >= deadline:
                        self.cancel(End.OWNERSHIP_UNCERTAIN)
                        return
                    operation = self._own(
                        "renew_operation",
                        self._guarded_renewal(),
                    )
                    self._retain_renewal(operation)
                    operation.add_done_callback(self._retain_renewal)
                    timer = self._own("renew_deadline", self.clock.until(deadline))
                    try:
                        done, _ = await asyncio.wait(
                            (operation, timer), return_when=asyncio.FIRST_COMPLETED
                        )
                        if self._now() >= deadline or operation not in done:
                            self.cancel(End.OWNERSHIP_UNCERTAIN)
                            return
                        if not self._protected():
                            return
                        outcome = operation.result()
                        self._retain_renewal(operation)
                        if not isinstance(outcome, GuardedRenewalOutcome):
                            raise TypeError("Invalid guarded renewal outcome.")
                        disposition = outcome.disposition
                        if disposition is RenewalDisposition.UNKNOWN:
                            if attempt == 0:
                                self._renewal_diagnostic("unknown")
                                await self.clock.until(anchor + self.ttl / 2)
                                continue
                            await self.clock.until(deadline)
                            self.cancel(End.OWNERSHIP_UNCERTAIN)
                            return
                        if disposition is RenewalDisposition.SUPPRESSED:
                            if self._protected():
                                self._renewal_diagnostic("internal")
                                self.cancel(End.INTERNAL)
                            return
                        if disposition in (
                            RenewalDisposition.GUARD_ERROR,
                            RenewalDisposition.INTERNAL_ERROR,
                        ):
                            self._renewal_diagnostic("internal")
                            self.cancel(End.INTERNAL)
                            return
                        if not outcome.result.renewed:
                            self.cancel(End.OWNERSHIP_LOST)
                            return
                        if disposition is not RenewalDisposition.BACKEND_RESULT:
                            raise TypeError("Invalid guarded renewal disposition.")
                        dispatched = outcome.dispatched_at
                        if not anchor <= dispatched <= self._now():
                            raise ValueError("Invalid renewal timestamp.")
                        # Publish confirmed producing truth before timer cleanup.
                        self._confirmed_anchor = dispatched
                        self._confidence_deadline = dispatched + 2 * self.ttl / 3
                        if attempt:
                            self._renewal_diagnostic("recovered")
                    finally:
                        for task in (operation, timer):
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(operation, timer, return_exceptions=True)
                        self._retain_renewal(operation)
                        done = None
                        task = operation = None
                    anchor = dispatched
                    break
        except asyncio.CancelledError:
            if not self._stopping and self.end is None:
                self._renewal_diagnostic("internal")
                self.cancel(End.INTERNAL)
        except Exception:  # noqa: BLE001 -- a dead worker must end its request
            if self.end is None:
                self._renewal_diagnostic("internal")
            self.cancel(End.INTERNAL)

    def _renewal_finished(self, task):
        # Also covers cancellation before the worker's coroutine first runs.
        if not self._stopping and self.end is None:
            self._renewal_diagnostic("internal")
            self.cancel(End.INTERNAL)

    async def _complete_circuit(self):
        try:
            if self.lifecycle_scope.expired:
                self.cleanup_failures.add("circuit_completion")
                return
            method = (
                self.circuit.record_success
                if self.upstream.classification == CircuitClass.SUCCESS
                else self.circuit.record_failure
            )
            async with asyncio.timeout(self.dependency_bound):
                self.completion_result = await method(
                    self.circuit_identity, self.circuit_handle
                )
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 -- preserve outcome
            self.cleanup_failures.add("circuit_completion")

    @staticmethod
    def _stop(task):
        if task is not None and not task.done() and task.cancelling() == 0:
            task.cancel()

    async def _join_before_shutdown(self, *tasks):
        result = await self.lifecycle_scope.join(*tasks)
        if not result:
            self.cleanup_failures.add("shutdown_incomplete")
        return result

    async def _release(self):
        if self.lifecycle_scope.expired:
            self.cleanup_failures.add("release_not_attempted_shutdown")
            return
        self.release_attempted = True
        try:
            async with asyncio.timeout(self.dependency_bound):
                self.release_result = await self.concurrency.release(
                    self.policies.concurrency, self.handle
                )
        except asyncio.CancelledError:
            self.cleanup_failures.add("release")
        except (TimeoutError, ConcurrencyDependencyError):
            # The operation failed to return; its backend effect is unknown.
            self.cleanup_failures.add("release")
        except Exception:  # noqa: BLE001 -- safe boundary, original result preserved
            self.cleanup_failures.add("release")

    async def _cleanup(self):
        self._stopping = True
        for task in (self._work, self._renewal, self.receiver.task):
            self._stop(task)
        renewal_stopped = await self._join_before_shutdown(
            self._renewal,
            self._owned_tasks.get("renew_operation"),
            self._owned_tasks.get("renew_deadline"),
        )
        if self.handle is not None:
            if renewal_stopped and not self.lifecycle_scope.expired:
                # A blocked executor finalizer must not prevent this obligation.
                release = self._own("release", self._release())
                await self._join_before_shutdown(release)
                if not self.release_attempted:
                    self.cleanup_failures.add("release_not_attempted_shutdown")
            else:
                # Never race a still-running renewal or claim an unmade attempt.
                self.cleanup_failures.add("release_not_attempted_shutdown")
        await self._join_before_shutdown(
            self._work, self.receiver.task, self._completion
        )
