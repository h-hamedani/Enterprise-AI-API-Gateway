"""Additive R2 renewal evidence and redis-py 8.1.0 guarded writer boundary."""

from __future__ import annotations

import asyncio
import math
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import StrEnum
from inspect import iscoroutine
from typing import Protocol
from weakref import WeakSet

from redis.asyncio.connection import Connection, ConnectionPool
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    BusyLoadingError,
    ConnectionError,
    NoScriptError,
    ResponseError,
)

from app.redis.concurrency import RenewResult, semaphore_key
from app.redis.redis_failure import is_redis_availability_failure

# Internal request context, propagated into redis-py's scheduled child tasks.
# The public guarded interface and legacy callers remain unchanged.
_cleanup_scope = ContextVar("guarded_renewal_cleanup_scope", default=None)
_active_operation = ContextVar("guarded_renewal_operation", default=None)


@contextmanager
def managed_renewal_cleanup(scope):
    from app.normal_api.supervisor import LifecycleScope

    if not isinstance(scope, LifecycleScope):
        raise TypeError("Existing lifecycle scope required.")
    scope.supervisor.now()  # Validate the existing owner loop/clock.
    token = _cleanup_scope.set(scope)
    try:
        yield
    finally:
        _cleanup_scope.reset(token)


def _cleanup_allowed(connection=None):
    scope = _cleanup_scope.get()
    deferred = getattr(connection, "_gateway_deferred_scope", None)
    for authority in (scope, deferred):
        if authority is None:
            continue
        try:
            if authority.expired:
                return False
        except Exception:  # noqa: BLE001 -- invalid authority must not start cleanup or mask cancellation
            authority.expire()
            return False
    return True


def _defer_cleanup(connection):
    scope = _cleanup_scope.get()
    if scope is not None and not hasattr(connection, "_gateway_deferred_scope"):
        connection._gateway_deferred_scope = scope
    owner = getattr(connection, "_managed_pool_owner", None)
    if owner is not None:
        owner.quarantine(connection)
    operation = _active_operation.get()
    if operation is not None:
        operation.deferred_connections.add(connection)


class RenewalDisposition(StrEnum):
    BACKEND_RESULT = "BACKEND_RESULT"
    NOT_OWNED = "NOT_OWNED"
    SUPPRESSED = "SUPPRESSED"
    UNKNOWN = "UNKNOWN"
    GUARD_ERROR = "GUARD_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class RenewalAttemptStatus(StrEnum):
    RESULT = "RESULT"
    NOSCRIPT = "NOSCRIPT"
    RESOLVED_ERROR = "RESOLVED_ERROR"
    UNRESOLVED = "UNRESOLVED"


class RenewalFailure(StrEnum):
    PREPARATION_DEPENDENCY = "preparation_dependency"
    TRANSPORT_DEPENDENCY = "transport_dependency"
    REDIS_DEPENDENCY = "redis_dependency"
    REDIS_ERROR = "redis_error"
    GUARD_ERROR = "guard_error"
    PROTOCOL_ERROR = "protocol_error"
    INTERNAL_ERROR = "internal_error"


class RenewalDispatchGuard(Protocol):
    def monotonic(self) -> float: ...

    def permit(self, sampled_at: float) -> bool: ...


@dataclass(frozen=True, slots=True)
class RenewalAttempt:
    dispatched_at: float
    backend: str
    status: RenewalAttemptStatus
    result: RenewResult | None = None
    failure_category: RenewalFailure | None = None

    def __post_init__(self):
        if (
            type(self.dispatched_at) not in (int, float)
            or not math.isfinite(self.dispatched_at)
            or self.backend not in ("REDIS", "LOCAL")
            or not isinstance(self.status, RenewalAttemptStatus)
        ):
            raise ValueError("Invalid renewal attempt.")
        if (self.status is RenewalAttemptStatus.RESULT) != (
            self.result is not None
        ) or (self.result is not None and not isinstance(self.result, RenewResult)):
            raise ValueError("Invalid renewal result evidence.")
        if self.result is not None and type(self.result.renewed) is not bool:
            raise ValueError("Invalid renewal result evidence.")
        if self.failure_category is not None and not isinstance(
            self.failure_category, RenewalFailure
        ):
            raise ValueError("Invalid renewal error classification.")
        if (
            self.status is not RenewalAttemptStatus.RESOLVED_ERROR
            and self.failure_category is not None
        ):
            raise ValueError("Failure classification requires a resolved Redis error.")
        if self.backend == "LOCAL" and self.status in (
            RenewalAttemptStatus.NOSCRIPT,
            RenewalAttemptStatus.RESOLVED_ERROR,
        ):
            raise ValueError("Invalid local renewal evidence.")
        if (
            self.status is RenewalAttemptStatus.RESOLVED_ERROR
            and self.failure_category
            not in (
                RenewalFailure.REDIS_ERROR,
                RenewalFailure.REDIS_DEPENDENCY,
            )
        ):
            raise ValueError("Missing renewal error classification.")


@dataclass(frozen=True, slots=True)
class GuardedRenewalOutcome:
    disposition: RenewalDisposition
    result: RenewResult | None = None
    dispatched_at: float | None = None
    attempts: tuple[RenewalAttempt, ...] = ()
    failure_category: RenewalFailure | None = None

    def __post_init__(self):
        if not isinstance(self.disposition, RenewalDisposition):
            raise TypeError("Invalid renewal disposition.")
        if self.dispatched_at is not None and (
            type(self.dispatched_at) not in (int, float)
            or not math.isfinite(self.dispatched_at)
        ):
            raise ValueError("Invalid producing timestamp.")
        if self.result is not None and (
            not isinstance(self.result, RenewResult)
            or type(self.result.renewed) is not bool
        ):
            raise ValueError("Invalid renewal result.")
        if self.failure_category is not None and not isinstance(
            self.failure_category, RenewalFailure
        ):
            raise ValueError("Invalid renewal failure category.")
        if type(self.attempts) is not tuple or len(self.attempts) > 2:
            raise ValueError("Invalid renewal history.")
        if not all(isinstance(a, RenewalAttempt) for a in self.attempts):
            raise ValueError("Invalid renewal history.")
        if len(self.attempts) == 2 and (
            self.attempts[0].status is not RenewalAttemptStatus.NOSCRIPT
            or any(a.backend != "REDIS" for a in self.attempts)
        ):
            raise ValueError("Invalid renewal retry history.")
        if self.disposition is RenewalDisposition.BACKEND_RESULT:
            if (
                not self.attempts
                or self.attempts[-1].status is not RenewalAttemptStatus.RESULT
                or self.result != self.attempts[-1].result
                or self.dispatched_at != self.attempts[-1].dispatched_at
                or self.failure_category is not None
            ):
                raise ValueError("Missing producing renewal attempt.")
        elif self.disposition is RenewalDisposition.NOT_OWNED:
            if self.result != RenewResult(False) or self.attempts:
                raise ValueError("Invalid missing ownership evidence.")
        elif self.result is not None:
            raise ValueError("Unexpected renewal result.")
        if (
            self.disposition is not RenewalDisposition.BACKEND_RESULT
            and self.dispatched_at is not None
        ):
            raise ValueError("Unexpected producing timestamp.")
        if self.disposition in (
            RenewalDisposition.SUPPRESSED,
            RenewalDisposition.GUARD_ERROR,
        ) and (
            len(self.attempts) > 1
            or any(a.status is not RenewalAttemptStatus.NOSCRIPT for a in self.attempts)
        ):
            raise ValueError("Invalid suppressed renewal history.")
        if self.disposition is RenewalDisposition.UNKNOWN and self.attempts:
            last = self.attempts[-1]
            if last.status is RenewalAttemptStatus.NOSCRIPT and len(self.attempts) != 1:
                raise ValueError("Invalid dependency history.")
            if last.status is RenewalAttemptStatus.RESULT:
                raise ValueError("Confirmed renewal cannot be unknown.")
            if (
                last.status is RenewalAttemptStatus.RESOLVED_ERROR
                and last.failure_category is not RenewalFailure.REDIS_DEPENDENCY
            ):
                raise ValueError("Non-dependency reply cannot become unknown.")


class GuardFault(RuntimeError):
    def __init__(self):
        super().__init__("Renewal authorization failed.")


class RenewalProtocolFault(RuntimeError):
    def __init__(self):
        super().__init__("Guarded renewal response is invalid.")


class GuardedTransportClosed(RuntimeError):
    def __init__(self):
        super().__init__("Guarded Redis transport is closed.")


class GuardedDisposalNotStarted(RuntimeError):
    def __init__(self):
        super().__init__("Guarded Redis disposal did not start before cutoff.")


class ManagedPoolDisposalError(RuntimeError):
    def __init__(self):
        super().__init__("Managed Redis connection disposal is unconfirmed.")


@dataclass(slots=True)
class WriterClosureObligation:
    writer: object
    protocol: object
    confirmation: object
    disconnect_requested: bool = False

    @property
    def state(self):
        future = self.confirmation
        if self.protocol is None or not isinstance(future, asyncio.Future):
            return "unresolved"
        if future.cancelled():
            return "unresolved"
        if future.done():
            if future.exception() is not None:
                return "failed"
            return "completed" if future.result() is None else "unresolved"
        return "pending" if self.disconnect_requested else "not_attempted"


class ManagedPoolOwnership:
    """Observe from creation; fence one pool synchronously on managed closure."""

    def __init__(self, pool):
        self.pool = pool
        self.loop = asyncio.get_running_loop()
        self.closing = False
        self.closed_admission = asyncio.Event()
        self.scope = None
        self.connections = {}
        self.inflight = {}
        self.last_outcome = {}
        self.last_cleanup = {}
        self.observed_tasks = WeakSet()
        self.observed_confirmations = WeakSet()
        self.writers = {}
        self.writer_obligations = {}
        self.untracked_initial_work = bool(pool._in_use_connections)
        for connection in (*pool._available_connections, *pool._in_use_connections):
            self._register(connection)
        make = pool.make_connection
        available = pool.get_available_connection
        checkout = pool.get_connection
        release = pool.release

        def make_connection():
            self.require_open()
            connection = make()
            self._register(connection)
            return connection

        def get_available_connection():
            self.require_open()
            connection = available()
            if getattr(connection, "_gateway_quarantined", False):
                raise GuardedTransportClosed()
            return connection

        async def release_connection(connection):
            if getattr(connection, "_gateway_quarantined", False):
                return
            if not _cleanup_allowed(connection):
                _defer_cleanup(connection)
                return
            try:
                await release(connection)
            finally:
                if getattr(connection, "_gateway_quarantined", False):
                    self.quarantine(connection)

        pool.release = release_connection
        pool.make_connection = make_connection
        pool.get_available_connection = get_available_connection
        pool.get_connection = self._observe(checkout, None)

    def require_open(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Redis pool ownership belongs to another event loop.")
        if self.closing:
            raise GuardedTransportClosed()

    def quarantine(self, connection):
        # Synchronous exclusion is not physical disposal. Keep the bounded pool
        # capacity slot and complete ownership registry; never auto-retry it.
        connection._gateway_quarantined = True
        if connection in self.pool._available_connections:
            self.pool._available_connections.remove(connection)
        self.pool._in_use_connections.add(connection)
        writer = getattr(connection, "_writer", None)
        if writer is not None:
            self.retain_writer(connection, writer)
        self.last_cleanup[connection] = "unresolved"

    def _register(self, connection):
        if connection in self.connections:
            return
        self.connections[connection] = len(self.connections)
        connection._managed_pool_owner = self
        if getattr(connection, "_writer", None) is not None:
            self.retain_writer(connection, connection._writer)
        for name in ("connect", "connect_check_health", "_connect"):
            setattr(
                connection, name, self._observe(getattr(connection, name), connection)
            )
        connection.disconnect = self._observe(
            connection.disconnect, connection, cleanup=True
        )

    def _observe(self, operation, connection, *, cleanup=False):
        async def execute(*args, **kwargs):
            if cleanup and not _cleanup_allowed(connection):
                _defer_cleanup(connection)
                return
            if not cleanup:
                self.require_open()
                if getattr(connection, "_gateway_quarantined", False):
                    raise GuardedTransportClosed()
            elif self.closing and self.scope.expired:
                raise ManagedPoolDisposalError()
            completion = asyncio.get_running_loop().create_future()
            self.inflight[completion] = (asyncio.current_task(), connection)
            if self.closing:
                self._observe_task(asyncio.current_task())
            outcome = "unresolved"
            writer = getattr(connection, "_writer", None)
            if writer is not None:
                self.retain_writer(connection, writer, requested=cleanup)
            if cleanup and writer is not None:
                self.last_cleanup[connection] = "pending"
            try:
                result = await operation(*args, **kwargs)
                outcome = "completed"
                # Never publish a usable connection after the admission fence.
                if not cleanup:
                    self.require_open()
                    if getattr(connection, "_gateway_quarantined", False):
                        raise GuardedTransportClosed()
                return result
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except Exception:
                if outcome != "completed":
                    outcome = "failed"
                if self.closing and not cleanup:
                    # redis-py may wrap a closed-admission rejection as a network
                    # error. Shutdown must not become a new dependency failure.
                    raise GuardedTransportClosed() from None
                raise
            finally:
                if (
                    connection is not None
                    and getattr(connection, "_writer", None) is not None
                ):
                    self.retain_writer(connection, connection._writer)
                elif cleanup and writer is not None:
                    self.retain_writer(connection, writer, requested=True)
                if cleanup:
                    self.last_cleanup[connection] = (
                        "completed"
                        if self.closure_confirmed(connection)
                        else "unresolved"
                    )
                self.last_outcome[connection] = outcome
                completion.set_result(outcome)
                self.inflight.pop(completion)

        return execute

    def retain_writer(self, connection, writer, *, requested=False):
        # Identity includes the protocol generation and its original close future.
        protocol = getattr(writer, "_protocol", None)
        try:
            confirmation = protocol._get_close_waiter(writer) if protocol else None
        except Exception:  # noqa: BLE001 -- missing confirmation remains unknown
            confirmation = None
        records = self.writer_obligations.setdefault(connection, {})
        if (
            isinstance(confirmation, asyncio.Future)
            and confirmation not in self.observed_confirmations
        ):
            self.observed_confirmations.add(confirmation)
            confirmation.add_done_callback(GuardedRenewalTransport._observe_close)
        key = (id(writer), id(protocol), id(confirmation))
        record = records.get(key)
        if record is None:
            record = WriterClosureObligation(writer, protocol, confirmation)
            records[key] = record
        record.disconnect_requested |= requested
        self.writers[connection] = writer
        return record

    def closure_confirmed(self, connection):
        writer = getattr(connection, "_writer", None)
        if writer is not None:
            self.retain_writer(connection, writer)
        records = self.writer_obligations.get(connection, {})
        had_records = bool(records)
        # Compact only authoritative successful confirmations. Never discard a
        # failed/cancelled/missing/pending obligation when a writer is replaced.
        for key, record in tuple(records.items()):
            if record.state == "completed":
                del records[key]
        confirmed = not records
        if (
            getattr(connection, "_gateway_quarantined", False)
            and not records
            and not had_records
        ):
            confirmed = self.last_cleanup.get(connection) == "completed"
        if (
            connection in self.last_cleanup
            and self.last_cleanup[connection] != "pending"
        ):
            self.last_cleanup[connection] = "completed" if confirmed else "unresolved"
        return confirmed

    def pending_confirmations(self, connection):
        return tuple(
            record.confirmation
            for record in self.writer_obligations.get(connection, {}).values()
            if isinstance(record.confirmation, asyncio.Future)
            and not record.confirmation.done()
        )

    def validate_scope(self, scope):
        if (
            asyncio.get_running_loop() is not self.loop
            or scope.supervisor.loop is not self.loop
        ):
            raise RuntimeError("Redis pool ownership belongs to another event loop.")
        if (
            scope.request_deadline is not None
            or scope.supervisor.process_deadline is None
        ):
            raise RuntimeError(
                "Shared Redis disposal requires active process authority."
            )

    def begin_close(self, scope):
        self.validate_scope(scope)
        if not self.closing:
            self.scope = scope
            self.closing = True
            for task, _ in self.inflight.values():
                self._observe_task(task)
            self.closed_admission.set()

    def _observe_task(self, task):
        if task not in self.observed_tasks:
            self.observed_tasks.add(task)
            task.add_done_callback(GuardedRenewalTransport._observe_close)

    async def settle(self):
        if self.untracked_initial_work:
            raise ManagedPoolDisposalError()
        await self.wait_for_completion(tuple(self.inflight))

    async def wait_for_completion(self, futures):
        pending = {future for future in futures if not future.done()}
        if not pending:
            return
        if self.scope is None or self.scope.expired:
            raise ManagedPoolDisposalError()
        expiry = asyncio.create_task(self.scope.wait_expired())
        try:
            while pending:
                if self.scope.expired:
                    raise ManagedPoolDisposalError()
                done, _ = await asyncio.wait(
                    (*pending, expiry), return_when=asyncio.FIRST_COMPLETED
                )
                if expiry in done:
                    raise ManagedPoolDisposalError()
                pending.difference_update(done)
        finally:
            expiry.cancel()
            await asyncio.gather(expiry, return_exceptions=True)


def observe_pool_ownership(pool):
    owner = getattr(pool, "_gateway_managed_owner", None)
    if owner is None:
        owner = ManagedPoolOwnership(pool)
        pool._gateway_managed_owner = owner
    return owner


class ManagedPoolDisposal:
    """Instance-local redis-py 8.1.0 disposal fencing, never a command retry."""

    def __init__(self, pool, can_start):
        import redis

        if redis.__version__ != "8.1.0" or not isinstance(pool, ConnectionPool):
            raise RuntimeError("Managed Redis pool disposal is unsupported.")
        self.pool = pool
        self.can_start = can_start
        self.states = {}
        self.tasks = {}
        self._connections = {}
        self.owner = getattr(pool, "_gateway_managed_owner", None)
        self._capture()
        original_disconnect = pool.disconnect
        original_hook = pool._on_close

        async def disconnect(*args, **kwargs):
            if not self.can_start():
                raise ManagedPoolDisposalError()
            if self.owner is not None:
                await self.owner.settle()
            if not self.can_start():
                raise ManagedPoolDisposalError()
            self._capture()
            failed = False
            try:
                await original_disconnect(*args, **kwargs)
            except (Exception, asyncio.CancelledError):  # noqa: BLE001 -- aggregate disposal certainty
                failed = True
            members = {*pool._available_connections, *pool._in_use_connections}
            retired = [c for c in self._connections if c not in members]
            if retired and self.can_start():
                results = await asyncio.gather(
                    *(c.disconnect() for c in retired), return_exceptions=True
                )
                failed = failed or any(isinstance(r, BaseException) for r in results)
            if self.owner is not None:
                for connection, index in self._connections.items():
                    if not self.owner.closure_confirmed(connection):
                        failed = True
                        if self.states[index] == "completed":
                            self.states[index] = "unresolved"
            if failed or any(state != "completed" for state in self.states.values()):
                raise ManagedPoolDisposalError()

        async def on_close():
            if not self.can_start():
                raise ManagedPoolDisposalError()
            await original_hook()

        pool.disconnect = disconnect
        pool._on_close = on_close

    def _capture(self):
        for connection in (
            *(self.owner.connections if self.owner is not None else ()),
            *self.pool._available_connections,
            *self.pool._in_use_connections,
        ):
            if connection in self._connections:
                continue
            index = len(self._connections)
            self._connections[connection] = index
            if self.owner is not None and connection not in self.owner.connections:
                # A foreign/pre-instrumentation admission has no trustworthy
                # establishment history. Retain it without claiming closure.
                self.states[index] = "unresolved"
                raise ManagedPoolDisposalError()
            self.states[index] = "not_attempted"
            connection.disconnect = self._wrap(connection, connection.disconnect, index)

    def _wrap(self, connection, operation, index):
        async def disconnect_connection(*args, **kwargs):
            state = self.states[index]
            confirmed = (
                self.owner.closure_confirmed(connection)
                if self.owner is not None
                else None
            )
            if (
                state == "not_attempted"
                and self.owner is not None
                and confirmed
                and self.owner.last_cleanup.get(connection) == "completed"
                and getattr(connection, "_writer", None) is None
            ):
                self.states[index] = state = "completed"
            if state == "completed":
                if (
                    getattr(connection, "_writer", None) is not None
                    or confirmed is False
                ):
                    self.states[index] = "unresolved"
                    raise ManagedPoolDisposalError()
                return
            if state == "pending":
                await asyncio.shield(self.tasks[index])
                return
            if state != "not_attempted":
                raise ManagedPoolDisposalError()
            if not self.can_start():
                self.states[index] = "skipped"
                raise ManagedPoolDisposalError()
            # No task handoff separates this check from the original disconnect.
            self.states[index] = "pending"
            self.tasks[index] = asyncio.current_task()
            self.tasks[index].add_done_callback(GuardedRenewalTransport._observe_close)
            writer = getattr(connection, "_writer", None)
            if writer is None and self.owner is not None:
                writer = self.owner.writers.get(connection)
            try:
                await operation(*args, **kwargs)
                # redis-py swallows OSError from writer close/wait_closed. Confirm
                # the actual close waiter rather than equating return with success.
                if self.owner is not None:
                    pending = self.owner.pending_confirmations(connection)
                    if pending and self.can_start():
                        async with asyncio.timeout(connection.socket_connect_timeout):
                            await self.owner.wait_for_completion(pending)
                    if not self.owner.closure_confirmed(connection):
                        self.states[index] = "unresolved"
                        raise ManagedPoolDisposalError()
                elif writer is not None:
                    protocol = getattr(writer, "_protocol", None)
                    waiter = protocol._get_close_waiter(writer) if protocol else None
                    if (
                        isinstance(waiter, asyncio.Future)
                        and not waiter.done()
                        and self.owner is not None
                        and self.owner.closing
                        and self.can_start()
                    ):
                        # An error-path nowait disconnect already initiated close.
                        # Join its existing confirmation, never retry disconnect.
                        async with asyncio.timeout(connection.socket_connect_timeout):
                            await self.owner.wait_for_completion((waiter,))
                    if (
                        not isinstance(waiter, asyncio.Future)
                        or not waiter.done()
                        or waiter.cancelled()
                        or waiter.exception() is not None
                    ):
                        self.states[index] = "unresolved"
                        raise ManagedPoolDisposalError()
            except asyncio.CancelledError:
                self.states[index] = "unresolved"
                raise
            except Exception:  # noqa: BLE001 -- never expose dependency details
                if self.states[index] != "unresolved":
                    self.states[index] = "failed"
                raise ManagedPoolDisposalError() from None
            else:
                self.states[index] = "completed"
                if self.owner is not None:
                    self.owner.last_cleanup[connection] = "completed"

        return disconnect_connection


class GuardedRenewalOperation:
    """Request-owned live evidence survives cancellation of its current task."""

    def __init__(self, backend="REDIS"):
        self.backend = backend
        self.cleanup_scope = _cleanup_scope.get()
        self.deferred_connections = set()
        self.attempts: tuple[RenewalAttempt, ...] = ()
        self.phase = "preparation"
        self.cancelled = False
        self.outcome: GuardedRenewalOutcome | None = None
        task = asyncio.current_task()
        if task is not None:
            task.guarded_renewal_operation = self

    @property
    def cleanup_pending(self):
        for connection in self.deferred_connections:
            owner = getattr(connection, "_managed_pool_owner", None)
            if owner is None or not owner.closure_confirmed(connection):
                return True
        return False

    def authorize(self, guard: RenewalDispatchGuard) -> float | None:
        try:
            stamp = guard.monotonic()
            if type(stamp) not in (int, float) or not math.isfinite(stamp):
                raise GuardFault()
            if self.attempts and stamp < self.attempts[-1].dispatched_at:
                raise GuardFault()
            permitted = guard.permit(stamp)
            if type(permitted) is not bool:
                if iscoroutine(permitted):
                    permitted.close()
                raise GuardFault()
            return stamp if permitted else None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- caller guard faults are sanitized
            raise GuardFault() from None

    def begin(self, stamp: float):
        self.attempts += (
            RenewalAttempt(stamp, self.backend, RenewalAttemptStatus.UNRESOLVED),
        )
        self.phase = "write" if self.backend == "REDIS" else "local"

    def resolve(self, status, *, result=None, failure=None):
        self.attempts = self.attempts[:-1] + (
            replace(
                self.attempts[-1],
                status=status,
                result=result,
                failure_category=failure,
            ),
        )

    def finish(self, disposition, *, failure=None):
        producing = (
            self.attempts[-1]
            if disposition is RenewalDisposition.BACKEND_RESULT
            else None
        )
        self.outcome = GuardedRenewalOutcome(
            disposition,
            result=producing.result if producing else None,
            dispatched_at=producing.dispatched_at if producing else None,
            attempts=self.attempts,
            failure_category=failure,
        )
        return self.outcome

    def failed(self, exc: Exception):
        if isinstance(exc, GuardFault):
            return self.finish(
                RenewalDisposition.GUARD_ERROR, failure=RenewalFailure.GUARD_ERROR
            )
        confirmed = (
            self.attempts and self.attempts[-1].status is RenewalAttemptStatus.RESULT
        )
        if (
            self.backend == "REDIS"
            and is_redis_availability_failure(exc)
            and not confirmed
        ):
            category = RenewalFailure.PREPARATION_DEPENDENCY
            if (
                self.attempts
                and self.attempts[-1].status is RenewalAttemptStatus.UNRESOLVED
            ):
                category = RenewalFailure.TRANSPORT_DEPENDENCY
            if (
                self.attempts
                and self.attempts[-1].status is RenewalAttemptStatus.RESOLVED_ERROR
            ):
                category = RenewalFailure.REDIS_DEPENDENCY
            return self.finish(RenewalDisposition.UNKNOWN, failure=category)
        category = RenewalFailure.INTERNAL_ERROR
        if isinstance(exc, RenewalProtocolFault):
            category = RenewalFailure.PROTOCOL_ERROR
        elif (
            self.attempts
            and self.attempts[-1].status is RenewalAttemptStatus.RESOLVED_ERROR
        ):
            category = RenewalFailure.REDIS_ERROR
        return self.finish(RenewalDisposition.INTERNAL_ERROR, failure=category)


def not_owned() -> GuardedRenewalOutcome:
    return GuardedRenewalOutcome(RenewalDisposition.NOT_OWNED, RenewResult(False))


class GuardedRenewalConnection(Connection):
    """Private writer integration pinned and tested against redis-py 8.1.0."""

    def __init__(self, *args, transport_owner=None, **kwargs):
        self.transport_owner = transport_owner
        super().__init__(*args, **kwargs)

    def _require_open(self):
        if self.transport_owner is not None:
            self.transport_owner.require_open()

    async def _connect(self):
        self._require_open()
        try:
            await super()._connect()
        finally:
            owner = getattr(self, "_managed_pool_owner", None)
            if (
                self.transport_owner is not None
                and self.transport_owner.closed
                and not (owner is not None and owner.closing)
            ):
                if _cleanup_allowed(self):
                    await self.disconnect(nowait=True)
                else:
                    _defer_cleanup(self)
        self._require_open()

    async def _send_packed_command(self, command):
        # Guarded pools always have a finite socket timeout. Inherited preparation
        # sends reach this hook after their own connection/health awaits.
        self._require_open()
        await super()._send_packed_command(command)

    async def send_renewal(self, command, guard, operation):
        self._require_open()
        await self.connect_check_health(check_health=False)
        await self.check_health()
        packed = self.pack_command(*command)

        async def write():
            if self.transport_owner is not None and self.transport_owner.closed:
                return False
            stamp = operation.authorize(guard)
            if stamp is None:
                return False
            operation.begin(stamp)
            self._writer.writelines(packed)
            operation.phase = "drain"
            await self._writer.drain()
            return True

        # The guard is inside the write coroutine, after any timeout-wrapper handoff.
        try:
            return await asyncio.wait_for(write(), self.socket_timeout)
        except OSError as exc:
            if isinstance(exc, TimeoutError):
                raise
            raise ConnectionError("Guarded Redis write failed.") from None


class GuardedRenewalPool(ConnectionPool):
    def __init__(self, *, transport_owner, **kwargs):
        self.transport_owner = transport_owner
        super().__init__(transport_owner=transport_owner, **kwargs)

    def get_available_connection(self):
        # Called under redis-py's checkout lock, after any contention wait.
        self.transport_owner.require_open()
        return super().get_available_connection()


class GuardedRenewalTransport:
    MAX_CONNECTIONS = 16

    def __init__(self, client, timeout_seconds):
        import redis

        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Guarded Redis operation requires a finite bound.")
        if redis.__version__ != "8.1.0":
            raise RuntimeError("Guarded Redis transport version is unsupported.")
        source = client.connection_pool
        if source.connection_class is not Connection:
            raise RuntimeError("Guarded Redis connection security is unsupported.")
        options = dict(source.connection_kwargs)
        options["retry"] = Retry(NoBackoff(), 0)
        options["retry_on_timeout"] = False
        options["retry_on_error"] = []
        options["socket_timeout"] = timeout_seconds
        options["socket_connect_timeout"] = timeout_seconds
        self.timeout_seconds = timeout_seconds
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._close_task: asyncio.Task | None = None
        self.pool = GuardedRenewalPool(
            transport_owner=self,
            connection_class=GuardedRenewalConnection,
            max_connections=self.MAX_CONNECTIONS,
            **options,
        )
        self.ownership = observe_pool_ownership(self.pool)

    @property
    def closed(self):
        return self._closed

    def assert_owner(self):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Guarded Redis transport belongs to another event loop.")

    def require_open(self):
        self.assert_owner()
        if self._closed:
            raise GuardedTransportClosed()

    async def _disconnect_operation(self, connection):
        # Executed at actual coroutine entry, not at task creation.
        if not _cleanup_allowed(connection):
            _defer_cleanup(connection)
            return
        await connection.disconnect(nowait=True)

    async def _release_operation(self, connection):
        if not _cleanup_allowed(connection):
            _defer_cleanup(connection)
            return
        await self.pool.release(connection)

    async def script_load(self, script):
        self.require_open()
        connection = await self.pool.get_connection()
        aligned = False
        try:
            self.require_open()
            await connection.send_command("SCRIPT", "LOAD", script)
            result = await connection.read_response()
            if not isinstance(result, (str, bytes)):
                raise RenewalProtocolFault()
            aligned = True
            return result
        finally:
            try:
                if not aligned or self.closed:
                    await self._disconnect_operation(connection)
            finally:
                await self._release_operation(connection)

    async def execute(self, sha, keys, arguments, *, guard, operation):
        self.assert_owner()
        if self.closed:
            return None
        operation.phase = "preparation"
        connection = None
        previous_attempts = len(operation.attempts)
        aligned = False
        try:
            connection = await self.pool.get_connection()
            connection.transport_owner = self
            if self.closed:
                return None
            sent = await connection.send_renewal(
                ("EVALSHA", sha, len(keys), *keys, *arguments), guard, operation
            )
            if not sent:
                aligned = True
                return None
            if self.closed:
                raise ConnectionError("Guarded Redis transport closed after send.")
            operation.phase = "response"
            try:
                response = await connection.read_response()
            except NoScriptError:
                operation.resolve(RenewalAttemptStatus.NOSCRIPT)
                aligned = connection.is_connected
                raise
            except (
                ResponseError,
                AuthenticationError,
                AuthorizationError,
                BusyLoadingError,
            ) as exc:
                operation.resolve(
                    RenewalAttemptStatus.RESOLVED_ERROR,
                    failure=(
                        RenewalFailure.REDIS_DEPENDENCY
                        if is_redis_availability_failure(exc)
                        else RenewalFailure.REDIS_ERROR
                    ),
                )
                aligned = connection.is_connected
                raise
            if type(response) is not int or response not in (0, 1):
                raise RenewalProtocolFault()
            operation.resolve(
                RenewalAttemptStatus.RESULT, result=RenewResult(bool(response))
            )
            aligned = True
            return response
        except GuardedTransportClosed:
            if len(operation.attempts) > previous_attempts:
                raise ConnectionError(
                    "Guarded Redis transport closed after send."
                ) from None
            return None
        except Exception as exc:
            if (
                self.closed
                and len(operation.attempts) == previous_attempts
                and is_redis_availability_failure(exc)
            ):
                return None
            raise
        finally:
            if connection is not None:
                try:
                    if not aligned or self.closed:
                        await self._disconnect_operation(connection)
                finally:
                    await self._release_operation(connection)

    def latch_close(self):
        self.assert_owner()
        # No await separates this latch from a competing final writer segment.
        self._closed = True

    def request_close(self, *, can_start=None):
        self.latch_close()
        if self._close_task is None:
            if can_start is not None and isinstance(self.pool, ConnectionPool):
                self._pool_disposal = ManagedPoolDisposal(self.pool, can_start)

            async def disconnect():
                # Recheck at actual task entry, after the scheduling handoff.
                if can_start is not None and not can_start():
                    raise GuardedDisposalNotStarted()
                await self.pool.disconnect()

            self._close_task = asyncio.create_task(disconnect())
            self._close_task.add_done_callback(self._observe_close)

    async def close(self, *, can_start=None):
        self.request_close(can_start=can_start)
        # Retain the single disposal task if a caller stops waiting at its R1 cutoff.
        await asyncio.shield(self._close_task)

    @staticmethod
    def _observe_close(task):
        if not task.cancelled():
            task.exception()


async def renew_redis_guarded(semaphore, ordered, lease_id, guard):
    operation = GuardedRenewalOperation()
    token = _active_operation.set(operation)
    try:
        return await _renew_redis_guarded(
            semaphore, ordered, lease_id, guard, operation
        )
    finally:
        _active_operation.reset(token)


async def _renew_redis_guarded(semaphore, ordered, lease_id, guard, operation):
    transport = None
    try:
        transport = semaphore._runtime.guarded_renewal_transport
        keys = [semaphore_key(policy) for policy in ordered]
        arguments = [semaphore._duration, str(lease_id)]
        async with asyncio.timeout(transport.timeout_seconds):
            sha = await semaphore._load("renew", transport=transport)
            try:
                response = await transport.execute(
                    sha, keys, arguments, guard=guard, operation=operation
                )
            except NoScriptError:
                operation.phase = "preparation"
                transport.require_open()
                sha = await semaphore._load("renew", force=True, transport=transport)
                response = await transport.execute(
                    sha, keys, arguments, guard=guard, operation=operation
                )
            disposition = (
                RenewalDisposition.SUPPRESSED
                if response is None
                else RenewalDisposition.BACKEND_RESULT
            )
            return operation.finish(disposition)
    except asyncio.CancelledError:
        operation.cancelled = True
        raise
    except GuardedTransportClosed:
        return operation.finish(RenewalDisposition.SUPPRESSED)
    except Exception as exc:  # noqa: BLE001 -- R2 classifies faults without exposing text
        if (
            transport is not None
            and transport.closed
            and is_redis_availability_failure(exc)
            and all(
                a.status is RenewalAttemptStatus.NOSCRIPT for a in operation.attempts
            )
        ):
            return operation.finish(RenewalDisposition.SUPPRESSED)
        return operation.failed(exc)
