"""R1 process-local deadline and residual ownership; one owning event loop."""

from __future__ import annotations

import asyncio
import math


class MonotonicClock:
    def now(self):
        return asyncio.get_running_loop().time()

    async def until(self, deadline):
        await asyncio.sleep(max(0, deadline - self.now()))


class AdmissionClosed(RuntimeError):
    pass


class LifecycleSupervisor:
    def __init__(self, grace_seconds, *, clock=None):
        if (
            isinstance(grace_seconds, bool)
            or not isinstance(grace_seconds, (int, float))
            or not math.isfinite(grace_seconds)
            or grace_seconds <= 0
        ):
            raise ValueError("An explicit finite positive lifecycle grace is required.")
        self.grace_seconds = grace_seconds
        self.clock = clock or MonotonicClock()
        self.loop = asyncio.get_running_loop()
        self._last_time = None
        self.process_deadline = None
        self.scopes = set()
        self.residuals = set()
        self.resources = {}
        self.unclosed_resources = set()
        self.membership_changed = asyncio.Event()
        self.new_deadline()  # Reject an unusable derived deadline before enabling work.

    def now(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Lifecycle authority belongs to another event loop.")
        value = self.clock.now()
        if not math.isfinite(value) or (
            self._last_time is not None and value < self._last_time
        ):
            raise RuntimeError("Invalid lifecycle monotonic clock.")
        self._last_time = value
        return value

    def new_deadline(self):
        deadline = self.now() + self.grace_seconds
        if not math.isfinite(deadline):
            raise ValueError("Lifecycle deadline is not finite.")
        return deadline

    def register(self):
        # No await: registration and shutdown closure linearize on this loop.
        self.now()
        if self.process_deadline is not None:
            raise AdmissionClosed("Managed admission is closed.")
        scope = LifecycleScope(self)
        self.scopes.add(scope)
        self.membership_changed.set()
        return scope

    def begin_shutdown(self):
        self.now()
        if self.process_deadline is None:
            self.process_deadline = self.new_deadline()
            for scope in tuple(self.scopes):
                scope.changed.set()
        return self.process_deadline

    def _observe(self, scope):
        self.loop.call_soon(scope.refresh)

    async def drain(self):
        self.begin_shutdown()
        boundary = LifecycleScope(self)
        while self.scopes or self.residuals:
            self.membership_changed.clear()
            tasks = {
                task
                for scope in self.scopes | self.residuals
                for task in scope.tasks.values()
                if not task.done()
            }
            if not tasks:
                if not self.scopes:
                    break
                changed = asyncio.create_task(self.membership_changed.wait())
                try:
                    if not await boundary.join(changed):
                        break
                finally:
                    changed.cancel()
                    await asyncio.gather(changed, return_exceptions=True)
                continue
            if not await boundary.join(*tasks):
                break
            for scope in tuple(self.scopes):
                scope.refresh()
        for scope in tuple(self.scopes):
            if boundary.expired:
                scope.expire()

    async def close_resources(self, resources):
        """Lifespan cleanup shares the original process cutoff, never a new G."""
        self.begin_shutdown()
        boundary = LifecycleScope(self)
        self.unclosed_resources.update(name for name, _ in resources)
        for name, close in resources:
            if boundary.expired:
                break

            async def invoke(operation=close):
                if not boundary.expired:
                    await operation()
                    return True
                return False

            task = asyncio.create_task(invoke())
            self.resources[name] = task
            task.add_done_callback(LifecycleScope.consume)

            def observed(completed, resource=name):
                if (
                    not completed.cancelled()
                    and completed.exception() is None
                    and completed.result() is True
                ):
                    self.unclosed_resources.discard(resource)

            task.add_done_callback(observed)
            if not await boundary.join(task):
                break
            if (
                not task.cancelled()
                and task.exception() is None
                and task.result() is True
            ):
                self.unclosed_resources.discard(name)


class LifecycleScope:
    def __init__(self, supervisor):
        self.supervisor = supervisor
        self.request_deadline = None
        self.changed = asyncio.Event()
        self.notification = asyncio.Event()
        self._expired = False
        self.tasks = {}
        self.owner = None
        self.runner = None

    @property
    def deadline(self):
        values = (self.request_deadline, self.supervisor.process_deadline)
        return (
            min(value for value in values if value is not None)
            if any(value is not None for value in values)
            else None
        )

    @property
    def expired(self):
        if self._expired:
            return True
        if self.notification.is_set() or (
            self.deadline is not None and self.supervisor.now() >= self.deadline
        ):
            self._expired = True
        return self._expired

    def expire(self):
        self._expired = True
        self.changed.set()

    def begin_cleanup(self):
        if self.request_deadline is None:
            try:
                self.request_deadline = self.supervisor.new_deadline()
            except RuntimeError:
                # No trustworthy remaining time: conservatively stop waiting.
                self.expire()
                return self.deadline
            self.changed.set()
        return self.deadline

    @staticmethod
    def consume(task):
        if not task.cancelled():
            task.exception()

    def track(self, role, task):
        self.tasks[role] = task
        task.add_done_callback(self.consume)
        task.add_done_callback(lambda _: self.supervisor._observe(self))
        return task

    def launch(self, owner, factory):
        self.supervisor.now()
        if self not in self.supervisor.scopes or self.runner is not None:
            raise RuntimeError("A registered unused lifecycle scope is required.")
        if self.expired:
            self.supervisor.scopes.discard(self)
            self.supervisor.membership_changed.set()
            raise AdmissionClosed("Lifecycle cutoff has expired.")
        self.owner = owner

        async def enter():
            return await factory()

        scope = self

        class Runner(asyncio.Task):
            def cancel(self, msg=None):
                if not self.done():
                    scope.begin_cleanup()
                return super().cancel(msg)

        self.runner = self.track("runner", Runner(enter(), loop=self.supervisor.loop))
        self.runner.add_done_callback(self._runner_finished)
        self.supervisor.membership_changed.set()
        return self.runner

    def _runner_finished(self, task):
        # Also owns cancellation before the coroutine's first instruction.
        if task.cancelled() and not self.owner._started:
            self.begin_cleanup()
        self.refresh()

    def refresh(self):
        if self.runner is None or not self.runner.done():
            return
        pending = any(not task.done() for task in self.tasks.values())
        unresolved = self.owner is not None and self.owner.release_obligation in {
            "not_attempted",
            "attempted_unconfirmed",
        }
        unresolved = unresolved or (
            getattr(self.owner, "circuit_obligation", None) == "unresolved"
            or getattr(self.owner, "renewal_obligation", None) == "unresolved"
            or "cleanup_failed" in getattr(self.owner, "cleanup_failures", ())
        )
        if pending or unresolved:
            self.supervisor.residuals.add(self)
        else:
            self.supervisor.residuals.discard(self)
        # Explicit residual transfer precedes deregistration.
        self.supervisor.scopes.discard(self)
        self.supervisor.membership_changed.set()

    async def wait_expired(self):
        while not self.expired:
            self.changed.clear()
            tasks = [
                asyncio.create_task(self.changed.wait()),
                asyncio.create_task(self.notification.wait()),
            ]
            deadline = self.deadline
            if deadline is not None:
                tasks.append(asyncio.create_task(self.supervisor.clock.until(deadline)))
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if tasks[1] in done:
                    self.expire()  # Completed notification remains truth after clear.
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def join(self, *tasks):
        pending = {task for task in tasks if task is not None and not task.done()}
        if not pending:
            return True
        expiry = asyncio.create_task(self.wait_expired())
        try:
            while pending and not self.expired:
                done, _ = await asyncio.wait(
                    (*pending, expiry), return_when=asyncio.FIRST_COMPLETED
                )
                pending.difference_update(done)
                if expiry in done:
                    break
            if pending:
                for task in pending:
                    if not task.cancelling():
                        task.cancel()
                return False
            return True
        finally:
            expiry.cancel()
            await asyncio.gather(expiry, return_exceptions=True)
