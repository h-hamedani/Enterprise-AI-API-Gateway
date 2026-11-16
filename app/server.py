"""Managed server entrypoint: latch R1 shutdown before Uvicorn request draining."""

import argparse
import asyncio

import uvicorn

from app.core.config import get_settings
from app.main import create_app
from app.normal_api.supervisor import LifecycleScope, LifecycleSupervisor


class LifecycleServer(uvicorn.Server):
    def __init__(self, config, supervisor):
        super().__init__(config)
        self.supervisor = supervisor
        self.listener_close_requested = set()
        self.listener_close_confirmed = set()

    def _stop_listeners(self, sockets):
        for index, listener in enumerate(getattr(self, "servers", ())):
            name = f"listener_{index}"
            if name in self.listener_close_requested:
                continue
            listener.close()
            self.listener_close_requested.add(name)
            self.supervisor.unclosed_resources.add(name)
            # close() is synchronous; wait_closed() completion is separate truth.
            task = asyncio.create_task(listener.wait_closed())
            self.supervisor.resources[name] = task

            def observed(completed, identity=name):
                LifecycleScope.consume(completed)
                if not completed.cancelled() and completed.exception() is None:
                    self.listener_close_confirmed.add(identity)
                    self.supervisor.unclosed_resources.discard(identity)

            task.add_done_callback(observed)
        for sock in sockets or ():
            sock.close()
        for connection in tuple(self.server_state.connections):
            connection.shutdown()

    def _cancel_remaining(self):
        for scope in tuple(self.supervisor.scopes | self.supervisor.residuals):
            scope.expire()
            for task in scope.tasks.values():
                if (
                    task is not asyncio.current_task()
                    and not task.done()
                    and not task.cancelling()
                ):
                    task.cancel()
        for index, request in enumerate(tuple(self.server_state.tasks)):
            self.supervisor.resources[f"server_request_{index}"] = request
            request.add_done_callback(LifecycleScope.consume)
            if not request.done() and not request.cancelling():
                request.cancel()

    def handle_exit(self, sig, frame):
        # Python signal handlers can interrupt a synchronous register() between
        # bytecodes. Marshal activation onto the owning loop so both operations
        # really share one non-interleaved synchronization boundary.
        self.supervisor.loop.call_soon(self._activate_exit, sig, frame)

    def _activate_exit(self, sig, frame):
        self.supervisor.begin_shutdown()
        super().handle_exit(sig, frame)

    async def shutdown(self, sockets=None):
        self.supervisor.begin_shutdown()
        boundary = LifecycleScope(self.supervisor)
        self.supervisor.unclosed_resources.add("application_lifespan")
        self._stop_listeners(sockets)
        if boundary.expired:
            self._cancel_remaining()
            return
        # Uvicorn has its own drain phase followed by lifespan. An outer bound
        # uses the same absolute cutoff, including the latter phase.
        task = asyncio.create_task(super().shutdown(sockets))
        self.supervisor.resources["server_shutdown"] = task
        task.add_done_callback(LifecycleScope.consume)
        if not await boundary.join(task):
            # Cancelling Uvicorn's drain waiter alone does not cancel the
            # server-owned request tasks it was draining. Retain them explicitly.
            self._cancel_remaining()
        elif not task.cancelled() and task.exception() is None and not self.force_exit:
            self.supervisor.unclosed_resources.discard("application_lifespan")


async def serve(host, port):
    settings = get_settings()
    supervisor = LifecycleSupervisor(settings.lifecycle_grace_timeout_seconds)
    app = create_app()
    app.state.lifecycle_supervisor = supervisor
    config = uvicorn.Config(app, host=host, port=port, timeout_graceful_shutdown=None)
    await LifecycleServer(config, supervisor).serve()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    asyncio.run(serve(args.host, args.port))


if __name__ == "__main__":
    main()
