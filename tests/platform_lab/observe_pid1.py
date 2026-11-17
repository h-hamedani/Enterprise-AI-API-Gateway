"""Test-only PID-1 observer; delegates to the unmodified managed entrypoint."""

import json
import os
import runpy
import sys
import time

from app.normal_api.supervisor import LifecycleSupervisor
from app.redis.runtime import RedisRuntime

owner = None
runtime = None
initialize = LifecycleSupervisor.__init__
begin = LifecycleSupervisor.begin_shutdown
close = RedisRuntime.close


def emit(event, **values):
    print(
        "LAB "
        + json.dumps(
            {
                "event": event,
                "pid": os.getpid(),
                "monotonic": time.monotonic(),
                **values,
            }
        ),
        flush=True,
    )


def observed_initialize(self, *args, **kwargs):
    global owner
    initialize(self, *args, **kwargs)
    owner = self
    emit("supervisor", grace=self.grace_seconds)


def observed_begin(self, *args, **kwargs):
    result = begin(self, *args, **kwargs)
    emit("shutdown", deadline=self.process_deadline)
    return result


async def observed_close(self, *args, **kwargs):
    global runtime
    runtime = self
    emit("redis_close_begin", deadline=owner.process_deadline)
    try:
        return await close(self, *args, **kwargs)
    finally:
        emit("redis_disposal", states=self.disposal_states)


LifecycleSupervisor.__init__ = observed_initialize
LifecycleSupervisor.begin_shutdown = observed_begin
RedisRuntime.close = observed_close
emit("process", ppid=os.getppid())
sys.argv = ["app.server", "--host", "0.0.0.0", "--port", "8000"]
try:
    runpy.run_module("app.server", run_name="__main__")
finally:
    if owner is not None:
        emit(
            "final",
            deadline=owner.process_deadline,
            active=len(owner.scopes),
            residuals=len(owner.residuals),
            unclosed=sorted(owner.unclosed_resources),
            disposal=runtime.disposal_states if runtime else None,
        )
