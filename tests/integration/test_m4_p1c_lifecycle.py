"""Real PostgreSQL identity/projection and Redis facades; no HTTP forwarding."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.core.config import get_settings
from app.core.request_context import new_request_id
from app.normal_api.lifecycle import (
    End,
    Hooks,
    Policies,
    RequestLifecycle,
    UpstreamOutcome,
)
from app.normal_api.supervisor import LifecycleSupervisor
from app.persistence.models.enums import RateScopeType
from app.redis.circuit import CircuitConfig, CircuitState, RedisCircuitStore
from app.redis.concurrency import RedisConcurrencySemaphore, ResolvedConcurrencyPolicy
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.protection_facade import (
    RecoveryCircuitStore,
    RecoveryConcurrencySemaphore,
    RecoveryRateLimiter,
)
from app.redis.rate_limit import RedisTokenBucket, ResolvedRatePolicy
from app.redis.recovery import (
    Backend,
    CircuitCompletionDisposition,
    RecoveryCoordinator,
)
from app.redis.runtime import RedisRuntime
from tests.integration.test_m4_p1b_resolution import request_scope, world


@pytest.mark.asyncio
@pytest.mark.parametrize("local", [False, True])
async def test_real_facades_two_tenants_and_original_backend_cleanup(local):
    settings = get_settings()
    supervisor = LifecycleSupervisor(30)
    runtime = RedisRuntime(settings.redis_url, settings.dependency_timeout_seconds)
    await runtime.start()
    ttl = 5000
    local_factory = lambda: LocalDegradedProtection(lease_duration_ms=ttl)
    coordinator = RecoveryCoordinator(
        runtime,
        SimpleNamespace(ready=False),
        None,
        None,
        local_factory=local_factory,
        lease_duration_ms=ttl,
    )
    rate = RecoveryRateLimiter(RedisTokenBucket(runtime), coordinator)
    concurrency = RecoveryConcurrencySemaphore(
        RedisConcurrencySemaphore(runtime, ttl), coordinator
    )
    config = CircuitConfig.from_settings(settings)
    circuit = RecoveryCircuitStore(
        RedisCircuitStore(runtime, config), coordinator, config
    )
    if local:
        coordinator.mark_unreachable()
    tenant_ids = []
    try:
        async with world() as a, world() as b:
            tenant_ids = [a["tenant"], b["tenant"]]
            entered, finish = asyncio.Event(), asyncio.Event()
            held = None

            def make(context):
                return RequestLifecycle(
                    context["resolver"],
                    rate,
                    concurrency,
                    circuit,
                    lease_duration_ms=ttl,
                    dependency_timeout_seconds=settings.dependency_timeout_seconds,
                )

            def hooks(context, *, hold=False):
                async def policies(resolved):
                    return Policies(
                        (
                            ResolvedRatePolicy(
                                uuid4(),
                                context["tenant"],
                                RateScopeType.SERVICE,
                                context["service"],
                                100,
                                60,
                            ),
                        ),
                        (
                            ResolvedConcurrencyPolicy(
                                uuid4(),
                                context["tenant"],
                                RateScopeType.SERVICE,
                                context["service"],
                                1,
                            ),
                        ),
                    )

                async def body(resolved, receiver):
                    await receiver.next_body_message()

                async def prepare(resolved, entity):
                    return None

                async def execute(owner, prepared):
                    nonlocal held
                    if hold:
                        held = owner.handle
                        entered.set()
                        await finish.wait()
                    owner.observe_upstream(UpstreamOutcome.complete_http(200))

                async def deliver(owner):
                    owner.committed_downstream()

                return Hooks(policies, body, prepare, execute, deliver)

            def receive():
                first = True

                async def call():
                    nonlocal first
                    if first:
                        first = False
                        return {"type": "http.request", "body": b""}
                    await asyncio.Event().wait()

                return call

            first = make(a)
            task = asyncio.ensure_future(
                first.run(
                    request_scope(a),
                    lifecycle_scope=supervisor.register(),
                    request_id=new_request_id(),
                    receive=receive(),
                    hooks=hooks(a, hold=True),
                )
            )
            try:
                await asyncio.wait_for(entered.wait(), 5)
                assert held.backend == (Backend.LOCAL if local else Backend.REDIS)
                denied = make(a)
                result = await denied.run(
                    request_scope(a),
                    lifecycle_scope=supervisor.register(),
                    request_id=new_request_id(),
                    receive=receive(),
                    hooks=hooks(a),
                )
                assert result.end == End.REJECTED and result.error.status_code == 429
                assert denied.circuit_handle is None and not denied.dispatched
                other = make(b)
                result = await other.run(
                    request_scope(b),
                    lifecycle_scope=supervisor.register(),
                    request_id=new_request_id(),
                    receive=receive(),
                    hooks=hooks(b),
                )
                assert result.end == End.DELIVERED
                assert (
                    other.resolved.identity.tenant_id
                    != first.resolved.identity.tenant_id
                )
                assert (
                    await concurrency.renew(first.policies.concurrency, held)
                ).renewed
                if not local:
                    # New admissions become local; this known Redis lease and
                    # circuit token must still complete against Redis only.
                    coordinator.mark_unreachable()
                finish.set()
                assert (await asyncio.wait_for(task, 5)).end == End.DELIVERED
                assert first.handle is held
                assert not first.cleanup_failures
                assert (
                    first.completion_result.disposition
                    == CircuitCompletionDisposition.APPLIED
                )
                assert first.completion_result.resulting_state == CircuitState.CLOSED
                assert not (
                    await concurrency.release(first.policies.concurrency, held)
                ).released
                assert first._renewal.done() and first.receiver.task.done()
            finally:
                finish.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        # Remove only keys belonging to these test-generated tenant UUIDs.
        for tenant in tenant_ids:
            keys = [
                key
                async for key in runtime.client.scan_iter(match=f"gw:v1:*{{{tenant}}}*")
            ]
            if keys:
                await runtime.client.delete(*keys)
        await runtime.close()


@pytest.mark.asyncio
async def test_foreign_policy_rejected_before_any_protection():
    async with world() as context:

        class Forbidden:
            def __getattr__(self, name):
                pytest.fail("Foreign policy reached protection")

        async def policies(resolved):
            return Policies(
                (),
                (
                    ResolvedConcurrencyPolicy(
                        uuid4(), uuid4(), RateScopeType.SERVICE, context["service"], 1
                    ),
                ),
            )

        async def forbidden(*args):
            pytest.fail("Foreign policy reached body/transport")

        lifecycle = RequestLifecycle(
            context["resolver"],
            Forbidden(),
            Forbidden(),
            Forbidden(),
            lease_duration_ms=5000,
            dependency_timeout_seconds=1,
        )
        result = await lifecycle.run(
            request_scope(context),
            lifecycle_scope=LifecycleSupervisor(30).register(),
            request_id=new_request_id(),
            receive=forbidden,
            hooks=Hooks(policies, forbidden, forbidden, forbidden, forbidden),
        )
        assert result.end == End.INTERNAL and result.error.status_code == 500


def test_no_public_endpoint_or_new_lifespan_worker():
    from app.main import create_app

    assert not any(
        getattr(route, "path", "").startswith("/api/") for route in create_app().routes
    )
