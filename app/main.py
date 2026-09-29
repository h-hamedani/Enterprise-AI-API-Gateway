from __future__ import annotations

import asyncio
import base64
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.api.control_plane import (
    admin_read_router,
    llm_registry_router,
    normal_api_registry_router,
    permission_router,
    price_router,
)
from app.api.control_plane import router as control_plane_router
from app.api.health import router as health_router
from app.control_plane.admin_reads import create_admin_read_service
from app.control_plane.application_api_keys import create_control_plane_admin_services
from app.control_plane.config_publish import RedisConfigInvalidationPublisher
from app.control_plane.config_reconciler import (
    ConfigReconciler,
    ConfigVersionRepository,
)
from app.control_plane.config_subscriber import (
    InvalidationRegistry,
    RedisInvalidationSubscriber,
)
from app.control_plane.llm_registry import create_llm_registry_service
from app.control_plane.normal_api_registry import create_normal_api_registry_service
from app.control_plane.protection import ControlPlaneProtection
from app.core.config import get_settings
from app.core.errors import install_error_handlers
from app.core.request_context import request_context_middleware
from app.redis.circuit import CircuitConfig, RedisCircuitStore
from app.redis.circuit_cleanup import CircuitAuthorityRepository, CircuitOrphanSweeper
from app.redis.concurrency import RedisConcurrencySemaphore
from app.redis.local_degraded import LocalDegradedProtection
from app.redis.protection_facade import (
    RecoveryCircuitStore,
    RecoveryConcurrencySemaphore,
    RecoveryRateLimiter,
)
from app.redis.rate_limit import RedisTokenBucket
from app.redis.recovery import RecoveryCoordinator
from app.redis.runtime import RedisRuntime

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    db_engine: AsyncEngine = create_async_engine(
        settings.postgres_dsn,
        pool_pre_ping=True,
    )

    redis_runtime = RedisRuntime(
        settings.redis_url,
        settings.dependency_timeout_seconds,
    )
    await redis_runtime.start()
    redis = redis_runtime.client

    app.state.db_engine = db_engine
    app.state.redis = redis
    app.state.redis_runtime = redis_runtime

    def new_local_generation() -> LocalDegradedProtection:
        return LocalDegradedProtection(
            max_entries=settings.degraded_local_max_entries_per_store,
            lease_duration_ms=settings.concurrency_lease_duration_ms,
        )

    app.state.local_degraded_protection = new_local_generation()
    app.state.config_invalidation_publisher = RedisConfigInvalidationPublisher(redis)
    invalidation_registry = InvalidationRegistry()
    invalidation_subscriber = RedisInvalidationSubscriber(redis, invalidation_registry)
    config_reconciler = ConfigReconciler(
        invalidation_registry, ConfigVersionRepository(db_engine).lookup
    )
    app.state.invalidation_registry = invalidation_registry
    app.state.invalidation_subscriber = invalidation_subscriber
    app.state.config_reconciler = config_reconciler
    recovery = RecoveryCoordinator(
        redis_runtime,
        invalidation_subscriber,
        config_reconciler,
        invalidation_registry,
        local_factory=new_local_generation,
        initial_local=app.state.local_degraded_protection,
        lease_duration_ms=settings.concurrency_lease_duration_ms,
    )
    app.state.recovery_coordinator = recovery
    circuit_config = CircuitConfig.from_settings(settings)
    app.state.recovery_rate_limiter = RecoveryRateLimiter(
        RedisTokenBucket(redis_runtime), recovery
    )
    app.state.recovery_concurrency = RecoveryConcurrencySemaphore(
        RedisConcurrencySemaphore(
            redis_runtime, settings.concurrency_lease_duration_ms
        ),
        recovery,
    )
    app.state.recovery_circuit = RecoveryCircuitStore(
        RedisCircuitStore(redis_runtime, circuit_config),
        recovery,
        circuit_config,
        normal_ttl_ms=settings.local_circuit_normal_completion_ttl_ms,
    )
    circuit_orphan_sweeper = CircuitOrphanSweeper(
        redis, CircuitAuthorityRepository(db_engine).lookup
    )
    app.state.circuit_orphan_sweeper = circuit_orphan_sweeper
    if settings.credential_hmac_secret is not None:
        app.state.control_plane_protection = ControlPlaneProtection(
            redis_runtime,
            settings,
            local=app.state.local_degraded_protection,
            recovery=recovery,
        )
    await invalidation_subscriber.start()
    await config_reconciler.start()
    await recovery.start()
    await circuit_orphan_sweeper.start()
    app.state.runtime_mode = "NORMAL"
    if (
        settings.credential_hmac_secret is not None
        and settings.idempotency_hmac_secret is not None
        and settings.encryption_current_key_version is not None
    ):
        app.state.control_plane_admin_services = create_control_plane_admin_services(
            credential_hmac_key=base64.b64decode(settings.credential_hmac_secret),
            idempotency_hmac_key=base64.b64decode(settings.idempotency_hmac_secret),
            encryption_keys={
                version: base64.b64decode(encoded)
                for version, encoded in settings.encryption_keys.items()
            },
            current_encryption_key_version=settings.encryption_current_key_version,
        )
        app.state.admin_read_service = create_admin_read_service(
            base64.b64decode(settings.idempotency_hmac_secret)
        )
        app.state.normal_api_registry_service = create_normal_api_registry_service(
            idempotency_hmac_key=base64.b64decode(settings.idempotency_hmac_secret),
            encryption_keys={
                version: base64.b64decode(encoded)
                for version, encoded in settings.encryption_keys.items()
            },
            current_encryption_key_version=settings.encryption_current_key_version,
        )
        app.state.llm_registry_service = create_llm_registry_service(
            base64.b64decode(settings.idempotency_hmac_secret),
            {
                version: base64.b64decode(encoded)
                for version, encoded in settings.encryption_keys.items()
            },
            settings.encryption_current_key_version,
        )

    try:
        yield
    finally:
        await circuit_orphan_sweeper.stop()
        await recovery.stop()
        await config_reconciler.stop()
        await invalidation_subscriber.stop()
        await redis_runtime.close()
        await db_engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    app.middleware("http")(request_context_middleware)
    install_error_handlers(app)
    app.include_router(health_router)
    app.include_router(control_plane_router)
    app.include_router(admin_read_router)
    app.include_router(permission_router)
    app.include_router(normal_api_registry_router)
    app.include_router(llm_registry_router)
    app.include_router(price_router)

    return app


app = create_app()
