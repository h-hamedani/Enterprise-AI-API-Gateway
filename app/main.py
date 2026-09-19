from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.api.control_plane import router as control_plane_router
from app.api.health import router as health_router
from app.core.config import get_settings
from app.core.errors import install_error_handlers
from app.core.request_context import request_context_middleware

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    db_engine: AsyncEngine = create_async_engine(
        settings.postgres_dsn,
        pool_pre_ping=True,
    )

    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    app.state.db_engine = db_engine
    app.state.redis = redis
    app.state.runtime_mode = "NORMAL"

    try:
        yield
    finally:
        await redis.aclose()
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

    return app


app = create_app()
