from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.api.health import router as health_router
from app.core.config import get_settings
from app.core.request_context import request_context_middleware


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
    app.include_router(health_router)

    return app


app = create_app()
