from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

router = APIRouter(tags=["Health"])


@router.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok"}


async def _check_postgres(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False


async def _check_redis(redis: Redis) -> bool:
    try:
        return bool(await redis.ping())
    except RedisError:
        return False


@router.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    engine: AsyncEngine = request.app.state.db_engine
    redis: Redis = request.app.state.redis

    postgres_ok, redis_ok = await asyncio.gather(
        _check_postgres(engine),
        _check_redis(redis),
    )

    if postgres_ok and redis_ok:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ready",
                "postgres": "ok",
                "redis": "ok",
            },
        )

    return JSONResponse(
        status_code=503,
        content={"status": "not_ready"},
    )


@router.get("/health/traffic")
async def health_traffic(request: Request) -> JSONResponse:
    engine: AsyncEngine = request.app.state.db_engine

    postgres_ok = await _check_postgres(engine)

    if not postgres_ok:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "runtime_mode": "NOT_READY_DB",
            },
        )

    runtime_mode = getattr(
        request.app.state,
        "runtime_mode",
        "NORMAL",
    )

    if runtime_mode == "DRAINING":
        return JSONResponse(
            status_code=503,
            content={
                "status": "draining",
                "runtime_mode": "DRAINING",
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "status": "accepting",
            "runtime_mode": runtime_mode,
        },
    )
