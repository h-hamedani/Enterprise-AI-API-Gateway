"""Exact Redis availability classification for traffic protection."""

import asyncio

from redis import exceptions as redis_errors

_AVAILABILITY_TYPES = {
    redis_errors.ConnectionError,
    redis_errors.TimeoutError,
    asyncio.TimeoutError,
    redis_errors.BusyLoadingError,
    redis_errors.MaxConnectionsError,
}


def is_redis_availability_failure(exc: BaseException) -> bool:
    """Exclude auth/ACL subclasses of redis-py's ConnectionError."""
    return type(exc) in _AVAILABILITY_TYPES
