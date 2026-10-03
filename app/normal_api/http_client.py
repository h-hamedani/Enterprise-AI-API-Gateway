"""Process-local Normal API upstream HTTP client ownership."""

import httpx

from app.core.config import Settings


def create_normal_api_upstream_client(settings: Settings) -> httpx.AsyncClient:
    limits = httpx.Limits(
        max_connections=settings.normal_api_upstream_max_connections,
        max_keepalive_connections=(
            settings.normal_api_upstream_max_keepalive_connections
        ),
    )
    return httpx.AsyncClient(limits=limits)
