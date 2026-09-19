from app.api.control_plane.application_api_keys import (
    router as application_api_keys_router,
)

ADMIN_API_PREFIX = "/api/v1/admin"

router = application_api_keys_router

__all__ = ["ADMIN_API_PREFIX", "router"]
