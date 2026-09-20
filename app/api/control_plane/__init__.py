from app.api.control_plane.application_api_keys import (
    router as application_api_keys_router,
)
from app.api.control_plane.normal_api_registry import (
    router as normal_api_registry_router,
)
from app.api.control_plane.permissions import router as permission_router

ADMIN_API_PREFIX = "/api/v1/admin"

router = application_api_keys_router

__all__ = [
    "ADMIN_API_PREFIX",
    "normal_api_registry_router",
    "permission_router",
    "router",
]
