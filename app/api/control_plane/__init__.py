from app.api.control_plane.admin_reads import router as admin_read_router
from app.api.control_plane.application_api_keys import (
    router as application_api_keys_router,
)
from app.api.control_plane.llm_registry import (
    price_router,
)
from app.api.control_plane.llm_registry import (
    router as llm_registry_router,
)
from app.api.control_plane.normal_api_registry import (
    router as normal_api_registry_router,
)
from app.api.control_plane.permissions import router as permission_router

ADMIN_API_PREFIX = "/api/v1/admin"

router = application_api_keys_router

__all__ = [
    "ADMIN_API_PREFIX",
    "admin_read_router",
    "llm_registry_router",
    "normal_api_registry_router",
    "permission_router",
    "price_router",
    "router",
]
