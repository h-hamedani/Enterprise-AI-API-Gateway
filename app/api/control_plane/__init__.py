from fastapi import APIRouter

ADMIN_API_PREFIX = "/api/v1/admin"

router = APIRouter(prefix=ADMIN_API_PREFIX)

__all__ = ["ADMIN_API_PREFIX", "router"]
