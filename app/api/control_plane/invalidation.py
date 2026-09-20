from __future__ import annotations

import logging
from uuid import UUID

from fastapi import Request

from app.control_plane.config_publish import InvalidationPublisher
from app.control_plane.mutation_coordinator import CommittedMutation

logger = logging.getLogger(__name__)


async def publish_committed_mutation(
    request: Request,
    mutation: CommittedMutation,
    *,
    request_id: UUID,
) -> bool:
    publisher: InvalidationPublisher | None = getattr(
        request.app.state, "config_invalidation_publisher", None
    )
    if publisher is None:
        logger.error(
            "Config invalidation publisher is unavailable",
            extra={
                "request_id": str(request_id),
                "tenant_id": str(mutation.tenant_id),
                "config_version": mutation.version,
                "resource_type": mutation.resource_type.value,
                "resource_id": str(mutation.resource_id),
                "publication_outcome": "UNAVAILABLE",
            },
        )
        return False
    try:
        return await publisher.publish(mutation, request_id=request_id)
    except Exception:  # noqa: BLE001 - publication is best-effort after commit
        logger.error(
            "Config invalidation publisher failed unexpectedly",
            extra={
                "request_id": str(request_id),
                "tenant_id": str(mutation.tenant_id),
                "config_version": mutation.version,
                "resource_type": mutation.resource_type.value,
                "resource_id": str(mutation.resource_id),
                "publication_outcome": "FAILED",
            },
        )
        return False
