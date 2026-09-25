from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.control_plane.config_subscriber import InvalidationRegistry
from app.persistence.models import Base

logger = logging.getLogger(__name__)


class ConfigVersionRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._versions = Base.metadata.tables["config_versions"]

    async def lookup(self, tenant_ids: frozenset[UUID]) -> dict[UUID, int]:
        if not tenant_ids:
            return {}
        statement = select(self._versions.c.tenant_id, self._versions.c.version).where(
            self._versions.c.tenant_id.in_(tenant_ids)
        )
        async with self._engine.connect() as connection:
            rows = (await connection.execute(statement)).all()
        versions = dict.fromkeys(tenant_ids, 0)
        versions.update(rows)
        return versions


class ConfigReconciler:
    def __init__(
        self,
        registry: InvalidationRegistry,
        lookup: Callable[[frozenset[UUID]], Awaitable[dict[UUID, int]]],
        *,
        delay_chooser: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._registry = registry
        self._lookup = lookup
        self._delay_chooser = delay_chooser or (lambda: random.uniform(27.0, 33.0))
        self._sleep = sleep
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def reconcile_once(self) -> str:
        tenant_ids = self._registry.tenant_ids()
        if not tenant_ids:
            logger.debug("Config version reconciliation outcome=empty_membership")
            return "empty_membership"
        try:
            versions = await self._lookup(tenant_ids)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - contain arbitrary database failures
            logger.warning("Config version reconciliation lookup failed")
            return "pass_error"
        failed = False
        for tenant_id in tenant_ids:
            try:
                outcome = await self._registry.reconcile(tenant_id, versions[tenant_id])
                logger.debug("Config version reconciliation outcome=%s", outcome)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - contain user callback failures
                failed = True
                logger.warning("Config version reconciliation callback failed")
        logger.debug(
            "Config version reconciliation pass=%s",
            "pass_error" if failed else "pass_success",
        )
        return "pass_error" if failed else "pass_success"

    async def start(self) -> None:
        if not self.running:
            self._task = asyncio.create_task(
                self._run(), name="config-version-reconciler"
            )

    async def stop(self) -> None:
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep the managed task alive
                logger.warning("Config version reconciliation pass failed")
            await self._sleep(self._delay_chooser())
