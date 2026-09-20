from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import and_, delete, desc, insert, or_, select

from app.core.pagination import CursorPosition, PaginationCursorCodec
from app.persistence.models import Base
from app.persistence.repositories.tenant_scoped import TenantScopedRepository
from app.schemas.control_plane import (
    AliasTargetWrite,
    LlmAliasCreate,
    LlmAliasPatch,
    LlmAliasResponse,
    LlmModelCreate,
    LlmModelPatch,
    LlmModelResponse,
    LlmProviderCreate,
    LlmProviderPatch,
    LlmProviderResponse,
    LlmTargetCreate,
    LlmTargetPatch,
    LlmTargetResponse,
    ModelPriceCreate,
    ModelPricePatch,
    ModelPriceResponse,
)


class LlmRegistryNotFoundError(RuntimeError):
    pass


class LlmRegistryConflictError(RuntimeError):
    pass


class PriceWindowConflictError(LlmRegistryConflictError):
    pass


def _value(value):
    return value.value if hasattr(value, "value") else str(value)


class LlmRegistryAdminService:
    def __init__(self, cursor: PaginationCursorCodec) -> None:
        self.cursor = cursor
        self.providers = Base.metadata.tables["llm_providers"]
        self.targets = Base.metadata.tables["llm_provider_targets"]
        self.models = Base.metadata.tables["llm_models"]
        self.capabilities = Base.metadata.tables["llm_model_capabilities"]
        self.aliases = Base.metadata.tables["llm_aliases"]
        self.alias_targets = Base.metadata.tables["llm_alias_targets"]
        self.prices = Base.metadata.tables["model_prices"]

    def _get(self, connection, table, tenant_id, resource_id):
        row = TenantScopedRepository(table).get(
            connection, tenant_id=tenant_id, resource_id=resource_id
        )
        if row is None:
            raise LlmRegistryNotFoundError()
        return row

    def _update(self, connection, table, tenant_id, resource_id, values):
        if "updated_at" in table.c:
            values["updated_at"] = datetime.now(UTC)
        if not TenantScopedRepository(table).update(
            connection, tenant_id=tenant_id, resource_id=resource_id, values=values
        ):
            raise LlmRegistryNotFoundError()
        return self._get(connection, table, tenant_id, resource_id)

    def _page(self, connection, table, tenant_id, limit, cursor):
        statement = select(table).where(table.c.tenant_id == tenant_id)
        if cursor:
            pos = self.cursor.decode(cursor, tenant_id=tenant_id)
            statement = statement.where(
                or_(
                    table.c.created_at < pos.created_at,
                    and_(
                        table.c.created_at == pos.created_at,
                        table.c.id < pos.resource_id,
                    ),
                )
            )
        rows = (
            connection.execute(
                statement.order_by(desc(table.c.created_at), desc(table.c.id)).limit(
                    limit + 1
                )
            )
            .mappings()
            .all()
        )
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = self.cursor.encode(
                CursorPosition(tenant_id, last["created_at"], last["id"])
            )
        return page, next_cursor

    def provider(self, row):
        return LlmProviderResponse(
            id=row["id"],
            name=row["name"],
            provider_type=_value(row["provider_type"]),
            status=_value(row["status"]),
        )

    def create_provider(self, connection, tenant_id, request: LlmProviderCreate):
        now, resource_id = datetime.now(UTC), uuid4()
        connection.execute(
            insert(self.providers).values(
                id=resource_id,
                tenant_id=tenant_id,
                **request.model_dump(),
                created_at=now,
                updated_at=now,
            )
        )
        return self.get_provider(connection, tenant_id, resource_id)

    def get_provider(self, connection, tenant_id, resource_id):
        return self.provider(
            self._get(connection, self.providers, tenant_id, resource_id)
        )

    def patch_provider(
        self, connection, tenant_id, resource_id, patch: LlmProviderPatch
    ):
        return self.provider(
            self._update(
                connection,
                self.providers,
                tenant_id,
                resource_id,
                patch.model_dump(exclude_unset=True),
            )
        )

    def list_providers(self, connection, tenant_id, limit, cursor):
        rows, token = self._page(connection, self.providers, tenant_id, limit, cursor)
        return {"data": [self.provider(r) for r in rows], "next_cursor": token}

    def target(self, row):
        return LlmTargetResponse(
            id=row["id"],
            provider_id=row["provider_id"],
            name=row["name"],
            base_url=row["base_url"],
            timeout_ms=row["timeout_ms"],
            max_concurrent_requests=row["max_concurrent_requests"],
            status=_value(row["status"]),
            pre_output_idle_timeout_ms=row["pre_output_idle_timeout_ms"],
            pre_output_budget_ms=row["pre_output_budget_ms"],
            post_output_idle_timeout_ms=row["post_output_idle_timeout_ms"],
            allow_uncertified_runtime=row["allow_uncertified_runtime"],
            certification_status=_value(row["certification_status"]),
            certified_at=row["certified_at"],
            certification_metadata=row["certification_metadata"],
        )

    def create_target(self, connection, tenant_id, request: LlmTargetCreate):
        self._get(connection, self.providers, tenant_id, request.provider_id)
        now, resource_id = datetime.now(UTC), uuid4()
        connection.execute(
            insert(self.targets).values(
                id=resource_id,
                tenant_id=tenant_id,
                **request.model_dump(),
                certification_status="UNVERIFIED",
                certified_at=None,
                certification_metadata=None,
                created_at=now,
                updated_at=now,
            )
        )
        return self.get_target(connection, tenant_id, resource_id)

    def get_target(self, connection, tenant_id, resource_id):
        return self.target(self._get(connection, self.targets, tenant_id, resource_id))

    def patch_target(self, connection, tenant_id, resource_id, patch: LlmTargetPatch):
        row = self._get(connection, self.targets, tenant_id, resource_id)
        values = patch.model_dump(exclude_unset=True)
        idle = values.get(
            "pre_output_idle_timeout_ms", row["pre_output_idle_timeout_ms"]
        )
        budget = values.get("pre_output_budget_ms", row["pre_output_budget_ms"])
        if budget <= idle:
            raise ValueError("Invalid streaming timeout configuration.")
        return self.target(
            self._update(connection, self.targets, tenant_id, resource_id, values)
        )

    def list_targets(self, connection, tenant_id, limit, cursor):
        rows, token = self._page(connection, self.targets, tenant_id, limit, cursor)
        return {"data": [self.target(r) for r in rows], "next_cursor": token}

    def model(self, connection, tenant_id, row):
        caps = (
            connection.execute(
                select(self.capabilities.c.capability)
                .where(
                    self.capabilities.c.tenant_id == tenant_id,
                    self.capabilities.c.model_id == row["id"],
                )
                .order_by(self.capabilities.c.capability)
            )
            .scalars()
            .all()
        )
        return LlmModelResponse(
            id=row["id"],
            provider_target_id=row["provider_target_id"],
            provider_model_name=row["provider_model_name"],
            display_name=row["display_name"],
            status=_value(row["status"]),
            capabilities=[_value(c) for c in caps],
        )

    def create_model(self, connection, tenant_id, request: LlmModelCreate):
        self._get(connection, self.targets, tenant_id, request.provider_target_id)
        if connection.execute(
            select(self.aliases.c.id).where(
                self.aliases.c.tenant_id == tenant_id,
                self.aliases.c.name == request.provider_model_name,
            )
        ).first():
            raise LlmRegistryConflictError()
        now, resource_id = datetime.now(UTC), uuid4()
        connection.execute(
            insert(self.models).values(
                id=resource_id,
                tenant_id=tenant_id,
                **request.model_dump(),
                created_at=now,
                updated_at=now,
            )
        )
        return self.get_model(connection, tenant_id, resource_id)

    def get_model(self, connection, tenant_id, resource_id):
        return self.model(
            connection,
            tenant_id,
            self._get(connection, self.models, tenant_id, resource_id),
        )

    def patch_model(self, connection, tenant_id, resource_id, patch: LlmModelPatch):
        return self.model(
            connection,
            tenant_id,
            self._update(
                connection,
                self.models,
                tenant_id,
                resource_id,
                patch.model_dump(exclude_unset=True),
            ),
        )

    def list_models(self, connection, tenant_id, limit, cursor):
        rows, token = self._page(connection, self.models, tenant_id, limit, cursor)
        return {
            "data": [self.model(connection, tenant_id, r) for r in rows],
            "next_cursor": token,
        }

    def replace_capabilities(self, connection, tenant_id, model_id, capabilities):
        self._get(connection, self.models, tenant_id, model_id)
        connection.execute(
            delete(self.capabilities).where(
                self.capabilities.c.tenant_id == tenant_id,
                self.capabilities.c.model_id == model_id,
            )
        )
        now = datetime.now(UTC)
        if capabilities:
            connection.execute(
                insert(self.capabilities),
                [
                    {
                        "tenant_id": tenant_id,
                        "model_id": model_id,
                        "capability": item,
                        "created_at": now,
                    }
                    for item in capabilities
                ],
            )

    def alias(self, connection, tenant_id, row):
        targets = (
            connection.execute(
                select(self.alias_targets)
                .where(
                    self.alias_targets.c.tenant_id == tenant_id,
                    self.alias_targets.c.alias_id == row["id"],
                    self.alias_targets.c.enabled.is_(True),
                )
                .order_by(self.alias_targets.c.priority)
            )
            .mappings()
            .all()
        )
        return LlmAliasResponse(
            id=row["id"],
            name=row["name"],
            status=_value(row["status"]),
            targets=[
                AliasTargetWrite(model_id=t["model_id"], priority=t["priority"])
                for t in targets
            ],
        )

    def create_alias(self, connection, tenant_id, request: LlmAliasCreate):
        if connection.execute(
            select(self.models.c.id).where(
                self.models.c.tenant_id == tenant_id,
                self.models.c.provider_model_name == request.name,
            )
        ).first():
            raise LlmRegistryConflictError()
        now, resource_id = datetime.now(UTC), uuid4()
        connection.execute(
            insert(self.aliases).values(
                id=resource_id,
                tenant_id=tenant_id,
                **request.model_dump(),
                created_at=now,
                updated_at=now,
            )
        )
        return self.get_alias(connection, tenant_id, resource_id)

    def get_alias(self, connection, tenant_id, resource_id):
        return self.alias(
            connection,
            tenant_id,
            self._get(connection, self.aliases, tenant_id, resource_id),
        )

    def patch_alias(self, connection, tenant_id, resource_id, patch: LlmAliasPatch):
        if (
            patch.name is not None
            and connection.execute(
                select(self.models.c.id).where(
                    self.models.c.tenant_id == tenant_id,
                    self.models.c.provider_model_name == patch.name,
                )
            ).first()
        ):
            raise LlmRegistryConflictError()
        return self.alias(
            connection,
            tenant_id,
            self._update(
                connection,
                self.aliases,
                tenant_id,
                resource_id,
                patch.model_dump(exclude_unset=True),
            ),
        )

    def list_aliases(self, connection, tenant_id, limit, cursor):
        rows, token = self._page(connection, self.aliases, tenant_id, limit, cursor)
        return {
            "data": [self.alias(connection, tenant_id, r) for r in rows],
            "next_cursor": token,
        }

    def replace_alias_targets(self, connection, tenant_id, alias_id, targets):
        self._get(connection, self.aliases, tenant_id, alias_id)
        for item in targets:
            self._get(connection, self.models, tenant_id, item.model_id)
        connection.execute(
            delete(self.alias_targets).where(
                self.alias_targets.c.tenant_id == tenant_id,
                self.alias_targets.c.alias_id == alias_id,
            )
        )
        now = datetime.now(UTC)
        connection.execute(
            insert(self.alias_targets),
            [
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "alias_id": alias_id,
                    "model_id": item.model_id,
                    "priority": item.priority,
                    "enabled": True,
                    "created_at": now,
                }
                for item in targets
            ],
        )

    def price(self, row):
        return ModelPriceResponse(
            id=row["id"],
            model_id=row["model_id"],
            input_price_per_unit=row["input_price_per_unit"],
            output_price_per_unit=row["output_price_per_unit"],
            unit_tokens=row["unit_tokens"],
            currency=row["currency"].strip(),
            effective_from=row["effective_from"],
            effective_to=row["effective_to"],
        )

    def _validate_overlap(
        self, connection, tenant_id, model_id, start, end, exclude=None
    ):
        q = select(self.prices.c.id).where(
            self.prices.c.tenant_id == tenant_id,
            self.prices.c.model_id == model_id,
            or_(
                self.prices.c.effective_to.is_(None), self.prices.c.effective_to > start
            ),
        )
        if end is not None:
            q = q.where(self.prices.c.effective_from < end)
        if exclude:
            q = q.where(self.prices.c.id != exclude)
        if connection.execute(q).first():
            raise PriceWindowConflictError()

    def create_price(self, connection, tenant_id, model_id, request: ModelPriceCreate):
        self._get(connection, self.models, tenant_id, model_id)
        self._validate_overlap(
            connection,
            tenant_id,
            model_id,
            request.effective_from,
            request.effective_to,
        )
        resource_id = uuid4()
        connection.execute(
            insert(self.prices).values(
                id=resource_id,
                tenant_id=tenant_id,
                model_id=model_id,
                **request.model_dump(),
                created_at=datetime.now(UTC),
            )
        )
        return self.get_price(connection, tenant_id, resource_id)

    def get_price(self, connection, tenant_id, resource_id):
        return self.price(self._get(connection, self.prices, tenant_id, resource_id))

    def patch_price(self, connection, tenant_id, resource_id, patch: ModelPricePatch):
        row = self._get(connection, self.prices, tenant_id, resource_id)
        values = patch.model_dump(exclude_unset=True)
        start = values.get("effective_from", row["effective_from"])
        end = values.get("effective_to", row["effective_to"])
        if end is not None and end <= start:
            raise ValueError("Invalid effective window.")
        self._validate_overlap(
            connection, tenant_id, row["model_id"], start, end, resource_id
        )
        return self.price(
            self._update(connection, self.prices, tenant_id, resource_id, values)
        )

    def list_prices(self, connection, tenant_id, model_id):
        self._get(connection, self.models, tenant_id, model_id)
        rows = (
            connection.execute(
                select(self.prices)
                .where(
                    self.prices.c.tenant_id == tenant_id,
                    self.prices.c.model_id == model_id,
                )
                .order_by(desc(self.prices.c.effective_from))
            )
            .mappings()
            .all()
        )
        return [self.price(r) for r in rows]


def create_llm_registry_service(signing_key: bytes):
    key = hmac.new(signing_key, b"llm-registry-pagination-v1", hashlib.sha256).digest()
    return LlmRegistryAdminService(PaginationCursorCodec(key))
