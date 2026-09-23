from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, insert

from app.control_plane.protection import enabled_admin_token_policies_sync
from app.core.config import get_settings
from app.persistence.models import Base


@pytest_asyncio.fixture
async def policy_database():
    settings = get_settings()
    sync = create_engine(settings.postgres_migration_dsn)
    tenant_a, tenant_b, token_a, token_b = uuid4(), uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    with sync.connect() as connection:
        transaction = connection.begin()
        try:
            tenants = Base.metadata.tables["tenants"]
            policies = Base.metadata.tables["rate_limit_policies"]
            connection.execute(
                insert(tenants),
                [
                    {
                        "id": tenant_a,
                        "name": "m35-a",
                        "status": "ACTIVE",
                        "created_at": now,
                        "updated_at": now,
                    },
                    {
                        "id": tenant_b,
                        "name": "m35-b",
                        "status": "ACTIVE",
                        "created_at": now,
                        "updated_at": now,
                    },
                ],
            )
            values = {
                "id": uuid4(),
                "scope_type": "ADMIN_TOKEN",
                "requests_per_window": 7,
                "window_seconds": 30,
                "max_concurrency": None,
                "degraded_factor": Decimal("0.25"),
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(
                insert(policies),
                [
                    {
                        **values,
                        "tenant_id": tenant_a,
                        "scope_id": token_a,
                        "enabled": True,
                    },
                    {
                        **values,
                        "id": uuid4(),
                        "tenant_id": tenant_a,
                        "scope_id": token_b,
                        "enabled": True,
                    },
                    {
                        **values,
                        "id": uuid4(),
                        "tenant_id": tenant_a,
                        "scope_id": token_a,
                        "enabled": False,
                    },
                    {
                        **values,
                        "id": uuid4(),
                        "tenant_id": tenant_b,
                        "scope_id": token_a,
                        "enabled": True,
                    },
                    {
                        **values,
                        "id": uuid4(),
                        "tenant_id": tenant_a,
                        "scope_id": uuid4(),
                        "scope_type": "ROUTE",
                        "enabled": True,
                    },
                ],
            )
            yield connection, tenant_a, tenant_b, token_a, token_b
        finally:
            transaction.rollback()
            sync.dispose()


@pytest.mark.asyncio
async def test_admin_policy_resolution_is_enabled_tenant_and_token_scoped(
    policy_database,
):
    connection, tenant_a, tenant_b, token_a, token_b = policy_database
    policies = enabled_admin_token_policies_sync(connection, tenant_a, token_a)
    assert len(policies) == 1
    assert policies[0].scope_id == token_a
    assert policies[0].requests_per_window == 7
    assert enabled_admin_token_policies_sync(connection, tenant_a, token_b)
    assert enabled_admin_token_policies_sync(connection, tenant_b, uuid4()) == []
