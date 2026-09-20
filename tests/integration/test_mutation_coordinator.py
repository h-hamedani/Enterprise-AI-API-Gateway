from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, insert, select

from app.control_plane.application_api_keys import create_control_plane_admin_services
from app.control_plane.auth import AdminContext
from app.control_plane.config_publish import RedisConfigInvalidationPublisher
from app.control_plane.mutation_coordinator import (
    AuditAction,
    MutationCoordinationError,
    MutationCoordinator,
    ResourceType,
    response_resource_id,
)
from app.core.config import get_settings
from app.persistence.models import Base
from app.schemas.control_plane import ApiKeyCreate


@pytest.fixture
def coordinator_fixture():
    engine = create_engine(get_settings().postgres_migration_dsn)
    tenant_id, admin_user_id, admin_token_id = uuid4(), uuid4(), uuid4()
    timestamp = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]).values(
                id=tenant_id,
                name=f"m29-{tenant_id}",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        connection.execute(
            insert(Base.metadata.tables["admin_users"]).values(
                id=admin_user_id,
                tenant_id=tenant_id,
                name="m29-admin",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        connection.execute(
            insert(Base.metadata.tables["admin_tokens"]).values(
                id=admin_token_id,
                tenant_id=tenant_id,
                admin_user_id=admin_user_id,
                token_prefix="adm_m29",
                token_hash=f"hash-{admin_token_id}",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    context = AdminContext(tenant_id, admin_user_id, admin_token_id, uuid4())
    yield engine, context
    with engine.begin() as connection:
        for table_name in (
            "config_versions",
            "audit_logs",
            "idempotency_records",
            "api_keys",
            "applications",
            "admin_tokens",
            "admin_users",
        ):
            table = Base.metadata.tables[table_name]
            connection.execute(table.delete().where(table.c.tenant_id == tenant_id))
        connection.execute(
            Base.metadata.tables["tenants"]
            .delete()
            .where(Base.metadata.tables["tenants"].c.id == tenant_id)
        )
    engine.dispose()


def test_success_audit_and_version_commit_atomically(coordinator_fixture):
    engine, context = coordinator_fixture
    resource_id = uuid4()
    with engine.begin() as connection:
        committed = MutationCoordinator().record_success(
            connection,
            context=context,
            action=AuditAction.APPLICATION_CREATE,
            resource_type=ResourceType.APPLICATION,
            resource_id=resource_id,
        )

    with engine.connect() as connection:
        audit = connection.execute(
            select(Base.metadata.tables["audit_logs"]).where(
                Base.metadata.tables["audit_logs"].c.tenant_id == context.tenant_id
            )
        ).one()
        version = connection.execute(
            select(Base.metadata.tables["config_versions"]).where(
                Base.metadata.tables["config_versions"].c.tenant_id == context.tenant_id
            )
        ).one()

    assert committed.version == 1
    assert audit.actor_admin_user_id == context.admin_user_id
    assert audit.actor_admin_token_id == context.admin_token_id
    assert audit.request_id == context.request_id
    assert audit.action == AuditAction.APPLICATION_CREATE
    assert audit.resource_type == ResourceType.APPLICATION
    assert audit.resource_id == resource_id
    assert audit.result == "SUCCESS"
    assert audit.metadata is None
    assert version.version == 1
    assert version.updated_by_admin_user_id == context.admin_user_id


def test_config_failure_rolls_back_business_change_and_audit(coordinator_fixture):
    engine, context = coordinator_fixture
    applications = Base.metadata.tables["applications"]
    resource_id = uuid4()

    class FailingVersionCoordinator(MutationCoordinator):
        def _increment_config_version(self, *args, **kwargs):
            raise RuntimeError("simulated config-version failure")

    with pytest.raises(MutationCoordinationError), engine.begin() as connection:
        timestamp = datetime.now(UTC)
        connection.execute(
            insert(applications).values(
                id=resource_id,
                tenant_id=context.tenant_id,
                name="must-roll-back",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        FailingVersionCoordinator().record_success(
            connection,
            context=context,
            action=AuditAction.APPLICATION_CREATE,
            resource_type=ResourceType.APPLICATION,
            resource_id=resource_id,
        )

    with engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(applications)
                .where(applications.c.id == resource_id)
            )
            == 0
        )
        assert (
            connection.scalar(
                select(func.count())
                .select_from(Base.metadata.tables["audit_logs"])
                .where(
                    Base.metadata.tables["audit_logs"].c.tenant_id == context.tenant_id
                )
            )
            == 0
        )
        assert (
            connection.scalar(
                select(func.count())
                .select_from(Base.metadata.tables["config_versions"])
                .where(
                    Base.metadata.tables["config_versions"].c.tenant_id
                    == context.tenant_id
                )
            )
            == 0
        )


def test_audit_failure_rolls_back_business_change(coordinator_fixture):
    engine, context = coordinator_fixture
    applications = Base.metadata.tables["applications"]
    resource_id = uuid4()

    class FailingAuditCoordinator(MutationCoordinator):
        def _insert_audit(self, *args, **kwargs):
            raise RuntimeError("simulated audit failure")

    with pytest.raises(MutationCoordinationError), engine.begin() as connection:
        timestamp = datetime.now(UTC)
        connection.execute(
            insert(applications).values(
                id=resource_id,
                tenant_id=context.tenant_id,
                name="audit-must-roll-back",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        FailingAuditCoordinator().record_success(
            connection,
            context=context,
            action=AuditAction.APPLICATION_CREATE,
            resource_type=ResourceType.APPLICATION,
            resource_id=resource_id,
        )

    with engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(applications)
                .where(applications.c.id == resource_id)
            )
            == 0
        )


def test_concurrent_version_increments_are_monotonic(coordinator_fixture):
    engine, context = coordinator_fixture
    coordinator = MutationCoordinator()

    def mutate(index):
        with engine.begin() as connection:
            result = coordinator.record_success(
                connection,
                context=AdminContext(
                    context.tenant_id,
                    context.admin_user_id,
                    context.admin_token_id,
                    uuid4(),
                ),
                action=AuditAction.ROUTE_UPDATE,
                resource_type=ResourceType.ROUTE,
                resource_id=uuid4(),
            )
            return index, result.version

    with ThreadPoolExecutor(max_workers=6) as pool:
        observed = dict(pool.map(mutate, range(12)))

    with engine.connect() as connection:
        final_version = connection.scalar(
            select(Base.metadata.tables["config_versions"].c.version).where(
                Base.metadata.tables["config_versions"].c.tenant_id == context.tenant_id
            )
        )
        audit_count = connection.scalar(
            select(func.count())
            .select_from(Base.metadata.tables["audit_logs"])
            .where(Base.metadata.tables["audit_logs"].c.tenant_id == context.tenant_id)
        )

    assert sorted(observed.values()) == list(range(1, 13))
    assert final_version == 12
    assert audit_count == 12


def test_api_key_idempotent_replay_does_not_repeat_audit_or_version(
    coordinator_fixture,
):
    engine, context = coordinator_fixture
    application_id = uuid4()
    timestamp = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["applications"]).values(
                id=application_id,
                tenant_id=context.tenant_id,
                name="m29-idempotency",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )

    services = create_control_plane_admin_services(
        credential_hmac_key=b"c" * 32,
        idempotency_hmac_key=b"i" * 32,
        encryption_keys={1: b"e" * 32},
        current_encryption_key_version=1,
    )
    coordinator = MutationCoordinator()
    request = ApiKeyCreate(application_id=application_id, name="replay-safe")

    for _ in range(2):
        with engine.begin() as connection:
            result = services.api_keys.create(
                connection,
                tenant_id=context.tenant_id,
                admin_user_id=context.admin_user_id,
                request=request,
                raw_idempotency_key="m29-replay-key",
            )
            if not result.replayed:
                coordinator.record_success(
                    connection,
                    context=context,
                    action=AuditAction.API_KEY_CREATE,
                    resource_type=ResourceType.API_KEY,
                    resource_id=response_resource_id(result.response),
                )

    with engine.connect() as connection:
        api_key_count = connection.scalar(
            select(func.count())
            .select_from(Base.metadata.tables["api_keys"])
            .where(Base.metadata.tables["api_keys"].c.tenant_id == context.tenant_id)
        )
        audit_count = connection.scalar(
            select(func.count())
            .select_from(Base.metadata.tables["audit_logs"])
            .where(Base.metadata.tables["audit_logs"].c.tenant_id == context.tenant_id)
        )
        version = connection.scalar(
            select(Base.metadata.tables["config_versions"].c.version).where(
                Base.metadata.tables["config_versions"].c.tenant_id == context.tenant_id
            )
        )

    assert api_key_count == 1
    assert audit_count == 1
    assert version == 1


def test_publication_observes_committed_postgresql_state(coordinator_fixture):
    engine, context = coordinator_fixture
    application_id = uuid4()
    timestamp = datetime.now(UTC)

    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["applications"]).values(
                id=application_id,
                tenant_id=context.tenant_id,
                name="visible-after-commit",
                status="ACTIVE",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
        committed = MutationCoordinator().record_success(
            connection,
            context=context,
            action=AuditAction.APPLICATION_CREATE,
            resource_type=ResourceType.APPLICATION,
            resource_id=application_id,
        )

    class CommitObservingRedis:
        observed = None

        async def publish(self, channel, payload):
            with engine.connect() as observer:
                self.observed = (
                    observer.scalar(
                        select(func.count())
                        .select_from(Base.metadata.tables["applications"])
                        .where(
                            Base.metadata.tables["applications"].c.id == application_id
                        )
                    ),
                    observer.scalar(
                        select(Base.metadata.tables["config_versions"].c.version).where(
                            Base.metadata.tables["config_versions"].c.tenant_id
                            == context.tenant_id
                        )
                    ),
                    json.loads(payload),
                )
            return 1

    redis = CommitObservingRedis()
    assert asyncio.run(
        RedisConfigInvalidationPublisher(redis).publish(
            committed, request_id=context.request_id
        )
    )
    assert redis.observed == (
        1,
        committed.version,
        {
            "tenant_id": str(context.tenant_id),
            "resource_type": "APPLICATION",
            "resource_id": str(application_id),
            "version": committed.version,
        },
    )
