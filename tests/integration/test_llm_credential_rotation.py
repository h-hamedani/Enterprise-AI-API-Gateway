import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, insert, select

from app.control_plane.llm_registry import (
    LlmRegistryNotFoundError,
    create_llm_registry_service,
)
from app.core.config import get_settings
from app.core.idempotency import IdempotencyConflictError
from app.core.security.secrets import EncryptedSecret, SecretService
from app.persistence.models import Base
from app.schemas.control_plane import (
    LlmProviderCreate,
    LlmTargetCreate,
    ProviderCredentialWrite,
)


def test_rotation_is_encrypted_tenant_scoped_atomic_and_idempotent():
    engine = create_engine(get_settings().postgres_migration_dsn)
    connection = engine.connect()
    transaction = connection.begin()
    now = datetime.now(UTC)
    tenant_a, tenant_b, admin_id, admin_b = uuid4(), uuid4(), uuid4(), uuid4()
    connection.execute(
        insert(Base.metadata.tables["tenants"]),
        [
            {
                "id": t,
                "name": f"m28-{t}",
                "status": "ACTIVE",
                "created_at": now,
                "updated_at": now,
            }
            for t in (tenant_a, tenant_b)
        ],
    )
    connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=admin_id,
            tenant_id=tenant_a,
            name="m28-admin",
            status="ACTIVE",
            created_at=now,
            updated_at=now,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["admin_users"]).values(
            id=admin_b,
            tenant_id=tenant_b,
            name="m28-admin-b",
            status="ACTIVE",
            created_at=now,
            updated_at=now,
        )
    )
    service = create_llm_registry_service(b"I" * 32, {1: b"E" * 32}, 1)
    try:
        provider = service.create_provider(
            connection,
            tenant_a,
            LlmProviderCreate(name="provider", provider_type="OPENAI"),
        )
        target = service.create_target(
            connection,
            tenant_a,
            LlmTargetCreate(
                provider_id=provider.id,
                name="target",
                base_url="https://provider.example",
            ),
        )
        request = ProviderCredentialWrite(
            secret="provider-secret", secret_type="API_KEY"
        )
        first = service.rotate_credential(
            connection,
            tenant_id=tenant_a,
            admin_user_id=admin_id,
            target_id=target.id,
            request=request,
            raw_idempotency_key="m28-rotation-key",
        )
        replay = service.rotate_credential(
            connection,
            tenant_id=tenant_a,
            admin_user_id=admin_id,
            target_id=target.id,
            request=request,
            raw_idempotency_key="m28-rotation-key",
        )
        assert replay.replayed and replay.response == first.response
        credential_id = UUID(json.loads(first.response.body)["id"])
        table = Base.metadata.tables["llm_provider_credentials"]
        row = (
            connection.execute(select(table).where(table.c.id == credential_id))
            .mappings()
            .one()
        )
        assert b"provider-secret" not in row["secret_ciphertext"]
        assert b"provider-secret" not in first.response.body
        plaintext = SecretService({1: b"E" * 32}, current_key_version=1).decrypt(
            EncryptedSecret(int(row["key_version"]), row["secret_ciphertext"]),
            aad=service.credential_aad(tenant_a, target.id, credential_id, "API_KEY"),
        )
        assert plaintext == b"provider-secret"
        assert (
            len(
                connection.execute(
                    select(table).where(table.c.provider_target_id == target.id)
                ).all()
            )
            == 1
        )
        with pytest.raises(IdempotencyConflictError):
            service.rotate_credential(
                connection,
                tenant_id=tenant_a,
                admin_user_id=admin_id,
                target_id=target.id,
                request=ProviderCredentialWrite(secret="different"),
                raw_idempotency_key="m28-rotation-key",
            )
        with pytest.raises(LlmRegistryNotFoundError):
            service.rotate_credential(
                connection,
                tenant_id=tenant_b,
                admin_user_id=admin_b,
                target_id=target.id,
                request=request,
                raw_idempotency_key="m28-foreign-key",
            )
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()
