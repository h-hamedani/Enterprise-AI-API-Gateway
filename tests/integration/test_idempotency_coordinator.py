from __future__ import annotations

from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, insert, select, update

from app.core.config import get_settings
from app.core.idempotency import (
    ClaimState,
    IdempotencyConflictError,
    IdempotencyCoordinator,
    IdempotencyExpiredError,
    IdempotencyInProgressError,
    IdempotencyReplayError,
    IdempotencyRepository,
    StoredHttpResponse,
)
from app.core.security.idempotency import IdempotencyDigester
from app.core.security.secrets import SecretService
from app.persistence.models import Base


@pytest.fixture
def idempotency_fixture():
    engine = create_engine(get_settings().postgres_migration_dsn)
    timestamp = datetime.now(UTC)
    tenant_a, tenant_b = uuid4(), uuid4()
    admin_a, admin_a2, admin_b = uuid4(), uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            insert(Base.metadata.tables["tenants"]),
            [
                {
                    "id": tenant_a,
                    "name": f"m23-tenant-{tenant_a}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": tenant_b,
                    "name": f"m23-tenant-{tenant_b}",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            ],
        )
        connection.execute(
            insert(Base.metadata.tables["admin_users"]),
            [
                {
                    "id": admin_a,
                    "tenant_id": tenant_a,
                    "name": "m23-admin-a",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": admin_a2,
                    "tenant_id": tenant_a,
                    "name": "m23-admin-a2",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
                {
                    "id": admin_b,
                    "tenant_id": tenant_b,
                    "name": "m23-admin-b",
                    "status": "ACTIVE",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            ],
        )
    yield {
        "engine": engine,
        "tenant_a": tenant_a,
        "tenant_b": tenant_b,
        "admin_a": admin_a,
        "admin_a2": admin_a2,
        "admin_b": admin_b,
    }
    with engine.begin() as connection:
        for table_name in ("idempotency_records", "applications", "admin_users"):
            connection.execute(
                Base.metadata.tables[table_name]
                .delete()
                .where(
                    Base.metadata.tables[table_name].c.tenant_id.in_(
                        [tenant_a, tenant_b]
                    )
                )
            )
        connection.execute(
            Base.metadata.tables["tenants"]
            .delete()
            .where(Base.metadata.tables["tenants"].c.id.in_([tenant_a, tenant_b]))
        )
    engine.dispose()


def _coordinator(*, encryption_key: bytes = b"e" * 32) -> IdempotencyCoordinator:
    return IdempotencyCoordinator(
        IdempotencyRepository(IdempotencyDigester(b"i" * 32)),
        SecretService({1: encryption_key}, current_key_version=1),
    )


def _fingerprint(body) -> str:
    return IdempotencyCoordinator.fingerprint(
        method="POST",
        endpoint_key="api-key.create",
        path_parameters={"application_id": UUID_SENTINEL},
        body=body,
    )


UUID_SENTINEL = uuid4()


def test_first_claim_completion_and_encrypted_exact_replay(idempotency_fixture):
    coordinator = _coordinator()
    raw_key = "customer-idempotency-key"
    raw_secret = "gw_show-once-value"
    fingerprint = _fingerprint({"name": "production", "labels": {"b": 2, "a": 1}})
    response = StoredHttpResponse(
        201,
        f'{{"key":"{raw_secret}","id":"one"}}'.encode(),
        (("Content-Type", "application/json"), ("Location", "/api-keys/one")),
    )

    with idempotency_fixture["engine"].begin() as connection:
        claim = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key=raw_key,
            request_fingerprint=fingerprint,
        )
        assert claim.state is ClaimState.CREATED
        assert claim.record_id is not None
        coordinator.complete(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            record_id=claim.record_id,
            response=response,
        )

    table = Base.metadata.tables["idempotency_records"]
    with idempotency_fixture["engine"].connect() as connection:
        row = connection.execute(select(table)).mappings().one()
        replay = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key=raw_key,
            request_fingerprint=fingerprint,
        )

    assert replay.state is ClaimState.REPLAY
    assert replay.response == response
    assert raw_key not in str(row)
    assert raw_secret.encode() not in row["response_body_ciphertext"]
    assert raw_secret not in str(row)
    assert row["response_metadata"] == {
        "format": "encrypted-http-response-v1",
        "key_version": 1,
    }
    assert row["expires_at"] == row["created_at"] + timedelta(hours=24)


def test_fingerprint_is_canonical_and_conflicts_are_fail_closed(idempotency_fixture):
    first = _fingerprint({"z": 1, "nested": {"b": 2, "a": 1}})
    equivalent = _fingerprint({"nested": {"a": 1, "b": 2}, "z": 1})
    different = _fingerprint({"z": 2, "nested": {"a": 1, "b": 2}})
    assert first == equivalent
    assert first != different
    coordinator = _coordinator()
    with idempotency_fixture["engine"].begin() as connection:
        coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="conflict-key-value",
            request_fingerprint=first,
        )
    with (
        idempotency_fixture["engine"].begin() as connection,
        pytest.raises(IdempotencyConflictError),
    ):
        coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="conflict-key-value",
            request_fingerprint=different,
        )


def test_concurrent_claim_returns_immediately_and_only_owner_mutates(
    idempotency_fixture,
):
    coordinator = _coordinator()
    fingerprint = _fingerprint({"name": "concurrent"})
    first_connection = idempotency_fixture["engine"].connect()
    first_transaction = first_connection.begin()
    try:
        claim = coordinator.claim(
            first_connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="concurrent-key-value",
            request_fingerprint=fingerprint,
        )
        assert claim.record_id is not None
        first_connection.execute(
            insert(Base.metadata.tables["applications"]).values(
                id=uuid4(),
                tenant_id=idempotency_fixture["tenant_a"],
                name="one-business-mutation",
                status="ACTIVE",
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        started = monotonic()
        with (
            idempotency_fixture["engine"].begin() as second_connection,
            pytest.raises(IdempotencyInProgressError),
        ):
            coordinator.claim(
                second_connection,
                tenant_id=idempotency_fixture["tenant_a"],
                admin_user_id=idempotency_fixture["admin_a"],
                endpoint_key="api-key.create",
                raw_key="concurrent-key-value",
                request_fingerprint=fingerprint,
            )
        assert monotonic() - started < 1
        first_transaction.commit()
    finally:
        if first_transaction.is_active:
            first_transaction.rollback()
        first_connection.close()

    with idempotency_fixture["engine"].connect() as connection:
        mutation_count = connection.scalar(
            select(func.count())
            .select_from(Base.metadata.tables["applications"])
            .where(
                Base.metadata.tables["applications"].c.tenant_id
                == idempotency_fixture["tenant_a"]
            )
        )
    assert mutation_count == 1


@pytest.mark.parametrize(
    ("tenant_key", "admin_key", "endpoint"),
    [
        ("tenant_b", "admin_b", "api-key.create"),
        ("tenant_a", "admin_a2", "api-key.create"),
        ("tenant_a", "admin_a", "llm-target.credential.rotate"),
    ],
)
def test_scope_isolated_by_tenant_admin_and_endpoint(
    idempotency_fixture, tenant_key, admin_key, endpoint
):
    coordinator = _coordinator()
    with idempotency_fixture["engine"].begin() as connection:
        first = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="shared-key-value",
            request_fingerprint="a" * 64,
        )
        isolated = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture[tenant_key],
            admin_user_id=idempotency_fixture[admin_key],
            endpoint_key=endpoint,
            raw_key="shared-key-value",
            request_fingerprint="a" * 64,
        )
    assert first.state is isolated.state is ClaimState.CREATED
    assert first.record_id != isolated.record_id


def test_failed_mutation_rolls_back_claim_and_allows_clean_retry(idempotency_fixture):
    coordinator = _coordinator()
    connection = idempotency_fixture["engine"].connect()
    transaction = connection.begin()
    try:
        first = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="rollback-key-value",
            request_fingerprint="r" * 64,
        )
        assert first.state is ClaimState.CREATED
        transaction.rollback()
    finally:
        if transaction.is_active:
            transaction.rollback()
        connection.close()

    with idempotency_fixture["engine"].begin() as retry_connection:
        retry = coordinator.claim(
            retry_connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="rollback-key-value",
            request_fingerprint="r" * 64,
        )
    assert retry.state is ClaimState.CREATED


def test_expired_record_and_corrupt_replay_fail_closed(idempotency_fixture):
    coordinator = _coordinator()
    table = Base.metadata.tables["idempotency_records"]
    with idempotency_fixture["engine"].begin() as connection:
        claim = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="expired-key-value",
            request_fingerprint="a" * 64,
        )
        assert claim.record_id is not None
        connection.execute(
            update(table)
            .where(table.c.id == claim.record_id)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    with (
        idempotency_fixture["engine"].begin() as connection,
        pytest.raises(IdempotencyExpiredError),
    ):
        coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="expired-key-value",
            request_fingerprint="a" * 64,
        )

    with idempotency_fixture["engine"].begin() as connection:
        claim = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="tampered-key-value",
            request_fingerprint="b" * 64,
        )
        assert claim.record_id is not None
        coordinator.complete(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            record_id=claim.record_id,
            response=StoredHttpResponse(201, b'{"key":"gw_secret"}'),
        )
        ciphertext = connection.scalar(
            select(table.c.response_body_ciphertext).where(
                table.c.id == claim.record_id
            )
        )
        connection.execute(
            update(table)
            .where(table.c.id == claim.record_id)
            .values(
                response_body_ciphertext=ciphertext[:-1] + bytes([ciphertext[-1] ^ 1])
            )
        )
    with (
        idempotency_fixture["engine"].begin() as connection,
        pytest.raises(IdempotencyReplayError),
    ):
        coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="tampered-key-value",
            request_fingerprint="b" * 64,
        )


def test_wrong_encryption_key_fails_closed_without_secret_leak(
    idempotency_fixture, caplog
):
    coordinator = _coordinator()
    with idempotency_fixture["engine"].begin() as connection:
        claim = coordinator.claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="wrong-key-version-value",
            request_fingerprint="c" * 64,
        )
        assert claim.record_id is not None
        coordinator.complete(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            record_id=claim.record_id,
            response=StoredHttpResponse(201, b'{"key":"gw_never-log-this"}'),
        )
    with (
        idempotency_fixture["engine"].begin() as connection,
        pytest.raises(IdempotencyReplayError),
    ):
        _coordinator(encryption_key=b"x" * 32).claim(
            connection,
            tenant_id=idempotency_fixture["tenant_a"],
            admin_user_id=idempotency_fixture["admin_a"],
            endpoint_key="api-key.create",
            raw_key="wrong-key-version-value",
            request_fingerprint="c" * 64,
        )
    assert "wrong-key-version-value" not in caplog.text
    assert "gw_never-log-this" not in caplog.text
