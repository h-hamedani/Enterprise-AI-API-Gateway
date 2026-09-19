from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import uuid_utils.compat as uuid_utils
from sqlalchemy import create_engine, insert, inspect
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.core.config import get_settings
from app.persistence.models import Base


@pytest.fixture
def db_connection():
    settings = get_settings()
    if settings.environment == "production":
        pytest.fail("Integration database tests must never run in production.")
    engine = create_engine(settings.postgres_migration_dsn)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()
    engine.dispose()


def timestamp():
    return datetime.now(UTC)


def add_tenant(connection, name):
    tenant_id, ts = uuid4(), timestamp()
    connection.execute(
        insert(Base.metadata.tables["tenants"]).values(
            id=tenant_id,
            name=name,
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    return tenant_id


def add_normal_graph(connection, tenant_id, suffix):
    ts = timestamp()
    slug_suffix = suffix.replace("_", "-")
    application_id, api_key_id, service_id, route_id = (uuid4() for _ in range(4))
    connection.execute(
        insert(Base.metadata.tables["applications"]).values(
            id=application_id,
            tenant_id=tenant_id,
            name=f"app-{suffix}",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["api_keys"]).values(
            id=api_key_id,
            tenant_id=tenant_id,
            application_id=application_id,
            name=f"key-{suffix}",
            key_prefix=f"gw_{suffix}"[:32],
            key_hash=f"hash-{suffix}",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["normal_api_services"]).values(
            id=service_id,
            tenant_id=tenant_id,
            slug=f"service-{slug_suffix}",
            display_name="Service",
            upstream_base_url="https://service.example.test",
            status="ACTIVE",
            request_body_limit_bytes=1024,
            connect_timeout_seconds=5,
            pool_timeout_seconds=5,
            write_timeout_seconds=30,
            read_idle_timeout_seconds=60,
            pre_response_timeout_seconds=120,
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["normal_api_routes"]).values(
            id=route_id,
            tenant_id=tenant_id,
            service_id=service_id,
            method="GET",
            path_pattern=f"/{slug_suffix}",
            upstream_path_template=f"/{slug_suffix}",
            priority=0,
            timeout_ms=None,
            header_policy=None,
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    return application_id, api_key_id, route_id


def add_llm_graph(connection, tenant_id, suffix):
    ts = timestamp()
    provider_id, target_id, model_id, alias_id = (uuid4() for _ in range(4))
    connection.execute(
        insert(Base.metadata.tables["llm_providers"]).values(
            id=provider_id,
            tenant_id=tenant_id,
            name=f"provider-{suffix}",
            provider_type="OPENAI",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["llm_provider_targets"]).values(
            id=target_id,
            tenant_id=tenant_id,
            provider_id=provider_id,
            name=f"target-{suffix}",
            base_url="https://llm.example.test",
            timeout_ms=30000,
            pre_output_idle_timeout_ms=20000,
            pre_output_budget_ms=30000,
            post_output_idle_timeout_ms=60000,
            status="ACTIVE",
            certification_status="CERTIFIED",
            allow_uncertified_runtime=False,
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["llm_models"]).values(
            id=model_id,
            tenant_id=tenant_id,
            provider_target_id=target_id,
            provider_model_name=f"model-{suffix}",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    connection.execute(
        insert(Base.metadata.tables["llm_aliases"]).values(
            id=alias_id,
            tenant_id=tenant_id,
            name=f"alias-{suffix}",
            status="ACTIVE",
            created_at=ts,
            updated_at=ts,
        )
    )
    return target_id, model_id, alias_id


def request_values(tenant_id, **overrides):
    ts = timestamp()
    values = {
        "id": uuid_utils.uuid7(),
        "tenant_id": tenant_id,
        "request_id": uuid_utils.uuid7(),
        "workload_type": "LLM",
        "route_id": None,
        "method": "POST",
        "started_at": ts,
        "completed_at": ts + timedelta(milliseconds=10),
        "latency_ms": 10,
        "degraded_mode": False,
    }
    values.update(overrides)
    return values


def add_request(connection, tenant_id, **overrides):
    values = request_values(tenant_id, **overrides)
    connection.execute(insert(Base.metadata.tables["requests"]).values(**values))
    return values["id"]


def llm_request_values(tenant_id, request_pk, **overrides):
    values = {
        "id": uuid_utils.uuid7(),
        "tenant_id": tenant_id,
        "request_id_fk": request_pk,
        "requested_model": "public-model",
        "stream": True,
        "tool_calling": False,
        "attempt_count": 0,
        "cost_complete": False,
        "output_committed": False,
    }
    values.update(overrides)
    return values


def test_valid_normal_and_llm_requests_accept_uuid7(db_connection):
    tenant_id = add_tenant(db_connection, "telemetry-valid")
    app_id, key_id, route_id = add_normal_graph(db_connection, tenant_id, "valid")
    normal = request_values(
        tenant_id,
        application_id=app_id,
        api_key_id=key_id,
        workload_type="NORMAL",
        route_id=route_id,
        method="GET",
    )
    assert normal["request_id"].version == 7
    db_connection.execute(insert(Base.metadata.tables["requests"]).values(**normal))
    add_request(db_connection, tenant_id)


@pytest.mark.parametrize("reference", ["application_id", "api_key_id", "route_id"])
def test_cross_tenant_normal_attribution_is_rejected(db_connection, reference):
    tenant_a = add_tenant(db_connection, f"telemetry-cross-a-{reference}")
    tenant_b = add_tenant(db_connection, f"telemetry-cross-b-{reference}")
    app_id, key_id, route_id = add_normal_graph(db_connection, tenant_a, reference)
    refs = {"application_id": app_id, "api_key_id": key_id, "route_id": route_id}
    values = request_values(
        tenant_b,
        workload_type="NORMAL",
        route_id=route_id,
        **({reference: refs[reference]} if reference != "route_id" else {}),
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(insert(Base.metadata.tables["requests"]).values(**values))


@pytest.mark.parametrize(
    "overrides",
    [
        {"workload_type": "NORMAL", "route_id": None},
        {"workload_type": "LLM", "route_id": uuid4()},
        {"latency_ms": -1},
        {"status_code": 99},
        {"status_code": 600},
        {"completed_at": datetime(2000, 1, 1, tzinfo=UTC)},
    ],
)
def test_invalid_request_metadata_is_rejected(db_connection, overrides):
    tenant_id = add_tenant(db_connection, f"request-invalid-{uuid4()}")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["requests"]).values(
                **request_values(tenant_id, **overrides)
            )
        )


def test_duplicate_canonical_request_id_is_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "request-id-unique")
    table = Base.metadata.tables["requests"]
    canonical = uuid_utils.uuid7()
    db_connection.execute(
        insert(table).values(**request_values(tenant_id, request_id=canonical))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(table).values(**request_values(tenant_id, request_id=canonical))
        )


def test_one_llm_extension_per_request(db_connection):
    tenant_id = add_tenant(db_connection, "llm-extension")
    request_pk = add_request(db_connection, tenant_id)
    table = Base.metadata.tables["llm_requests"]
    db_connection.execute(
        insert(table).values(**llm_request_values(tenant_id, request_pk))
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(table).values(**llm_request_values(tenant_id, request_pk))
        )


@pytest.mark.parametrize("reference", ["request", "alias", "model"])
def test_cross_tenant_llm_request_references_are_rejected(db_connection, reference):
    tenant_a = add_tenant(db_connection, f"llm-ref-a-{reference}")
    tenant_b = add_tenant(db_connection, f"llm-ref-b-{reference}")
    request_pk = add_request(db_connection, tenant_a)
    _, model_id, alias_id = add_llm_graph(db_connection, tenant_a, reference)
    overrides = {}
    if reference == "alias":
        request_pk = add_request(db_connection, tenant_b)
        overrides["resolved_alias_id"] = alias_id
    elif reference == "model":
        request_pk = add_request(db_connection, tenant_b)
        overrides["final_model_id"] = model_id
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_requests"]).values(
                **llm_request_values(tenant_b, request_pk, **overrides)
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempt_count": -1},
        {"input_tokens_total": -1},
        {"output_tokens_total": -1},
        {"known_cost_total": Decimal("-0.01")},
        {"ttft_ms": -1},
    ],
)
def test_negative_llm_aggregate_values_are_rejected(db_connection, overrides):
    tenant_id = add_tenant(db_connection, f"llm-negative-{uuid4()}")
    request_pk = add_request(db_connection, tenant_id)
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_requests"]).values(
                **llm_request_values(tenant_id, request_pk, **overrides)
            )
        )


def add_llm_request(connection, tenant_id):
    request_pk = add_request(connection, tenant_id)
    values = llm_request_values(tenant_id, request_pk)
    connection.execute(insert(Base.metadata.tables["llm_requests"]).values(**values))
    return values["id"]


def attempt_values(tenant_id, llm_request_id, target_id, model_id, **overrides):
    ts = timestamp()
    values = {
        "id": uuid_utils.uuid7(),
        "tenant_id": tenant_id,
        "llm_request_id": llm_request_id,
        "attempt_no": 1,
        "provider_target_id": target_id,
        "model_id": model_id,
        "started_at": ts,
        "completed_at": ts + timedelta(milliseconds=5),
        "status": "SUCCESS",
        "first_output_committed": True,
        "provider_midstream_failure": False,
    }
    values.update(overrides)
    return values


def test_valid_attempt_and_duplicate_attempt_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "attempt-valid")
    llm_request_id = add_llm_request(db_connection, tenant_id)
    target_id, model_id, _ = add_llm_graph(db_connection, tenant_id, "attempt")
    table = Base.metadata.tables["llm_attempts"]
    db_connection.execute(
        insert(table).values(
            **attempt_values(tenant_id, llm_request_id, target_id, model_id)
        )
    )
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(table).values(
                **attempt_values(tenant_id, llm_request_id, target_id, model_id)
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempt_no": 0},
        {"input_tokens": -1},
        {"output_tokens": -1},
        {"known_cost": Decimal("-0.01")},
        {"provider_ttft_ms": -1},
        {"provider_midstream_failure": True, "status": "SUCCESS"},
        {"completed_at": datetime(2000, 1, 1, tzinfo=UTC)},
    ],
)
def test_invalid_attempt_values_are_rejected(db_connection, overrides):
    tenant_id = add_tenant(db_connection, f"attempt-invalid-{uuid4()}")
    llm_request_id = add_llm_request(db_connection, tenant_id)
    target_id, model_id, _ = add_llm_graph(db_connection, tenant_id, str(uuid4()))
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_attempts"]).values(
                **attempt_values(
                    tenant_id, llm_request_id, target_id, model_id, **overrides
                )
            )
        )


def test_cross_tenant_attempt_references_are_rejected(db_connection):
    tenant_a = add_tenant(db_connection, "attempt-cross-a")
    tenant_b = add_tenant(db_connection, "attempt-cross-b")
    llm_request_id = add_llm_request(db_connection, tenant_b)
    target_id, model_id, _ = add_llm_graph(db_connection, tenant_a, "cross")
    with pytest.raises(IntegrityError):
        db_connection.execute(
            insert(Base.metadata.tables["llm_attempts"]).values(
                **attempt_values(tenant_b, llm_request_id, target_id, model_id)
            )
        )


def test_invalid_telemetry_enums_are_rejected(db_connection):
    tenant_id = add_tenant(db_connection, "telemetry-enum-invalid")
    with pytest.raises(DBAPIError):
        db_connection.execute(
            insert(Base.metadata.tables["requests"]).values(
                **request_values(tenant_id, workload_type="UNKNOWN")
            )
        )


def test_telemetry_schema_contains_no_unsafe_payload_columns(db_connection):
    unsafe = {
        "request_body",
        "response_body",
        "prompt",
        "headers",
        "cookies",
        "authorization",
        "api_key",
        "credential",
        "provider_payload",
        "exception_trace",
    }
    inspector = inspect(db_connection)
    for table_name in ("requests", "llm_requests", "llm_attempts"):
        names = {column["name"] for column in inspector.get_columns(table_name)}
        assert names.isdisjoint(unsafe)
