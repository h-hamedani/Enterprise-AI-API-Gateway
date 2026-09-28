import asyncio
import base64
import logging
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.control_plane.config_subscriber import InvalidationRegistry
from app.redis.circuit import CircuitContractError, CircuitIdentity
from app.redis.recovery import RecoveryCoordinator
from app.redis.telemetry import BoundedEvent, RecordingTelemetry, normalize_category


@pytest.mark.asyncio
async def test_recovery_emits_bounded_start_and_not_ready_without_ids() -> None:
    class Unavailable:
        async def check(self):
            return SimpleNamespace(available=False)

    recorder = RecordingTelemetry()
    coordinator = RecoveryCoordinator(
        Unavailable(),
        SimpleNamespace(ready=False),
        object(),
        InvalidationRegistry(),
        telemetry=recorder,
    )
    coordinator.mark_unreachable()
    assert await coordinator.attempt_recovery() == "redis_not_ready"
    assert [
        event.dimensions for event in recorder.events if event.family == "recovery"
    ] == [
        {"outcome": "recovery_started"},
        {"outcome": "redis_not_ready"},
    ]
    assert BoundedEvent("traffic", {"mode": "DEGRADED_REDIS"}) in recorder.events


@pytest.mark.asyncio
async def test_successful_recovery_emits_recovering_and_normal_modes() -> None:
    class Available:
        async def check(self):
            return SimpleNamespace(available=True)

    class Reconciler:
        async def reconcile_once(self):
            return "empty_membership"

    recorder = RecordingTelemetry()
    coordinator = RecoveryCoordinator(
        Available(),
        SimpleNamespace(ready=True),
        Reconciler(),
        InvalidationRegistry(),
        telemetry=recorder,
    )
    coordinator.mark_unreachable()
    assert await coordinator.attempt_recovery() == "recovery_succeeded"
    assert [
        event.dimensions["mode"]
        for event in recorder.events
        if event.family == "traffic"
    ] == ["DEGRADED_REDIS", "RECOVERING_REDIS", "NORMAL"]


@pytest.mark.asyncio
async def test_publication_normalizes_resource_category_and_omits_ids() -> None:
    from app.control_plane.config_publish import RedisConfigInvalidationPublisher
    from app.control_plane.mutation_coordinator import CommittedMutation, ResourceType

    class Redis:
        async def publish(self, _channel, _payload):
            return 1

    recorder = RecordingTelemetry()
    tenant, resource = uuid4(), uuid4()
    mutation = CommittedMutation(tenant, 17, ResourceType.ROUTE, resource)
    assert await RedisConfigInvalidationPublisher(Redis(), telemetry=recorder).publish(
        mutation, request_id=uuid4()
    )
    assert recorder.events == [
        BoundedEvent(
            "config_publication",
            {"operation": "publish", "outcome": "published", "resource_kind": "route"},
        )
    ]
    assert str(tenant) not in str(recorder.events)
    assert str(resource) not in str(recorder.events)
    assert "17" not in str(recorder.events)


@pytest.mark.asyncio
async def test_empty_reconciliation_emits_bounded_outcome() -> None:
    from app.control_plane.config_reconciler import ConfigReconciler

    recorder = RecordingTelemetry()

    async def unused_lookup(_tenants):
        raise AssertionError("Empty membership must not query PostgreSQL")

    reconciler = ConfigReconciler(
        InvalidationRegistry(), unused_lookup, telemetry=recorder
    )
    assert await reconciler.reconcile_once() == "empty_membership"
    assert recorder.events == [
        BoundedEvent(
            "config_reconciliation",
            {"operation": "reconcile", "outcome": "empty_membership"},
        )
    ]


@pytest.mark.asyncio
async def test_subscriber_emits_bounded_connected_state() -> None:
    from app.control_plane.config_subscriber import RedisInvalidationSubscriber

    subscribed = asyncio.Event()

    class PubSub:
        async def subscribe(self, *_channels):
            subscribed.set()

        async def get_message(self, **_kwargs):
            await asyncio.Event().wait()

        async def aclose(self):
            return None

    class Redis:
        def pubsub(self):
            return PubSub()

    recorder = RecordingTelemetry()
    subscriber = RedisInvalidationSubscriber(
        Redis(), InvalidationRegistry(), telemetry=recorder
    )
    await subscriber.start()
    await asyncio.wait_for(subscribed.wait(), 1)
    await subscriber.stop()
    assert (
        BoundedEvent(
            "config_subscriber", {"operation": "subscribe", "outcome": "connected"}
        )
        in recorder.events
    )


@pytest.mark.asyncio
async def test_subscriber_normalizes_unknown_incoming_resource_type() -> None:
    import json

    from app.control_plane.config_subscriber import RedisInvalidationSubscriber

    received = asyncio.Event()
    tenant, resource = uuid4(), uuid4()
    payload = json.dumps(
        {
            "tenant_id": str(tenant),
            "resource_type": "client-arbitrary-secret",
            "resource_id": str(resource),
            "version": 9,
        }
    )

    class PubSub:
        delivered = False

        async def subscribe(self, *_channels):
            return None

        async def get_message(self, **_kwargs):
            if not self.delivered:
                self.delivered = True
                return {"data": payload}
            await asyncio.Event().wait()

        async def aclose(self):
            return None

    class Redis:
        def pubsub(self):
            return PubSub()

    class ObservingTelemetry(RecordingTelemetry):
        def record(self, event):
            super().record(event)
            if event.dimensions.get("outcome") == "received":
                received.set()

    recorder = ObservingTelemetry()
    subscriber = RedisInvalidationSubscriber(
        Redis(), InvalidationRegistry(), telemetry=recorder
    )
    await subscriber.start()
    await asyncio.wait_for(received.wait(), 1)
    await subscriber.stop()
    assert (
        BoundedEvent(
            "config_subscriber",
            {"operation": "subscribe", "outcome": "received", "resource_kind": "other"},
        )
        in recorder.events
    )
    assert "client-arbitrary-secret" not in str(recorder.events)


@pytest.mark.asyncio
async def test_reconciliation_failure_log_omits_exception_and_tenant(
    caplog, monkeypatch
) -> None:
    from app.control_plane.config_reconciler import ConfigReconciler

    tenant = uuid4()
    secret = "redis://user:password@host/0"

    class FailingRegistry:
        def tenant_ids(self):
            return frozenset({tenant})

        async def reconcile_fenced(self, _tenant, _lookup):
            raise RuntimeError(secret)

    async def unused_lookup(_tenants):
        raise AssertionError("Lookup belongs to registry")

    logger = logging.getLogger("app.control_plane.config_reconciler")
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", True)
    with caplog.at_level(logging.WARNING, logger=logger.name):
        assert (
            await ConfigReconciler(FailingRegistry(), unused_lookup).reconcile_once()
            == "pass_error"
        )
    assert caplog.records
    assert secret not in str([record.__dict__ for record in caplog.records])
    assert str(tenant) not in str([record.__dict__ for record in caplog.records])


@pytest.mark.asyncio
async def test_pre_auth_telemetry_omits_ip_and_hmac_identity() -> None:
    from app.control_plane.protection import ControlPlaneProtection
    from app.core.config import Settings

    class Client:
        async def script_load(self, _script):
            return "sha"

        async def evalsha(self, *_args):
            return [1, 0]

    recorder = RecordingTelemetry()
    secret = base64.b64encode(b"k" * 32).decode()
    guard = ControlPlaneProtection(
        SimpleNamespace(client=Client()),
        Settings(_env_file=None, credential_hmac_secret=secret),
        telemetry=recorder,
    )
    request = SimpleNamespace(client=SimpleNamespace(host="203.0.113.7"), headers={})
    await guard.pre_auth(request)
    assert (
        BoundedEvent("pre_auth", {"operation": "evaluate", "outcome": "allowed"})
        in recorder.events
    )
    assert "203.0.113.7" not in str(recorder.events)
    assert "adminpre" not in str(recorder.events)


@pytest.mark.asyncio
async def test_pre_auth_operational_logs_omit_ip_and_hmac_identity(
    caplog, monkeypatch
) -> None:
    from app.control_plane.protection import ControlPlaneProtection, pre_auth_policy
    from app.core.config import Settings

    class Client:
        async def script_load(self, _script):
            return "sha"

        async def evalsha(self, *_args):
            return [1, 0]

    ip = "203.0.113.7"
    key = b"k" * 32
    logger = logging.getLogger("app.redis.telemetry")
    monkeypatch.setattr(logger, "disabled", False)
    monkeypatch.setattr(logger, "propagate", True)
    guard = ControlPlaneProtection(
        SimpleNamespace(client=Client()),
        Settings(_env_file=None, credential_hmac_secret=base64.b64encode(key).decode()),
    )
    with caplog.at_level(logging.INFO, logger=logger.name):
        await guard.pre_auth(
            SimpleNamespace(client=SimpleNamespace(host=ip), headers={})
        )
    serialized = str([record.__dict__ for record in caplog.records])
    assert caplog.records
    assert ip not in serialized
    assert pre_auth_policy(ip, key).key_override.digest not in serialized


def test_unknown_external_category_normalizes_to_other() -> None:
    arbitrary = f"service:{uuid4()}:redis://secret"
    assert normalize_category(arbitrary, {"service", "route"}) == "other"
    assert normalize_category("route", {"service", "route"}) == "route"


@pytest.mark.parametrize(
    "name",
    [
        "tenant_id",
        "resource_id",
        "request_id",
        "config_version",
        "lease_id",
        "probe_id",
        "generation_id",
        "redis_key",
        "raw_ip",
        "hmac_digest",
        "token",
        "url",
        "resource_type",
    ],
)
def test_event_rejects_identifier_and_unbounded_dimensions(name: str) -> None:
    with pytest.raises(ValueError):
        BoundedEvent("config_publication", {name: str(uuid4())})
    with pytest.raises(ValueError):
        BoundedEvent("config_publication", {"outcome": str(uuid4())})


def test_event_dimensions_cannot_be_mutated_after_validation() -> None:
    labels = {"operation": "ping", "outcome": "success"}
    event = BoundedEvent("redis", labels)
    labels["outcome"] = str(uuid4())
    assert event.dimensions == {"operation": "ping", "outcome": "success"}
    with pytest.raises(TypeError):
        event.dimensions["outcome"] = "failure"


def test_recording_telemetry_keeps_numeric_latency_out_of_labels() -> None:
    recorder = RecordingTelemetry()
    recorder.record(
        BoundedEvent(
            "redis", {"operation": "ping", "outcome": "success"}, latency_ms=3.25
        )
    )
    assert recorder.events[0].dimensions == {"operation": "ping", "outcome": "success"}
    assert recorder.events[0].latency_ms == 3.25


def test_circuit_identity_rejects_arbitrary_key_fragment() -> None:
    with pytest.raises(CircuitContractError):
        CircuitIdentity._create(uuid4(), "route:adm_secret", uuid4(), uuid4())
