from __future__ import annotations

import json
import logging
from uuid import uuid4

import pytest

from app.control_plane.config_publish import (
    CANONICAL_INVALIDATION_CHANNEL,
    CONFIG_INVALIDATION_CHANNEL,
    RedisConfigInvalidationPublisher,
)
from app.control_plane.mutation_coordinator import CommittedMutation, ResourceType


class RecordingRedis:
    def __init__(self, error: Exception | None = None, failing_channels=None) -> None:
        self.error = error
        self.failing_channels = set(failing_channels or ())
        self.calls = []

    async def publish(self, channel, payload):
        if self.error is not None or channel in self.failing_channels:
            raise self.error or RuntimeError("injected redis failure")
        self.calls.append((channel, payload))
        return 1


@pytest.mark.asyncio
async def test_publisher_uses_frozen_channel_and_safe_payload():
    redis = RecordingRedis()
    mutation = CommittedMutation(
        tenant_id=uuid4(),
        version=42,
        resource_type=ResourceType.LLM_TARGET,
        resource_id=uuid4(),
    )

    published = await RedisConfigInvalidationPublisher(redis).publish(
        mutation, request_id=uuid4()
    )

    assert published is True
    assert len(redis.calls) == 2
    assert {channel for channel, _ in redis.calls} == {
        CANONICAL_INVALIDATION_CHANNEL,
        CONFIG_INVALIDATION_CHANNEL,
    }
    _channel, encoded = redis.calls[0]
    assert json.loads(encoded) == {
        "tenant_id": str(mutation.tenant_id),
        "resource_type": "LLM_TARGET",
        "resource_id": str(mutation.resource_id),
        "version": 42,
    }
    assert all(
        forbidden not in encoded
        for forbidden in (
            "adm_",
            "gw_",
            "secret",
            "credential",
            "authorization",
            "idempotency",
            "ciphertext",
            "hash",
        )
    )


@pytest.mark.asyncio
async def test_publish_failure_is_best_effort_and_secret_safe(caplog, monkeypatch):
    publisher_logger = logging.getLogger("app.control_plane.config_publish")
    monkeypatch.setattr(publisher_logger, "disabled", False)
    monkeypatch.setattr(publisher_logger, "propagate", True)
    caplog.set_level(logging.ERROR, logger="app.control_plane.config_publish")
    redis = RecordingRedis(RuntimeError("provider-secret-must-not-be-logged"))
    mutation = CommittedMutation(
        tenant_id=uuid4(),
        version=7,
        resource_type=ResourceType.SERVICE,
        resource_id=uuid4(),
    )

    published = await RedisConfigInvalidationPublisher(redis).publish(
        mutation, request_id=uuid4()
    )

    assert published is False
    assert "provider-secret-must-not-be-logged" not in caplog.text
    assert "Config invalidation publication failed" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failing", "expected"),
    [
        ({CONFIG_INVALIDATION_CHANNEL}, True),
        ({CANONICAL_INVALIDATION_CHANNEL}, True),
        ({CONFIG_INVALIDATION_CHANNEL, CANONICAL_INVALIDATION_CHANNEL}, False),
    ],
)
async def test_dual_publication_failure_matrix(failing, expected):
    redis = RecordingRedis(failing_channels=failing)
    mutation = CommittedMutation(uuid4(), 9, ResourceType.ROUTE, uuid4())
    assert (
        await RedisConfigInvalidationPublisher(redis).publish(
            mutation, request_id=uuid4()
        )
        is expected
    )
