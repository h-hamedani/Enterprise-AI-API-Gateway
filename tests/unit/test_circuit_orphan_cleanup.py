from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.redis.circuit import CircuitIdentity, circuit_key
from app.redis.circuit_cleanup import (
    CircuitAuthority,
    CircuitOrphanSweeper,
    circuit_failure_key,
    parse_circuit_key,
)
from app.redis.telemetry import RecordingTelemetry

TENANT = UUID("11111111-1111-4111-8111-111111111111")
TARGET = UUID("22222222-2222-4222-8222-222222222222")
DIMENSION = UUID("33333333-3333-4333-8333-333333333333")
PRIMARY = (
    "gw:v1:cb:{11111111-1111-4111-8111-111111111111}:route:"
    "22222222-2222-4222-8222-222222222222:"
    "33333333-3333-4333-8333-333333333333"
)


class FakeRedis:
    def __init__(self, keys: set[str]) -> None:
        self.keys = keys
        self.deleted: list[tuple[str, ...]] = []
        self.scan_calls: list[tuple[int, str, int]] = []

    async def scan(self, cursor: int, match: str, count: int):
        self.scan_calls.append((cursor, match, count))
        return 0, sorted(self.keys)

    async def delete(self, *keys: str) -> int:
        self.deleted.append(keys)
        count = sum(key in self.keys for key in keys)
        self.keys.difference_update(keys)
        return count


class FakeAuthority:
    def __init__(self, *results: CircuitAuthority) -> None:
        self.results = iter(results)
        self.calls = 0

    async def lookup(self, identity: CircuitIdentity) -> CircuitAuthority:
        self.calls += 1
        return next(self.results)


def test_canonical_parser_and_keys():
    identity = parse_circuit_key(PRIMARY)
    assert identity == CircuitIdentity._create(TENANT, "route", TARGET, DIMENSION)
    assert parse_circuit_key(PRIMARY + ":failures") == identity
    assert circuit_failure_key(identity) == PRIMARY + ":failures"


@pytest.mark.parametrize(
    "key",
    [
        "gw:v1:rate:{11111111-1111-4111-8111-111111111111}:route:x:y",
        PRIMARY.replace(":route:", ":unknown:"),
        PRIMARY.replace(str(TARGET), "not-a-uuid"),
        PRIMARY + ":failures:extra",
        PRIMARY.replace("{", "", 1),
    ],
)
def test_parser_rejects_noncanonical_keys(key):
    assert parse_circuit_key(key) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "results,expected_deletes",
    [
        ((CircuitAuthority.ACTIVE,), 0),
        ((CircuitAuthority.AUTHORITY_UNAVAILABLE,), 0),
        ((CircuitAuthority.RETIRED, CircuitAuthority.ACTIVE), 0),
        ((CircuitAuthority.RETIRED, CircuitAuthority.AUTHORITY_UNAVAILABLE), 0),
        ((CircuitAuthority.RETIRED, CircuitAuthority.RETIRED), 1),
    ],
)
async def test_authority_fences_deletion(results, expected_deletes):
    redis = FakeRedis({PRIMARY, PRIMARY + ":failures", "gw:v1:other"})
    authority = FakeAuthority(*results)
    telemetry = RecordingTelemetry()
    sweep = CircuitOrphanSweeper(redis, authority.lookup, telemetry=telemetry)
    await sweep.reconcile_once()
    assert len(redis.deleted) == expected_deletes
    assert "gw:v1:other" in redis.keys
    assert authority.calls == len(results)
    assert all(
        not any(
            str(value) in str(event.dimensions) for value in (TENANT, TARGET, DIMENSION)
        )
        for event in telemetry.events
    )


@pytest.mark.asyncio
async def test_missing_pair_is_idempotent_and_scan_is_bounded():
    redis = FakeRedis({PRIMARY})
    authority = FakeAuthority(CircuitAuthority.RETIRED, CircuitAuthority.RETIRED)
    sweep = CircuitOrphanSweeper(
        redis, authority.lookup, scan_count=4, max_candidates=1
    )
    await sweep.reconcile_once()
    await sweep.reconcile_once()
    assert redis.deleted == [(PRIMARY, PRIMARY + ":failures")]
    assert all(call == (0, "gw:v1:cb:*", 1) for call in redis.scan_calls)


@pytest.mark.asyncio
async def test_duplicate_cleanup_is_safe():
    redis = FakeRedis({PRIMARY, PRIMARY + ":failures"})
    first = CircuitOrphanSweeper(
        redis, FakeAuthority(CircuitAuthority.RETIRED, CircuitAuthority.RETIRED).lookup
    )
    second = CircuitOrphanSweeper(
        redis, FakeAuthority(CircuitAuthority.RETIRED, CircuitAuthority.RETIRED).lookup
    )
    await asyncio.gather(first.reconcile_once(), second.reconcile_once())
    assert PRIMARY not in redis.keys
    assert PRIMARY + ":failures" not in redis.keys


@pytest.mark.asyncio
async def test_managed_worker_stops_without_orphan_task():
    redis = FakeRedis(set())
    entered_sleep = asyncio.Event()

    async def blocked_sleep(_seconds: float):
        entered_sleep.set()
        await asyncio.Event().wait()

    sweep = CircuitOrphanSweeper(redis, FakeAuthority().lookup, sleep=blocked_sleep)
    await sweep.start()
    await entered_sleep.wait()
    assert sweep.running
    await sweep.stop()
    assert not sweep.running


@pytest.mark.asyncio
async def test_empty_scan_pages_are_bounded():
    class EmptyPages(FakeRedis):
        async def scan(self, cursor: int, match: str, count: int):
            self.scan_calls.append((cursor, match, count))
            return cursor + 1, []

    redis = EmptyPages(set())
    sweep = CircuitOrphanSweeper(
        redis, FakeAuthority().lookup, max_candidates=4, max_scan_calls=2
    )
    assert await sweep.reconcile_once() == 0
    assert len(redis.scan_calls) == 2


@pytest.mark.asyncio
async def test_scan_classifies_dependency_and_protocol_failures_without_raw_text():
    class FailingRedis(FakeRedis):
        def __init__(self, error: Exception):
            super().__init__(set())
            self.error = error

        async def scan(self, cursor: int, match: str, count: int):
            raise self.error

    for error, expected in (
        (RedisConnectionError("private redis detail"), "redis_unavailable"),
        (ValueError("private protocol detail"), "cleanup_error"),
    ):
        telemetry = RecordingTelemetry()
        sweep = CircuitOrphanSweeper(
            FailingRedis(error), FakeAuthority().lookup, telemetry=telemetry
        )
        assert await sweep.reconcile_once() == 0
        assert [event.dimensions["outcome"] for event in telemetry.events] == [expected]
        assert "private" not in str(telemetry.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("results", [(None,), (CircuitAuthority.RETIRED, None)])
async def test_only_explicit_retired_authorizes_deletion(results):
    redis = FakeRedis({PRIMARY, PRIMARY + ":failures"})
    sweep = CircuitOrphanSweeper(redis, FakeAuthority(*results).lookup)
    await sweep.reconcile_once()
    assert redis.deleted == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_call", [1, 2])
async def test_authority_callback_exception_retains_keys(fail_call):
    redis = FakeRedis({PRIMARY, PRIMARY + ":failures"})
    telemetry = RecordingTelemetry()
    calls = 0

    async def authority(_identity: CircuitIdentity) -> CircuitAuthority:
        nonlocal calls
        calls += 1
        if calls == fail_call:
            raise RuntimeError("private database detail")
        return CircuitAuthority.RETIRED

    sweep = CircuitOrphanSweeper(redis, authority, telemetry=telemetry)
    assert await sweep.reconcile_once() == 1
    assert redis.deleted == []
    assert telemetry.events[-1].dimensions["outcome"] == "authority_unavailable"
    assert "private" not in str(telemetry.events)


@pytest.mark.asyncio
async def test_oversized_malformed_scan_page_preserves_raw_entries_for_later_passes():
    class OversizedPage(FakeRedis):
        def __init__(self):
            super().__init__(set())
            self.page = [f"gw:v1:cb:malformed-{index}" for index in range(5)]

        async def scan(self, cursor: int, match: str, count: int):
            self.scan_calls.append((cursor, match, count))
            if len(self.scan_calls) > 1:
                return 0, []
            return 0, self.page.copy()

    redis = OversizedPage()
    authority = FakeAuthority(CircuitAuthority.ACTIVE)
    sweep = CircuitOrphanSweeper(
        redis,
        authority.lookup,
        scan_count=1,
        max_candidates=1,
        max_entries_examined=2,
    )
    assert await sweep.reconcile_once() == 0
    assert len(sweep._pending) == 3
    assert await sweep.reconcile_once() == 0
    assert len(sweep._pending) == 1
    assert await sweep.reconcile_once() == 0
    assert authority.calls == 0
    assert len(sweep._pending) == 0
    assert len(redis.scan_calls) == 2
    assert redis.scan_calls[0][2] == 1  # COUNT was only a hint.


@pytest.mark.asyncio
async def test_oversized_duplicate_page_obeys_raw_and_candidate_budgets():
    class OversizedPage(FakeRedis):
        async def scan(self, cursor: int, match: str, count: int):
            self.scan_calls.append((cursor, match, count))
            return 0, [
                PRIMARY,
                PRIMARY + ":failures",
                PRIMARY,
                PRIMARY + ":failures",
                "gw:v1:cb:malformed",
            ]

    redis = OversizedPage(set())
    calls = 0

    async def active(_identity: CircuitIdentity) -> CircuitAuthority:
        nonlocal calls
        calls += 1
        return CircuitAuthority.ACTIVE

    sweep = CircuitOrphanSweeper(
        redis, active, max_candidates=3, max_entries_examined=2
    )
    assert await sweep.reconcile_once() == 1
    assert len(sweep._pending) == 3
    assert calls == 1
    assert await sweep.reconcile_once() == 1
    assert len(sweep._pending) == 1
    assert calls == 2


@pytest.mark.asyncio
async def test_oversized_valid_page_still_obeys_candidate_limit():
    second = CircuitIdentity._create(TENANT, "route", UUID(int=4), DIMENSION)

    class OversizedPage(FakeRedis):
        async def scan(self, cursor: int, match: str, count: int):
            self.scan_calls.append((cursor, match, count))
            return 0, [PRIMARY, circuit_key(second)]

    redis = OversizedPage(set())
    calls = 0

    async def active(_identity: CircuitIdentity) -> CircuitAuthority:
        nonlocal calls
        calls += 1
        return CircuitAuthority.ACTIVE

    sweep = CircuitOrphanSweeper(
        redis, active, max_candidates=1, max_entries_examined=3
    )
    assert await sweep.reconcile_once() == 1
    assert calls == 1
    assert len(sweep._pending) == 1
    assert await sweep.reconcile_once() == 1
    assert calls == 2
