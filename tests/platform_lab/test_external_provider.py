"""The endpoint adapter must not grant daemon access or weaken scenario checks."""

import json

import pytest

from tests.integration import test_m4_platform_shutdown as harness


def test_external_provider_never_calls_docker(tmp_path, monkeypatch):
    services = {"pg": 5432, "redis": 6379, "pg_host": "pg", "redis_host": "redis"}
    path = tmp_path / "services.json"
    path.write_text(json.dumps(services), encoding="utf-8")
    monkeypatch.setenv("M4_PLATFORM_EXTERNAL_SERVICES_FILE", str(path))

    def forbidden(*args):
        pytest.fail("Linux endpoint provider invoked Docker")

    monkeypatch.setattr(harness, "docker", forbidden)
    provider = harness.infrastructure.__wrapped__()
    assert next(provider) == services
    with pytest.raises(StopIteration):
        next(provider)


@pytest.mark.parametrize("invalid", ["0", "-1", "true", '"5432"'])
def test_external_provider_rejects_invalid_port(tmp_path, monkeypatch, invalid):
    path = tmp_path / "services.json"
    path.write_text(
        '{"pg":' + invalid + ',"redis":6379,"pg_host":"pg","redis_host":"redis"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("M4_PLATFORM_EXTERNAL_SERVICES_FILE", str(path))
    with pytest.raises(AssertionError):
        next(harness.infrastructure.__wrapped__())
