import asyncio

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.normal_api.http_client import create_normal_api_upstream_client


def test_pool_setting_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NORMAL_API_UPSTREAM_MAX_CONNECTIONS", raising=False)
    monkeypatch.delenv("NORMAL_API_UPSTREAM_MAX_KEEPALIVE_CONNECTIONS", raising=False)
    settings = Settings(_env_file=None)
    assert (
        settings.normal_api_upstream_max_connections,
        settings.normal_api_upstream_max_keepalive_connections,
    ) == (100, 20)


@pytest.mark.parametrize("limit", [1, 512])
def test_pool_setting_boundaries(monkeypatch: pytest.MonkeyPatch, limit: int) -> None:
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_CONNECTIONS", str(limit))
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_KEEPALIVE_CONNECTIONS", str(limit))
    settings = Settings(_env_file=None)
    assert settings.normal_api_upstream_max_connections == limit
    assert settings.normal_api_upstream_max_keepalive_connections == limit


def test_pool_setting_explicit_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_CONNECTIONS", "64")
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_KEEPALIVE_CONNECTIONS", "12")
    settings = Settings(_env_file=None)
    assert (
        settings.normal_api_upstream_max_connections,
        settings.normal_api_upstream_max_keepalive_connections,
    ) == (64, 12)


@pytest.mark.parametrize("value", ["0", "513", "1.5", "bad", ""])
def test_invalid_pool_setting_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_CONNECTIONS", value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["1", "20", "100", "512", "01"])
def test_integer_environment_strings_load(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_CONNECTIONS", value)
    monkeypatch.setenv("NORMAL_API_UPSTREAM_MAX_KEEPALIVE_CONNECTIONS", "1")
    assert Settings(_env_file=None).normal_api_upstream_max_connections == int(value)


def test_null_boolean_and_cross_field_rejected() -> None:
    for value in (None, True, 1.5):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, normal_api_upstream_max_connections=value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, normal_api_upstream_max_connections=1)


@pytest.mark.asyncio
async def test_owned_client_limits_and_lifecycle() -> None:
    settings = Settings(
        _env_file=None,
        normal_api_upstream_max_connections=2,
        normal_api_upstream_max_keepalive_connections=1,
    )
    client = create_normal_api_upstream_client(settings)
    try:
        assert not client.is_closed
        assert client._transport._pool._max_connections == 2
        assert client._transport._pool._max_keepalive_connections == 1
    finally:
        await client.aclose()
    assert client.is_closed


@pytest.mark.asyncio
async def test_local_persistent_connection_reuse() -> None:
    connections = 0

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal connections
        connections += 1
        try:
            while True:
                request = await reader.readuntil(b"\r\n\r\n")
                if not request:
                    break
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with create_normal_api_upstream_client(
            Settings(_env_file=None)
        ) as client:
            for _ in range(2):
                response = await client.get(f"http://127.0.0.1:{port}/")
                assert response.text == "OK"
        assert connections == 1
    finally:
        server.close()
        await server.wait_closed()
