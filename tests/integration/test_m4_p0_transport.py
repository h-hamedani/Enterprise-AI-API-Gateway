"""Actual TCP/TLS proof with synthetic DNS; no ASGI streaming claims."""

import asyncio
import ipaddress
import socket
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpcore
import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tests.integration.m4_p0_transport import PeerMismatch, ValidatedTransport


@pytest.fixture(autouse=True)
def forbid_unvalidated_dns(monkeypatch):
    original = socket.getaddrinfo

    def numeric_only(host, *args, **kwargs):
        ipaddress.ip_address(host.decode() if isinstance(host, bytes) else host)
        return original(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", numeric_only)


@asynccontextmanager
async def listener(host="127.0.0.1", port=0, context=None, *, hook=None, state=None):
    requests = []
    connections = []
    closed = asyncio.Queue()
    tasks = set()
    if state is not None:
        state["tasks"] = tasks
        state["errors"] = []
        state["received"] = []

    async def serve(reader, writer):
        tasks.add(asyncio.current_task())
        connections.append(writer.get_extra_info("peername"))
        try:
            if hook is not None:
                await hook()
            while True:
                raw = await reader.readuntil(b"\r\n\r\n")
                if state is not None:
                    state["received"].append(raw)
                requests.append(raw)
                writer.write(
                    b"HTTP/1.1 302 Found\r\nContent-Length: 2\r\n"
                    b"Location: http://must-not-follow.invalid/\r\n\r\nOK"
                )
                await writer.drain()
        except asyncio.IncompleteReadError as exc:
            if state is not None and exc.partial:
                state["received"].append(exc.partial)
        except ConnectionError:
            pass
        except Exception as exc:
            if state is None:
                raise
            state["errors"].append(type(exc))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            finally:
                closed.put_nowait(True)
                tasks.discard(asyncio.current_task())

    server = await asyncio.start_server(serve, host, port, ssl=context)
    try:
        yield server.sockets[0].getsockname()[1], requests, connections, closed
    finally:
        server.close()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
        await asyncio.wait_for(server.wait_closed(), 3)


@pytest.fixture
def tls(tmp_path):
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "P0 synthetic CA")])
    now = datetime.now(UTC)

    def builder(subject, public_key):
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(name)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
        )

    ca = (
        builder(name, ca_key.public_key())
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    cert = (
        builder(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "upstream.test")]),
            key.public_key(),
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("upstream.test")]), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert_path, key_path)
    sni = []
    server.set_servername_callback(lambda socket, name, context: sni.append(name))
    client = httpcore.default_ssl_context()
    client.load_verify_locations(
        cadata=ca.public_bytes(serialization.Encoding.PEM).decode()
    )
    return server, client, sni


@pytest.mark.asyncio
@pytest.mark.parametrize("secure", [False, True])
async def test_real_peer_authority_wire_pool_and_close(secure, tls, monkeypatch):
    server_tls, client_tls, sni = tls
    calls = []

    def resolve(host):
        calls.append(host)
        return ["127.0.0.1"]

    async with listener() as (proxy_port, proxy_requests, proxy_connections, _):
        for name in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            monkeypatch.setenv(name, f"http://127.0.0.1:{proxy_port}")
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        monkeypatch.setenv("SSL_CERT_FILE", "P0-nonexistent-environment-CA")
        async with listener(context=server_tls if secure else None) as (
            port,
            requests,
            connections,
            closed,
        ):
            transport = ValidatedTransport(resolve, client_tls)
            scheme = "https" if secure else "http"
            raw_target = (
                b"/base/caf%C3%A9/a%20b/percent%25/end"
                b"?x=1&x=2&plus=a+b&space=a%20b&empty=&bare"
                b"&v=%2f&v=%2F&double=%252F&utf8=%E2%82%AC"
            )
            url = httpx.URL(f"{scheme}://upstream.test:{port}").copy_with(
                raw_path=raw_target
            )
            assert url.raw_path == raw_target
            async with httpx.AsyncClient(
                transport=transport, trust_env=False, follow_redirects=False
            ) as client:
                for _ in range(2):
                    response = await client.get(url)
                    assert response.status_code == 302 and response.content == b"OK"
                assert len(connections) == 1
                assert len(transport.dials) == 1
                assert transport.dials[0] == ("upstream.test", "127.0.0.1", port)
            await asyncio.wait_for(closed.get(), 3)
            assert len(requests) == 2
            assert requests[0].split(b"\r\n")[0] == b"GET " + raw_target + b" HTTP/1.1"
            assert f"Host: upstream.test:{port}".encode() in requests[0]
            assert calls == ["upstream.test", "upstream.test"]
            assert not proxy_requests and not proxy_connections
            if secure:
                assert sni == ["upstream.test"]


@pytest.mark.asyncio
async def test_complete_answer_set_denial_prevents_socket():
    async with listener() as (port, requests, connections, _):
        transport = ValidatedTransport(lambda host: ["127.0.0.1", "127.0.0.2"])
        transport.denied.add(("127.0.0.2", port))
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with pytest.raises(ValueError, match="destination denied"):
                await client.get(f"http://upstream.test:{port}")
        assert not requests and not connections and not transport.dials


@pytest.mark.asyncio
async def test_pool_cannot_reuse_peer_outside_new_approved_set():
    answers = ["127.0.0.1"]
    async with (
        listener() as (port, first_requests, _, first_closed),
        listener("127.0.0.2", port) as (_, second_requests, _, second_closed),
    ):
        transport = ValidatedTransport(lambda host: answers)
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            await client.get(f"http://upstream.test:{port}/first")
            answers[:] = ["127.0.0.2"]
            await client.get(f"http://upstream.test:{port}/second")
            await asyncio.wait_for(first_closed.get(), 3)
        await asyncio.wait_for(second_closed.get(), 3)
        assert len(first_requests) == len(second_requests) == 1
        assert [dial[1] for dial in transport.dials] == ["127.0.0.1", "127.0.0.2"]


@pytest.mark.asyncio
async def test_tls_hostname_verification_not_replaced_by_ip(tls):
    server_tls, client_tls, _ = tls
    async with listener(context=server_tls) as (port, requests, _, _):
        transport = ValidatedTransport(lambda host: ["127.0.0.1"], client_tls)
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with pytest.raises(
                httpcore.ConnectError, match="CERTIFICATE_VERIFY_FAILED"
            ):
                await client.get(f"https://wrong.test:{port}/")
        assert not requests


@pytest.mark.asyncio
async def test_untrusted_tls_and_separate_logical_origins(tls):
    server_tls, _, _ = tls
    async with listener(context=server_tls) as (port, requests, _, _):
        transport = ValidatedTransport(
            lambda host: ["127.0.0.1"], httpcore.default_ssl_context()
        )
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with pytest.raises(
                httpcore.ConnectError, match="CERTIFICATE_VERIFY_FAILED"
            ):
                await client.get(f"https://upstream.test:{port}/")
        assert not requests
    async with listener() as (port, requests, connections, closed):
        transport = ValidatedTransport(lambda host: ["127.0.0.1"])
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            await client.get(f"http://first.test:{port}/")
            await client.get(f"http://second.test:{port}/")
            assert len(connections) == 2
        await asyncio.wait_for(closed.get(), 3)
        await asyncio.wait_for(closed.get(), 3)
        assert b"Host: first.test:" in requests[0]
        assert b"Host: second.test:" in requests[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_raises", [False, True])
async def test_real_connected_peer_mismatch_closes_before_http(
    monkeypatch, cleanup_raises
):
    actual_peers = []
    original_connect = httpcore.AnyIOBackend.connect_tcp

    class CloseFailureStream:
        def __init__(self, stream):
            self.stream = stream

        def get_extra_info(self, name):
            return self.stream.get_extra_info(name)

        async def aclose(self):
            await self.stream.aclose()
            raise RuntimeError("synthetic cleanup failure")

    async def connect(backend, *args, **kwargs):
        stream = await original_connect(backend, *args, **kwargs)
        actual_peers.append(stream.get_extra_info("server_addr"))
        return CloseFailureStream(stream) if cleanup_raises else stream

    state = {}
    async with listener(state=state) as (port, requests, connections, closed):
        transport = ValidatedTransport(
            lambda host: ["127.0.0.2"],
            dial_address=lambda approved: "127.0.0.1",
        )
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with monkeypatch.context() as patch:
                patch.setattr(httpcore.AnyIOBackend, "connect_tcp", connect)
                with pytest.raises(PeerMismatch, match="^connected peer rejected$"):
                    await client.get(f"http://upstream.test:{port}/must-not-send")
            await asyncio.wait_for(closed.get(), 3)
            assert actual_peers == [("127.0.0.1", port)]
            assert not requests and len(connections) == 1
            assert not state["received"]
            assert not transport.pool.connections
            assert not transport.dials
            # A later valid request must obtain a fresh socket, not reuse rejection.
            transport.resolver = lambda host: ["127.0.0.1"]
            transport.dial_address = lambda address: address
            response = await client.get(f"http://upstream.test:{port}/valid")
            assert response.status_code == 302
            assert len(connections) == 2 and len(requests) == 1
            assert b"/valid " in requests[0]
        await asyncio.wait_for(closed.get(), 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_fails", [False, True])
async def test_listener_failure_or_teardown_cancellation_cleans_resources(
    handler_fails,
):
    entered = asyncio.Event()
    blocked = asyncio.Event()
    state = {}

    async def hook():
        entered.set()
        if handler_fails:
            raise RuntimeError("synthetic handler failure")
        await blocked.wait()

    async with listener(hook=hook, state=state) as (port, requests, _, closed):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            await asyncio.wait_for(entered.wait(), 3)
            if handler_fails:
                await asyncio.wait_for(closed.get(), 3)
        finally:
            writer.close()
            await writer.wait_closed()
    assert await asyncio.wait_for(reader.read(), 3) == b""
    assert not state["tasks"] and not requests
    assert state["errors"] == ([RuntimeError] if handler_fails else [])
