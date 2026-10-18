"""Isolated feasibility adapter, NOT a production transport or policy engine."""

import asyncio
import ipaddress
from contextlib import suppress
from contextvars import ContextVar

import httpcore
import httpx

approved_plan = ContextVar("p0_approved_plan")


class PeerMismatch(RuntimeError):
    """Safe test-fixture rejection; deliberately not a Gateway exception."""


class PeerBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, owner):
        self.owner = owner
        self.peer = None
        self.delegate = httpcore.AnyIOBackend()

    async def connect_tcp(self, host, port, **kwargs):
        # Only numeric addresses reach the network backend; no logical DNS lookup.
        address = approved_plan.get()[0]
        ipaddress.ip_address(address)
        destination = self.owner.dial_address(address)
        stream = await self.delegate.connect_tcp(destination, port, **kwargs)
        peer = stream.get_extra_info("server_addr")
        if peer is None or peer[0] != address or peer[1] != port:
            # Preserve rejection even if close raises; never log raw cleanup text.
            with suppress(BaseException):
                await stream.aclose()
            raise PeerMismatch("connected peer rejected") from None
        self.peer = peer
        self.owner.dials.append((host, address, port))
        return stream


class CheckedConnection(httpcore.AsyncHTTPConnection):
    def __init__(self, origin, owner):
        self.owner = owner
        self.backend = PeerBackend(owner)
        self.dispatch_lock = asyncio.Lock()
        super().__init__(
            origin, ssl_context=owner.context, network_backend=self.backend, retries=0
        )

    async def handle_async_request(self, request):
        async with self.dispatch_lock:
            return await self.checked_dispatch(request)

    async def checked_dispatch(self, request):
        approved = request.extensions["p0_approved"]
        if self.backend.peer is not None and self.backend.peer[0] not in approved:
            # Before any request bytes: pool reselects, not an upstream retry.
            await self.aclose()
            raise httpcore.ConnectionNotAvailable()
        token = approved_plan.set(approved)
        try:
            return await super().handle_async_request(request)
        finally:
            approved_plan.reset(token)


class CheckedPool(httpcore.AsyncConnectionPool):
    def __init__(self, owner):
        self.owner = owner
        super().__init__(max_connections=100, max_keepalive_connections=20)

    def create_connection(self, origin):
        return CheckedConnection(origin, self.owner)


class ResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream):
        self.stream = stream

    async def __aiter__(self):
        async for chunk in self.stream:
            yield chunk

    async def aclose(self):
        await self.stream.aclose()


class ValidatedTransport(httpx.AsyncBaseTransport):
    def __init__(self, resolver, context=None, *, dial_address=lambda address: address):
        self.resolver = resolver
        self.context = context
        self.dial_address = dial_address
        self.denied = set()
        self.dials = []
        self.pool = CheckedPool(self)

    async def handle_async_request(self, request):
        answers = tuple(self.resolver(request.url.host))
        port = request.url.port or (443 if request.url.scheme == "https" else 80)
        if not answers or any(
            (str(ipaddress.ip_address(ip)), port) in self.denied for ip in answers
        ):
            raise ValueError("destination denied")
        extensions = {**request.extensions, "p0_approved": answers}
        response = await self.pool.handle_async_request(
            httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=request.headers.raw,
                content=request.stream,
                extensions=extensions,
            )
        )
        return httpx.Response(
            response.status,
            headers=response.headers,
            stream=ResponseStream(response.stream),
            extensions=response.extensions,
        )

    async def aclose(self):
        await self.pool.aclose()
