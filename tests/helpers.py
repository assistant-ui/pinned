import asyncio
import contextlib
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from pinned import PinnedRouter


@asynccontextmanager
async def pinned_app(router: PinnedRouter, prefix: str = "/threads"):
    app = FastAPI()
    app.include_router(router, prefix=prefix)
    async with router.lifespan(app):
        yield app


def client_for(app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@asynccontextmanager
async def pinned_client(router: PinnedRouter, prefix: str = "/threads"):
    async with pinned_app(router, prefix) as app:
        async with client_for(app) as client:
            yield client


class ASGIStream:
    """Incremental ASGI response reader; httpx.ASGITransport buffers full bodies."""

    def __init__(self, app, path: str, method: str = "GET") -> None:
        self._app = app
        self._path = path
        self._method = method
        self._chunks: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._started = asyncio.Event()
        self._disconnected = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.status: int | None = None

    async def _receive(self):
        await self._disconnected.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message) -> None:
        if message["type"] == "http.response.start":
            self.status = message["status"]
            self._started.set()
        elif message["type"] == "http.response.body":
            body = message.get("body", b"")
            if body:
                await self._chunks.put(body)
            if not message.get("more_body"):
                await self._chunks.put(None)

    async def __aenter__(self) -> "ASGIStream":
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": self._method,
            "scheme": "http",
            "path": self._path,
            "raw_path": self._path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"test")],
            "server": ("test", 80),
            "client": ("test", 1234),
        }
        self._task = asyncio.create_task(self._app(scope, self._receive, self._send))
        await asyncio.wait_for(self._started.wait(), timeout=5)
        return self

    async def next_chunk(self, timeout: float = 5) -> bytes | None:
        return await asyncio.wait_for(self._chunks.get(), timeout=timeout)

    async def drain(self, timeout: float = 5) -> None:
        while await self.next_chunk(timeout=timeout) is not None:
            pass

    async def __aexit__(self, *exc) -> None:
        self._disconnected.set()
        assert self._task is not None
        try:
            await asyncio.wait_for(self._task, timeout=5)
        except Exception:
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task
