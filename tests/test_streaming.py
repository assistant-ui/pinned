import asyncio

from helpers import ASGIStream, client_for, pinned_app
from starlette.responses import StreamingResponse

from pinned import PinnedAPI, PinnedRouter, route


class Streamer(PinnedAPI):
    state: dict

    async def lifespan(self):
        self.state["activations"] += 1
        yield
        self.state["teardowns"] += 1

    @route.get("/stream")
    async def stream(self) -> StreamingResponse:
        async def gen():
            try:
                while True:
                    yield b"data: tick\n\n"
                    await asyncio.sleep(0.01)
            finally:
                self.state["stream_finally"] += 1

        return StreamingResponse(gen(), media_type="text/event-stream")

    @route.get("/ping")
    async def ping(self) -> dict:
        return {"instance": id(self)}

    @route.post("/abort")
    async def do_abort(self) -> dict:
        self.abort()
        return {}


def make_streamer():
    state = {"activations": 0, "teardowns": 0, "stream_finally": 0}
    cls = type("StreamerImpl", (Streamer,), {"state": state})
    return cls, state


async def test_stream_stays_open_across_requests_and_abort_closes_it():
    cls, state = make_streamer()
    router = PinnedRouter(cls, idle_timeout=0.05, sweep_interval=0.02)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                assert b"tick" in (await stream.next_chunk() or b"")

                first = (await client.get("/threads/a/ping")).json()["instance"]

                await asyncio.sleep(0.2)
                assert state["teardowns"] == 0
                second = (await client.get("/threads/a/ping")).json()["instance"]
                assert first == second

                await client.post("/threads/a/abort")
                await stream.drain()

            assert state["stream_finally"] == 1
            assert state["activations"] == 1


async def test_evicted_after_stream_closes():
    cls, state = make_streamer()
    router = PinnedRouter(cls, idle_timeout=0.05, sweep_interval=0.02)
    async with pinned_app(router) as app:
        async with ASGIStream(app, "/threads/a/stream") as stream:
            assert b"tick" in (await stream.next_chunk() or b"")
        await asyncio.sleep(0.3)
        assert state["stream_finally"] == 1
        assert state["teardowns"] == 1
