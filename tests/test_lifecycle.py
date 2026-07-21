import asyncio

import pytest
from helpers import pinned_client

from pinned import PinnedAPI, PinnedRouter, route


async def test_activation_and_instance_reuse():
    state = {"activations": 0}

    class Counter(PinnedAPI):
        async def lifespan(self):
            state["activations"] += 1
            self.n = 0
            yield

        @route.post("/incr")
        async def incr(self) -> dict:
            self.n += 1
            return {"n": self.n, "instance": id(self)}

    async with pinned_client(PinnedRouter(Counter)) as client:
        r1 = (await client.post("/threads/a/incr")).json()
        r2 = (await client.post("/threads/a/incr")).json()
        r3 = (await client.post("/threads/b/incr")).json()

    assert (r1["n"], r2["n"]) == (1, 2)
    assert r1["instance"] == r2["instance"]
    assert r3["n"] == 1
    assert r3["instance"] != r1["instance"]
    assert state["activations"] == 2


async def test_lifespan_setup_teardown_ordering():
    events: list[tuple[str, str]] = []

    class Obj(PinnedAPI):
        async def lifespan(self):
            events.append(("setup", self.id))
            yield
            events.append(("teardown", self.id))

        @route.get("/ping")
        async def ping(self) -> dict:
            events.append(("request", self.id))
            return {}

    async with pinned_client(PinnedRouter(Obj)) as client:
        await client.get("/threads/x/ping")
        assert events == [("setup", "x"), ("request", "x")]
    assert events == [("setup", "x"), ("request", "x"), ("teardown", "x")]


async def test_id_is_the_routed_segment():
    class Obj(PinnedAPI):
        @route.get("/whoami")
        async def whoami(self) -> dict:
            return {"id": self.id}

    async with pinned_client(PinnedRouter(Obj)) as client:
        assert (await client.get("/threads/abc-123/whoami")).json() == {"id": "abc-123"}


def test_sync_handler_rejected():
    with pytest.raises(TypeError, match="async def"):

        class Bad(PinnedAPI):
            @route.get("/x")
            def x(self):
                return {}


async def test_idle_eviction_and_reactivation():
    state = {"activations": 0, "teardowns": 0}

    class Obj(PinnedAPI):
        async def lifespan(self):
            state["activations"] += 1
            yield
            state["teardowns"] += 1

        @route.get("/ping")
        async def ping(self) -> dict:
            return {}

    router = PinnedRouter(Obj, idle_timeout=0.05, sweep_interval=0.02)
    async with pinned_client(router) as client:
        await client.get("/threads/a/ping")
        await asyncio.sleep(0.3)
        assert state == {"activations": 1, "teardowns": 1}
        await client.get("/threads/a/ping")
        assert state["activations"] == 2


async def test_concurrent_first_requests_activate_once():
    state = {"activations": 0}

    class Obj(PinnedAPI):
        async def lifespan(self):
            state["activations"] += 1
            await asyncio.sleep(0.05)
            self.n = 0
            yield

        @route.post("/incr")
        async def incr(self) -> dict:
            self.n += 1
            return {"n": self.n}

    async with pinned_client(PinnedRouter(Obj)) as client:
        responses = await asyncio.gather(
            *(client.post("/threads/a/incr") for _ in range(10))
        )

    assert state["activations"] == 1
    assert sorted(r.json()["n"] for r in responses) == list(range(1, 11))


async def test_abort_cancels_inflight_and_next_request_reactivates():
    state = {"activations": 0, "finally": 0}

    class Obj(PinnedAPI):
        async def lifespan(self):
            state["activations"] += 1
            yield

        @route.get("/slow")
        async def slow(self) -> dict:
            try:
                await asyncio.sleep(30)
            finally:
                state["finally"] += 1
            return {}

        @route.post("/abort")
        async def do_abort(self) -> dict:
            self.abort()
            return {}

        @route.get("/ping")
        async def ping(self) -> dict:
            return {"instance": id(self)}

    async with pinned_client(PinnedRouter(Obj)) as client:
        slow_task = asyncio.create_task(client.get("/threads/a/slow"))
        await asyncio.sleep(0.05)
        assert state["activations"] == 1

        abort_response = await client.post("/threads/a/abort")
        assert abort_response.status_code == 200

        slow_response = await slow_task
        assert slow_response.status_code == 503
        assert state["finally"] == 1

        assert (await client.get("/threads/a/ping")).status_code == 200

    assert state["activations"] == 2
