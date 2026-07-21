import asyncio
import json

from helpers import ASGIStream, client_for, pinned_app, pinned_client

from pinned import PinnedRouter, Thread, route


def frame_of(chunk: bytes | None) -> dict:
    assert chunk is not None
    text = chunk.decode()
    assert text.startswith("data: ") and text.endswith("\n\n")
    return json.loads(text[6:-2])


async def read_until_end(stream: ASGIStream) -> list[dict]:
    frames = []
    while (chunk := await stream.next_chunk()) is not None:
        frames.append(frame_of(chunk))
    return frames


class Echo(Thread):
    async def run(self, command):
        self.append(["messages"], {"role": "user", "text": command["text"]})
        await asyncio.sleep(command.get("delay", 0))
        self.append(["messages"], {"role": "assistant", "text": command["text"].upper()})


class QueueEcho(Echo):
    double_texting = "queue"


class InterruptEcho(Echo):
    double_texting = "interrupt"


async def test_reject_mode_409_while_run_active():
    async with pinned_client(PinnedRouter(Echo)) as client:
        first = await client.post(
            "/threads/a/commands", json={"text": "hi", "delay": 0.2}
        )
        assert first.status_code == 200
        assert first.json()["status"] == "started"
        second = await client.post("/threads/a/commands", json={"text": "again"})
        assert second.status_code == 409


async def test_queue_mode_runs_sequentially():
    router = PinnedRouter(QueueEcho)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                snap = frame_of(await stream.next_chunk())
                assert snap["op"] == "set" and snap["path"] == []

                ack1 = (
                    await client.post(
                        "/threads/a/commands", json={"text": "hi", "delay": 0.05}
                    )
                ).json()
                ack2 = (
                    await client.post("/threads/a/commands", json={"text": "yo"})
                ).json()
                assert ack1["status"] == "started"
                assert ack2["status"] == "queued"

                frames = await read_until_end(stream)

    terminals = [f for f in frames if f["op"] == "terminal"]
    assert terminals == [frames[-1]]
    assert frames[-1]["reason"] == "completed"
    texts = [f["value"]["text"] for f in frames if f["op"] == "append"]
    assert texts == ["hi", "HI", "yo", "YO"]


async def test_interrupt_mode_cancels_current_run():
    router = PinnedRouter(InterruptEcho)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            ack1 = (
                await client.post(
                    "/threads/a/commands", json={"text": "one", "delay": 5}
                )
            ).json()
            await asyncio.sleep(0.02)
            ack2 = (
                await client.post("/threads/a/commands", json={"text": "two"})
            ).json()
            assert ack1["status"] == "started"
            assert ack2["status"] == "interrupted"
            await asyncio.sleep(0.05)

            async with ASGIStream(app, "/threads/a/stream") as stream:
                snap = frame_of(await stream.next_chunk())

    texts = [m["text"] for m in snap["value"]["messages"]]
    assert texts == ["one", "two", "TWO"]


async def test_stream_protocol_snapshot_updates_terminal_completed():
    router = PinnedRouter(Echo)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                snap = frame_of(await stream.next_chunk())
                assert snap == {"v": 1, "op": "set", "path": [], "value": {}, "seq": 0}

                ack = (
                    await client.post("/threads/a/commands", json={"text": "hi"})
                ).json()
                assert ack["seq"] == 1

                frames = await read_until_end(stream)

    assert [f["op"] for f in frames] == ["append", "append", "terminal"]
    assert [f["seq"] for f in frames] == [2, 3, 3]
    assert all(f["v"] == 1 for f in frames)
    assert frames[0]["path"] == ["messages"]
    assert frames[0]["value"] == {"role": "user", "text": "hi"}
    assert frames[2]["reason"] == "completed"


async def test_abort_mid_run_yields_terminal_evicted_and_cancels_run():
    cancelled = {"count": 0}

    class Abortable(Thread):
        async def run(self, command):
            self.set(["phase"], "working")
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled["count"] += 1
                raise

        @route.post("/abort")
        async def do_abort(self) -> dict:
            self.abort()
            return {}

    router = PinnedRouter(Abortable)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                frame_of(await stream.next_chunk())
                await client.post("/threads/a/commands", json={})
                working = frame_of(await stream.next_chunk())
                assert working == {
                    "v": 1,
                    "op": "set",
                    "path": ["phase"],
                    "value": "working",
                    "seq": 2,
                }

                await client.post("/threads/a/abort")
                frames = await read_until_end(stream)

    assert frames == [{"v": 1, "op": "terminal", "reason": "evicted", "seq": 2}]
    assert cancelled["count"] == 1


async def test_error_in_run_yields_terminal_error():
    class Failing(Thread):
        async def run(self, command):
            self.set(["phase"], "working")
            raise ValueError("boom")

    router = PinnedRouter(Failing)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                frame_of(await stream.next_chunk())
                await client.post("/threads/a/commands", json={})
                frames = await read_until_end(stream)

            assert frames[-1]["op"] == "terminal"
            assert frames[-1]["reason"] == "error"

            retry = (await client.post("/threads/a/commands", json={})).json()
            assert retry["status"] == "started"


async def test_reconnect_snapshot_and_seq_watermark():
    router = PinnedRouter(Echo)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            ack = (
                await client.post("/threads/a/commands", json={"text": "hi"})
            ).json()
            await asyncio.sleep(0.05)

            async with ASGIStream(app, "/threads/a/stream") as stream:
                snap = frame_of(await stream.next_chunk())

    assert snap["op"] == "set" and snap["path"] == []
    assert [m["text"] for m in snap["value"]["messages"]] == ["hi", "HI"]
    assert snap["seq"] >= ack["seq"]
    assert snap["seq"] == ack["seq"] + 2


async def test_cancel_command_stops_active_run():
    router = PinnedRouter(Echo)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            async with ASGIStream(app, "/threads/a/stream") as stream:
                frame_of(await stream.next_chunk())
                await client.post(
                    "/threads/a/commands", json={"text": "hi", "delay": 30}
                )
                frame_of(await stream.next_chunk())

                ack = (
                    await client.post("/threads/a/commands", json={"type": "cancel"})
                ).json()
                assert ack["status"] == "cancelled"

                frames = await read_until_end(stream)

    assert frames == [{"v": 1, "op": "terminal", "reason": "completed", "seq": 3}]


async def test_async_generator_run_appends_yields_to_messages():
    class GenEcho(Thread):
        async def run(self, command):
            yield {"role": "user", "text": command["text"]}
            yield {"role": "assistant", "text": command["text"].upper()}

    router = PinnedRouter(GenEcho)
    async with pinned_app(router) as app:
        async with client_for(app) as client:
            await client.post("/threads/a/commands", json={"text": "hi"})
            await asyncio.sleep(0.05)
            async with ASGIStream(app, "/threads/a/stream") as stream:
                snap = frame_of(await stream.next_chunk())

    assert [m["text"] for m in snap["value"]["messages"]] == ["hi", "HI"]
