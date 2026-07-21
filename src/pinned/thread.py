from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, AsyncIterator, Awaitable

from fastapi import HTTPException
from starlette.responses import StreamingResponse

from . import gorp
from .api import PinnedAPI
from .routing import route

logger = logging.getLogger("pinned.thread")

_MODES = ("reject", "queue", "interrupt")
_LAGGARD: Any = object()


class Thread(PinnedAPI):
    double_texting: str = "reject"
    stream_queue_size: int = 256

    def __init__(self) -> None:
        if self.double_texting not in _MODES:
            raise ValueError(
                f"double_texting must be one of {_MODES}, got {self.double_texting!r}"
            )
        self.state: dict[str, Any] = {}
        self._seq = 0
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._pending: deque[Any] = deque()
        self._run_task: asyncio.Task[None] | None = None

    def run(self, command: Any) -> Awaitable[None] | AsyncIterator[Any]:
        raise NotImplementedError(f"{type(self).__name__} must implement run()")

    @property
    def seq(self) -> int:
        return self._seq

    def set(self, path: list[Any], value: Any) -> None:
        if not path:
            if not isinstance(value, dict):
                raise TypeError("setting the root state requires a dict value")
            self.state = value
        else:
            parent = self._walk(path[:-1])
            parent[path[-1]] = value
        self._broadcast(gorp.set_op(path, value, self._bump()))

    def append(self, path: list[Any], value: Any) -> None:
        if not path:
            raise ValueError("append requires a non-empty path")
        parent = self._walk(path[:-1])
        key = path[-1]
        if isinstance(parent, dict) and key not in parent:
            parent[key] = []
        parent[key].append(value)
        self._broadcast(gorp.append_op(path, value, self._bump()))

    @route.get("/stream")
    async def stream(self) -> StreamingResponse:
        return StreamingResponse(self._frames(), media_type="text/event-stream")

    @route.post("/commands")
    async def send(self, body: dict) -> dict:
        if body.get("type") == "cancel":
            return await self._cancel()
        task = self._run_task
        if task is not None and not task.done():
            mode = self.double_texting
            if mode == "reject":
                raise HTTPException(status_code=409, detail="a run is already active")
            if mode == "queue":
                seq = self._bump()
                self._pending.append(body)
                return {"seq": seq, "status": "queued"}
            task.cancel()
            await asyncio.wait([task])
            seq = self._bump()
            self._spawn(body)
            return {"seq": seq, "status": "interrupted"}
        seq = self._bump()
        self._spawn(body)
        return {"seq": seq, "status": "started"}

    async def _cancel(self) -> dict:
        seq = self._bump()
        task = self._run_task
        if task is None or task.done():
            return {"seq": seq, "status": "idle"}
        task.cancel()
        await asyncio.wait([task])
        self._finish_or_continue()
        return {"seq": seq, "status": "cancelled"}

    def _spawn(self, command: Any) -> None:
        record = self._pinned_record
        if record is not None and (record.closing or record.abort_event.is_set()):
            return
        task = asyncio.create_task(self._execute(command))
        self._run_task = task
        if record is not None:
            record.tasks.add(task)
            task.add_done_callback(record.tasks.discard)
            record.touch()

    async def _execute(self, command: Any) -> None:
        try:
            result = self.run(command)
            if hasattr(result, "__aiter__"):
                async for item in result:
                    if item is not None:
                        self.append(["messages"], item)
            else:
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("run failed for %s id=%s", type(self).__name__, self.id)
            self._pending.clear()
            self._broadcast(gorp.terminal("error", self._seq))
            return
        self._finish_or_continue()

    def _finish_or_continue(self) -> None:
        record = self._pinned_record
        if record is not None and (record.closing or record.abort_event.is_set()):
            return
        if self._pending:
            self._spawn(self._pending.popleft())
        else:
            self._broadcast(gorp.terminal("completed", self._seq))

    async def _frames(self) -> AsyncIterator[bytes]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self.stream_queue_size)
        self._subscribers.add(q)
        try:
            yield gorp.encode(gorp.snapshot(self.state, self._seq))
            while True:
                frame = await q.get()
                if frame is _LAGGARD:
                    return
                yield gorp.encode(frame)
                if gorp.is_terminal(frame):
                    return
        except asyncio.CancelledError:
            # Instance abort cancels the pending frame read; yielding here completes
            # the cancelled anext so eviction never silently drops a stream.
            yield gorp.encode(gorp.terminal("evicted", self._seq))
        finally:
            self._subscribers.discard(q)

    def _broadcast(self, frame: dict[str, Any]) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(frame)
            except asyncio.QueueFull:
                self._subscribers.discard(q)
                q.get_nowait()
                q.put_nowait(_LAGGARD)

    def _bump(self) -> int:
        self._seq += 1
        return self._seq

    def _walk(self, path: list[Any]) -> Any:
        node: Any = self.state
        for key in path:
            if isinstance(node, dict):
                node = node.setdefault(key, {})
            else:
                node = node[key]
        return node
