from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any, AsyncIterator, Callable, Coroutine

from fastapi import HTTPException
from starlette.responses import Response

from .api import PinnedAPI


class InstanceRecord:
    def __init__(self, id: str, instance: PinnedAPI) -> None:
        self.id = id
        self.instance = instance
        self.tasks: set[asyncio.Task[Any]] = set()
        self.active = 0
        self.last_activity = time.monotonic()
        self.abort_event = asyncio.Event()
        self.closing = False
        self.closed = asyncio.Event()
        self.has_lease = True
        self.agen: AsyncIterator[None] | None = None
        self._abort: Callable[[], None] | None = None

    def touch(self) -> None:
        self.last_activity = time.monotonic()

    def request_abort(self) -> None:
        if self._abort is not None and not self.closing:
            self._abort()


class _Entry:
    __slots__ = ("id", "task")

    def __init__(self, id: str) -> None:
        self.id = id
        self.task: asyncio.Task[InstanceRecord] | None = None


class LocalDirectory:
    def __init__(
        self, cls: type[PinnedAPI], *, idle_timeout: float, sweep_interval: float
    ) -> None:
        self._cls = cls
        self._idle_timeout = idle_timeout
        self._sweep_interval = sweep_interval
        self._entries: dict[str, _Entry] = {}
        self._sweep_task: asyncio.Task[None] | None = None
        self._background: set[asyncio.Task[None]] = set()
        self._shutting_down = False

    async def get(self, id: str) -> InstanceRecord:
        if self._shutting_down:
            raise RuntimeError("pinned directory is shut down")
        self._ensure_sweeper()
        while True:
            entry = self._entries.get(id)
            if entry is None:
                entry = _Entry(id)
                entry.task = asyncio.create_task(self._activate(entry))
                self._entries[id] = entry
            assert entry.task is not None
            try:
                record = await asyncio.shield(entry.task)
            except Exception:
                if self._entries.get(id) is entry:
                    del self._entries[id]
                raise
            if record.closing:
                await record.closed.wait()
                continue
            return record

    async def invoke(
        self,
        record: InstanceRecord,
        bound_method: Callable[..., Coroutine[Any, Any, Any]],
        kwargs: dict[str, Any],
    ) -> Any:
        record.active += 1
        task = asyncio.create_task(bound_method(**kwargs))
        record.tasks.add(task)
        task.add_done_callback(record.tasks.discard)
        try:
            result = await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if task.cancelled() and (current is None or not current.cancelling()):
                raise HTTPException(status_code=503, detail="instance aborted") from None
            task.cancel()
            raise
        finally:
            record.active -= 1
            record.touch()
        if isinstance(result, Response):
            iterator = getattr(result, "body_iterator", None)
            if iterator is not None:
                result.body_iterator = self._guard_stream(record, iterator)
        return result

    def start(self) -> None:
        self._shutting_down = False
        self._ensure_sweeper()

    async def shutdown(self) -> None:
        self._shutting_down = True
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweep_task
            self._sweep_task = None
        entries = [e for e in self._entries.values() if e.task is not None]
        await asyncio.gather(*(e.task for e in entries), return_exceptions=True)
        closers = [
            self._close(e, e.task.result(), cancel=True)
            for e in entries
            if e.task is not None
            and e.task.done()
            and not e.task.cancelled()
            and e.task.exception() is None
        ]
        await asyncio.gather(*closers, return_exceptions=True)
        if self._background:
            await asyncio.gather(*list(self._background), return_exceptions=True)

    async def _activate(self, entry: _Entry) -> InstanceRecord:
        instance = self._cls()
        instance.id = entry.id
        record = InstanceRecord(entry.id, instance)
        record._abort = lambda: self._schedule_close(entry, record, cancel=True)
        instance._pinned_record = record
        agen = instance.lifespan()
        record.agen = agen
        try:
            await anext(agen)
        except StopAsyncIteration:
            raise RuntimeError(
                f"{type(instance).__name__}.lifespan must yield exactly once"
            ) from None
        return record

    async def _close(self, entry: _Entry, record: InstanceRecord, *, cancel: bool) -> None:
        if record.closing:
            await record.closed.wait()
            return
        record.closing = True
        record.abort_event.set()
        if cancel:
            for task in list(record.tasks):
                task.cancel()
        if record.tasks:
            await asyncio.gather(*list(record.tasks), return_exceptions=True)
        with contextlib.suppress(Exception):
            await self._finish_lifespan(record)
        if self._entries.get(entry.id) is entry:
            del self._entries[entry.id]
        record.closed.set()

    def _schedule_close(
        self, entry: _Entry, record: InstanceRecord, *, cancel: bool
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(self._close(entry, record, cancel=cancel))
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def _finish_lifespan(self, record: InstanceRecord) -> None:
        assert record.agen is not None
        try:
            await anext(record.agen)
        except StopAsyncIteration:
            return
        await record.agen.aclose()  # type: ignore[attr-defined]
        raise RuntimeError("lifespan yielded more than once")

    def _ensure_sweeper(self) -> None:
        if self._sweep_task is None or self._sweep_task.done():
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(self._sweep_interval)
            self._sweep_once()

    def _sweep_once(self) -> None:
        now = time.monotonic()
        for entry in list(self._entries.values()):
            task = entry.task
            if task is None or not task.done() or task.cancelled() or task.exception():
                continue
            record = task.result()
            if record.closing or record.active > 0 or record.tasks:
                continue
            if now - record.last_activity >= self._idle_timeout:
                self._schedule_close(entry, record, cancel=False)

    async def _guard_stream(
        self, record: InstanceRecord, inner: Any
    ) -> AsyncIterator[Any]:
        record.active += 1
        iterator = inner.__aiter__()
        try:
            while not record.abort_event.is_set():
                next_task = asyncio.ensure_future(anext(iterator))
                abort_task = asyncio.ensure_future(record.abort_event.wait())
                try:
                    await asyncio.wait(
                        {next_task, abort_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    for pending in (next_task, abort_task):
                        if not pending.done():
                            pending.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await pending
                if next_task.cancelled():
                    break
                try:
                    chunk = next_task.result()
                except StopAsyncIteration:
                    break
                record.touch()
                yield chunk
        finally:
            aclose = getattr(iterator, "aclose", None)
            if aclose is not None:
                with contextlib.suppress(Exception):
                    await aclose()
            record.active -= 1
            record.touch()
