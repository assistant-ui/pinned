from __future__ import annotations

import asyncio
import contextlib
from typing import Callable


class LeaseLostError(RuntimeError):
    pass


class Lease:
    def __init__(
        self,
        *,
        abort_after: float = 10.0,
        fence_after: float = 13.0,
        on_abort: Callable[[], None] | None = None,
    ) -> None:
        if not 0 < abort_after < fence_after:
            raise ValueError("require 0 < abort_after < fence_after")
        self._abort_after = abort_after
        self._fence_after = fence_after
        self._on_abort = on_abort
        self._has_lease = False
        self._timer: asyncio.Task[None] | None = None

    @property
    def has_lease(self) -> bool:
        return self._has_lease

    def renewed(self) -> None:
        self._has_lease = True
        self._restart_timer()

    def revoke(self) -> None:
        self._has_lease = False
        self._cancel_timer()
        self._fire_abort()

    async def close(self) -> None:
        timer = self._timer
        self._cancel_timer()
        if timer is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await timer

    def _fire_abort(self) -> None:
        if self._on_abort is not None:
            self._on_abort()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _restart_timer(self) -> None:
        self._cancel_timer()
        self._timer = asyncio.create_task(self._expire())

    async def _expire(self) -> None:
        await asyncio.sleep(self._abort_after)
        self._fire_abort()
        await asyncio.sleep(self._fence_after - self._abort_after)
        self._has_lease = False
