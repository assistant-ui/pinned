from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from .directory import InstanceRecord


class PinnedAPI:
    id: str
    _pinned_record: "InstanceRecord | None" = None

    async def lifespan(self) -> AsyncIterator[None]:
        yield

    @property
    def has_lease(self) -> bool:
        record = self._pinned_record
        return True if record is None else record.has_lease

    def abort(self) -> None:
        record = self._pinned_record
        if record is None:
            raise RuntimeError("abort() called on an instance that is not activated")
        record.request_abort()
