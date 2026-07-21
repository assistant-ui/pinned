from __future__ import annotations

from typing import Any, AsyncIterator, Iterator, Protocol, runtime_checkable

from .lease import LeaseLostError

__all__ = ["FencedSaver", "LeaseLostError"]


@runtime_checkable
class HasLease(Protocol):
    @property
    def has_lease(self) -> bool: ...


class FencedSaver:
    def __init__(self, inner: Any, lease: HasLease | None) -> None:
        self.inner = inner
        self.lease = lease

    def _check(self) -> None:
        if self.lease is None:
            raise LeaseLostError("no lease attached; refusing checkpoint write")
        if not self.lease.has_lease:
            raise LeaseLostError("lease lost; checkpoint write fenced")

    def put(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return self.inner.put(*args, **kwargs)

    def put_writes(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return self.inner.put_writes(*args, **kwargs)

    def delete_thread(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return self.inner.delete_thread(*args, **kwargs)

    async def aput(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return await self.inner.aput(*args, **kwargs)

    async def aput_writes(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return await self.inner.aput_writes(*args, **kwargs)

    async def adelete_thread(self, *args: Any, **kwargs: Any) -> Any:
        self._check()
        return await self.inner.adelete_thread(*args, **kwargs)

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return self.inner.get(*args, **kwargs)

    def get_tuple(self, *args: Any, **kwargs: Any) -> Any:
        return self.inner.get_tuple(*args, **kwargs)

    def list(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self.inner.list(*args, **kwargs)

    async def aget(self, *args: Any, **kwargs: Any) -> Any:
        return await self.inner.aget(*args, **kwargs)

    async def aget_tuple(self, *args: Any, **kwargs: Any) -> Any:
        return await self.inner.aget_tuple(*args, **kwargs)

    def alist(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self.inner.alist(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)
