from __future__ import annotations

import inspect
import os
import typing
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from urllib.parse import urlsplit

from fastapi import APIRouter, Request

from .api import PinnedAPI
from .directory import LocalDirectory
from .lease import Lease
from .registration import RegistrationClient
from .routing import RouteSpec, routes_of

_FETCH_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]


class PinnedRouter(APIRouter):
    def __init__(
        self,
        cls: type[PinnedAPI],
        *,
        control_plane: str | None = None,
        advertise: str | None = None,
        namespace: str | None = None,
        token: str | None = None,
        idle_timeout: float = 300.0,
        sweep_interval: float | None = None,
        lease_ttl: float = 15.0,
        renew_interval: float = 3.0,
        **kwargs: Any,
    ) -> None:
        if not (isinstance(cls, type) and issubclass(cls, PinnedAPI)):
            raise TypeError("PinnedRouter requires a PinnedAPI subclass")
        control_plane = control_plane or os.environ.get("CONTROL_PLANE_URL")
        advertise = advertise or os.environ.get("ADVERTISE_URL")
        if sweep_interval is None:
            sweep_interval = max(min(idle_timeout / 10, 30.0), 0.05)
        self.cls = cls
        self.advertise = advertise
        self.directory = LocalDirectory(
            cls, idle_timeout=idle_timeout, sweep_interval=sweep_interval
        )
        self.lease: Lease | None = None
        self.registration: RegistrationClient | None = None
        if control_plane is not None:
            token = token or os.environ.get("PINBOARD_TOKEN")
            if token is None:
                raise ValueError(
                    "control plane mode requires a token (PINBOARD_TOKEN or token=)"
                )
            if advertise is None:
                raise ValueError(
                    "control plane mode requires an advertise URL (ADVERTISE_URL or advertise=)"
                )
            namespace = namespace or urlsplit(advertise).path.rstrip("/") or None
            if namespace is None:
                raise ValueError(
                    "control plane mode requires a namespace (namespace= or a path "
                    "on the advertise URL)"
                )
            self.lease = Lease(
                abort_after=lease_ttl * (10 / 15),
                fence_after=lease_ttl * (13 / 15),
                on_abort=self.directory.abort_all,
            )
            self.directory.lease = self.lease
            self.registration = RegistrationClient(
                control_plane=control_plane,
                token=token,
                namespaces=[namespace],
                advertise=advertise,
                lease=self.lease,
                renew_interval=renew_interval,
            )
        super().__init__(lifespan=self.lifespan, **kwargs)
        self._mount_handlers()

    @asynccontextmanager
    async def lifespan(self, app: Any = None) -> AsyncIterator[None]:
        self.directory.start()
        if self.registration is not None:
            self.registration.start()
        try:
            yield
        finally:
            if self.registration is not None:
                await self.registration.stop()
            if self.lease is not None:
                await self.lease.close()
            await self.directory.shutdown()

    def _mount_handlers(self) -> None:
        members: dict[str, Any] = {}
        for klass in reversed(self.cls.__mro__):
            members.update(vars(klass))
        for name, fn in members.items():
            specs = routes_of(fn)
            if specs:
                self._add_handler(name, fn, specs)
        fetch = members.get("fetch")
        if fetch is not None:
            if not inspect.iscoroutinefunction(fetch):
                raise TypeError(
                    f"{self.cls.__name__}.fetch must be 'async def'; "
                    "sync handlers are unsupported"
                )
            self._add_fetch()

    def _add_handler(self, name: str, fn: Any, specs: list[RouteSpec]) -> None:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if not params or params[0].name != "self":
            raise TypeError(f"handler {fn.__qualname__!r} must be an instance method")
        params = params[1:]
        if any(p.name == "id" for p in params):
            raise TypeError(
                f"handler {fn.__qualname__!r} may not declare a parameter named 'id'; "
                "'id' is the routed id path segment"
            )
        hints = typing.get_type_hints(fn)
        params = [p.replace(annotation=hints.get(p.name, p.annotation)) for p in params]
        id_param = inspect.Parameter(
            "id", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str
        )
        directory = self.directory

        async def endpoint(**kwargs: Any) -> Any:
            instance_id = kwargs.pop("id")
            record = await directory.get(instance_id)
            return await directory.invoke(record, getattr(record.instance, name), kwargs)

        endpoint.__signature__ = sig.replace(  # type: ignore[attr-defined]
            parameters=[id_param, *params],
            return_annotation=hints.get("return", sig.return_annotation),
        )
        endpoint.__name__ = name
        endpoint.__doc__ = fn.__doc__
        for spec in specs:
            self.add_api_route(
                "/{id}" + spec.path, endpoint, methods=[spec.method], **spec.kwargs
            )

    def _add_fetch(self) -> None:
        directory = self.directory

        async def fetch_root(id: str, request: Request) -> Any:
            record = await directory.get(id)
            return await directory.invoke(record, record.instance.fetch, {"request": request})  # type: ignore[attr-defined]

        async def fetch_path(id: str, path: str, request: Request) -> Any:
            record = await directory.get(id)
            return await directory.invoke(record, record.instance.fetch, {"request": request})  # type: ignore[attr-defined]

        self.add_api_route(
            "/{id}", fetch_root, methods=_FETCH_METHODS, include_in_schema=False
        )
        self.add_api_route(
            "/{id}/{path:path}", fetch_path, methods=_FETCH_METHODS, include_in_schema=False
        )
