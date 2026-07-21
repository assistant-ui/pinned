from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any, Callable, Sequence

import websockets

from .lease import Lease

DEFAULT_HEADROOM_MB = 1024


def default_headroom() -> int:
    try:
        import psutil
    except ImportError:
        return DEFAULT_HEADROOM_MB
    return int(psutil.virtual_memory().available // (1024 * 1024))


def registry_ws_url(control_plane: str) -> str:
    base = control_plane.rstrip("/")
    for prefix, replacement in (("http://", "ws://"), ("https://", "wss://")):
        if base.startswith(prefix):
            base = replacement + base[len(prefix) :]
    return base + "/registry/ws"


class RegistrationClient:
    def __init__(
        self,
        *,
        control_plane: str,
        token: str,
        namespaces: Sequence[str],
        advertise: str,
        lease: Lease,
        headroom: Callable[[], int] = default_headroom,
        renew_interval: float = 3.0,
        backoff_initial: float = 0.5,
        backoff_max: float = 5.0,
    ) -> None:
        self._url = registry_ws_url(control_plane)
        self._token = token
        self._namespaces = list(namespaces)
        self._advertise = advertise
        self._lease = lease
        self._headroom = headroom
        self._renew_interval = renew_interval
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._task: asyncio.Task[None] | None = None
        self.epoch: int | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        backoff = self._backoff_initial
        while True:
            try:
                await self._session()
                backoff = self._backoff_initial
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            self.epoch = None
            self._lease.revoke()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    async def _session(self) -> None:
        headers = {"Authorization": f"Bearer {self._token}"}
        async with websockets.connect(self._url, additional_headers=headers) as ws:
            headroom = self._headroom()
            await ws.send(
                json.dumps(
                    {
                        "v": 1,
                        "type": "register",
                        "namespaces": self._namespaces,
                        "advertise": self._advertise,
                        "headroom": headroom,
                    }
                )
            )
            renewer: asyncio.Task[None] | None = None
            try:
                async for raw in ws:
                    msg = self._parse(raw)
                    if msg is None:
                        continue
                    msg_type = msg.get("type")
                    if msg_type == "registered":
                        epoch = msg.get("epoch")
                        self.epoch = epoch if isinstance(epoch, int) else None
                        self._lease.renewed()
                        if renewer is None:
                            renewer = asyncio.create_task(self._renew_loop(ws, headroom))
                    elif msg_type == "renewed":
                        self._lease.renewed()
                    elif msg_type in ("expired", "error"):
                        return
            finally:
                if renewer is not None:
                    renewer.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await renewer

    async def _renew_loop(self, ws: Any, last_headroom: int) -> None:
        while True:
            await asyncio.sleep(self._renew_interval)
            await ws.send(json.dumps({"v": 1, "type": "renew"}))
            headroom = self._headroom()
            if headroom != last_headroom:
                await ws.send(json.dumps({"v": 1, "type": "headroom", "headroom": headroom}))
                last_headroom = headroom

    @staticmethod
    def _parse(raw: str | bytes) -> dict[str, Any] | None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(msg, dict) or msg.get("v") != 1:
            return None
        return msg
