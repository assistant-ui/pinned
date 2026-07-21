import asyncio
import json
from contextlib import asynccontextmanager

import websockets

from pinned.lease import Lease
from pinned.registration import RegistrationClient, default_headroom, registry_ws_url

ABORT_AFTER = 0.30
FENCE_AFTER = 0.39
RENEW_INTERVAL = 0.05


class FakeCP:
    def __init__(self):
        self.connections: list[list[dict]] = []
        self.sockets = []
        self.auth_headers: list[str | None] = []
        self.paths: list[str] = []
        self.epoch = 0
        self.grant_renewals = True
        self.expire_renewals = False
        self.url = ""

    async def handler(self, ws):
        self.auth_headers.append(ws.request.headers.get("Authorization"))
        self.paths.append(ws.request.path)
        self.sockets.append(ws)
        messages: list[dict] = []
        self.connections.append(messages)
        async for raw in ws:
            msg = json.loads(raw)
            messages.append(msg)
            if msg["type"] == "register":
                self.epoch += 1
                await ws.send(
                    json.dumps(
                        {"v": 1, "type": "registered", "epoch": self.epoch, "leaseTtlMs": 15000}
                    )
                )
            elif msg["type"] == "renew":
                if self.expire_renewals:
                    await ws.send(json.dumps({"v": 1, "type": "expired"}))
                    await ws.close(4408)
                elif self.grant_renewals:
                    await ws.send(json.dumps({"v": 1, "type": "renewed", "ttlMs": 15000}))


@asynccontextmanager
async def fake_cp():
    cp = FakeCP()
    async with websockets.serve(cp.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        cp.url = f"http://127.0.0.1:{port}"
        yield cp


@asynccontextmanager
async def running_client(cp, lease, **kwargs):
    client = RegistrationClient(
        control_plane=cp.url,
        token="secret",
        namespaces=["/threads"],
        advertise="http://127.0.0.1:9000/threads",
        lease=lease,
        renew_interval=RENEW_INTERVAL,
        backoff_initial=0.05,
        backoff_max=0.1,
        **kwargs,
    )
    client.start()
    try:
        yield client
    finally:
        await client.stop()
        await lease.close()


def make_lease(aborts):
    return Lease(
        abort_after=ABORT_AFTER,
        fence_after=FENCE_AFTER,
        on_abort=lambda: aborts.append(True),
    )


async def wait_for(predicate, timeout=2.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_register_message_matches_v1_protocol():
    aborts = []
    async with fake_cp() as cp:
        async with running_client(cp, make_lease(aborts), headroom=lambda: 512) as client:
            await wait_for(lambda: client.epoch is not None)
    assert cp.paths == ["/registry/ws"]
    assert cp.auth_headers == ["Bearer secret"]
    assert cp.connections[0][0] == {
        "v": 1,
        "type": "register",
        "namespaces": ["/threads"],
        "advertise": "http://127.0.0.1:9000/threads",
        "headroom": 512,
    }


async def test_renewals_keep_lease_alive():
    aborts = []
    lease = make_lease(aborts)
    async with fake_cp() as cp:
        async with running_client(cp, lease) as client:
            await wait_for(lambda: client.epoch == 1)
            await asyncio.sleep(FENCE_AFTER * 2)
            assert lease.has_lease is True
            assert aborts == []
            renews = [m for m in cp.connections[0] if m["type"] == "renew"]
            assert renews and all(m == {"v": 1, "type": "renew"} for m in renews)


async def test_stalled_renewal_grants_abort_then_fence():
    aborts = []
    lease = make_lease(aborts)
    async with fake_cp() as cp:
        async with running_client(cp, lease):
            await wait_for(lambda: lease.has_lease)
            cp.grant_renewals = False
            await wait_for(lambda: len(aborts) == 1)
            assert lease.has_lease is True
            await wait_for(lambda: not lease.has_lease)


async def test_expired_reply_voids_placements_and_reconnects_fresh_epoch():
    aborts = []
    lease = make_lease(aborts)
    async with fake_cp() as cp:
        async with running_client(cp, lease) as client:
            await wait_for(lambda: client.epoch == 1)
            cp.expire_renewals = True
            await wait_for(lambda: len(aborts) >= 1)
            cp.expire_renewals = False
            await wait_for(lambda: client.epoch == 2)
            assert lease.has_lease is True
            assert len(cp.connections) == 2
            assert cp.connections[1][0]["type"] == "register"


async def test_connection_drop_revokes_and_reconnects():
    aborts = []
    lease = make_lease(aborts)
    async with fake_cp() as cp:
        async with running_client(cp, lease) as client:
            await wait_for(lambda: client.epoch == 1)
            await cp.sockets[0].close()
            await wait_for(lambda: len(aborts) >= 1)
            await wait_for(lambda: client.epoch == 2)
            assert lease.has_lease is True
            assert len(cp.connections) == 2


async def test_registry_ws_url_scheme_mapping():
    assert registry_ws_url("http://cp:8080") == "ws://cp:8080/registry/ws"
    assert registry_ws_url("https://cp.internal/") == "wss://cp.internal/registry/ws"
    assert registry_ws_url("wss://cp.internal") == "wss://cp.internal/registry/ws"


async def test_default_headroom_is_a_positive_int():
    value = default_headroom()
    assert isinstance(value, int) and value > 0
