import pytest
from helpers import pinned_client
from test_registration import fake_cp, wait_for

from pinned import PinnedAPI, PinnedRouter, route

LEASE_TTL = 0.45
RENEW_INTERVAL = 0.05


class Obj(PinnedAPI):
    teardowns: list[str] = []

    async def lifespan(self):
        yield
        Obj.teardowns.append(self.id)

    @route.get("/lease")
    async def lease(self) -> dict:
        return {"has_lease": self.has_lease}


def cp_router(cp):
    return PinnedRouter(
        Obj,
        control_plane=cp.url,
        advertise="http://127.0.0.1:9000/threads",
        token="secret",
        lease_ttl=LEASE_TTL,
        renew_interval=RENEW_INTERVAL,
    )


async def test_local_mode_always_has_lease():
    async with pinned_client(PinnedRouter(Obj)) as client:
        assert (await client.get("/threads/a/lease")).json() == {"has_lease": True}


async def test_cp_mode_registers_namespace_and_holds_lease():
    async with fake_cp() as cp:
        router = cp_router(cp)
        async with pinned_client(router) as client:
            await wait_for(lambda: router.registration.epoch == 1)
            assert cp.connections[0][0] == {
                "v": 1,
                "type": "register",
                "namespaces": ["/threads"],
                "advertise": "http://127.0.0.1:9000/threads",
                "headroom": cp.connections[0][0]["headroom"],
            }
            assert (await client.get("/threads/a/lease")).json() == {"has_lease": True}


async def test_cp_mode_stalled_lease_aborts_instances_and_fences():
    Obj.teardowns = []
    async with fake_cp() as cp:
        router = cp_router(cp)
        async with pinned_client(router) as client:
            await wait_for(lambda: router.registration.epoch == 1)
            assert (await client.get("/threads/a/lease")).json() == {"has_lease": True}
            cp.grant_renewals = False
            await wait_for(lambda: Obj.teardowns == ["a"])
            assert router.lease.has_lease is True
            await wait_for(lambda: not router.lease.has_lease)


async def test_cp_mode_requires_token_and_advertise(monkeypatch):
    monkeypatch.delenv("PINBOARD_TOKEN", raising=False)
    monkeypatch.delenv("ADVERTISE_URL", raising=False)
    with pytest.raises(ValueError, match="token"):
        PinnedRouter(Obj, control_plane="ws://127.0.0.1:1")
    with pytest.raises(ValueError, match="advertise"):
        PinnedRouter(Obj, control_plane="ws://127.0.0.1:1", token="t")
    with pytest.raises(ValueError, match="namespace"):
        PinnedRouter(
            Obj, control_plane="ws://127.0.0.1:1", token="t", advertise="http://h:1"
        )


async def test_cp_mode_reads_env(monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_URL", "ws://127.0.0.1:1")
    monkeypatch.setenv("ADVERTISE_URL", "http://127.0.0.1:9000/threads")
    monkeypatch.setenv("PINBOARD_TOKEN", "secret")
    router = PinnedRouter(Obj)
    assert router.registration is not None
    assert router.lease is not None
