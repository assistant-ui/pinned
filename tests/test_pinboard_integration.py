import asyncio
import contextlib
import shutil
import socket
from pathlib import Path

import httpx
import pytest
from test_registration import wait_for

from pinned.lease import Lease
from pinned.registration import RegistrationClient

PINBOARD_MAIN = Path(__file__).parent.parent / "pinboard" / "dist" / "src" / "main.js"
TOKEN = "integration-token"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None or not PINBOARD_MAIN.exists(),
    reason="node or built pinboard dist unavailable",
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_registers_against_real_pinboard():
    port = free_port()
    node = shutil.which("node")
    assert node is not None
    proc = await asyncio.create_subprocess_exec(
        node,
        str(PINBOARD_MAIN),
        env={
            "PORT": str(port),
            "REDIS_URL": "memory:",
            "PINBOARD_TOKEN": TOKEN,
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    lease = Lease(abort_after=1.0, fence_after=1.3)
    client = RegistrationClient(
        control_plane=base,
        token=TOKEN,
        namespaces=["/threads"],
        advertise="http://127.0.0.1:9000/threads",
        lease=lease,
        renew_interval=0.1,
    )
    try:
        async with httpx.AsyncClient() as http:
            async with asyncio.timeout(10):
                while True:
                    with contextlib.suppress(httpx.TransportError):
                        if (await http.get(f"{base}/healthz")).status_code == 200:
                            break
                    await asyncio.sleep(0.1)
            client.start()
            await wait_for(lambda: client.epoch is not None, timeout=5)
            assert lease.has_lease is True
            response = await http.get(
                f"{base}/registry/workers",
                headers={"Authorization": f"Bearer {TOKEN}"},
            )
            workers = response.json()["workers"]
            assert [w for w in workers if w["epoch"] == client.epoch] == [
                {
                    "epoch": client.epoch,
                    "url": "http://127.0.0.1:9000/threads",
                    "namespaces": ["/threads"],
                    "headroom": workers[0]["headroom"],
                }
            ]
            await asyncio.sleep(0.5)
            assert lease.has_lease is True
    finally:
        await client.stop()
        await lease.close()
        proc.terminate()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
