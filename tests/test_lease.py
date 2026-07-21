import asyncio

from pinned.lease import Lease

ABORT_AFTER = 0.30
FENCE_AFTER = 0.39


def make_lease():
    aborts: list[float] = []
    loop = asyncio.get_event_loop()
    lease = Lease(
        abort_after=ABORT_AFTER,
        fence_after=FENCE_AFTER,
        on_abort=lambda: aborts.append(loop.time()),
    )
    return lease, aborts


async def test_lease_starts_without_lease():
    lease, aborts = make_lease()
    assert lease.has_lease is False
    await lease.close()


async def test_renewals_keep_lease_alive():
    lease, aborts = make_lease()
    lease.renewed()
    for _ in range(6):
        await asyncio.sleep(ABORT_AFTER / 3)
        lease.renewed()
    assert lease.has_lease is True
    assert aborts == []
    await lease.close()


async def test_stalled_renewals_abort_then_fence():
    lease, aborts = make_lease()
    lease.renewed()
    await asyncio.sleep(ABORT_AFTER * 0.6)
    assert aborts == []
    assert lease.has_lease is True
    await asyncio.sleep(ABORT_AFTER * 0.6)
    assert len(aborts) == 1
    assert lease.has_lease is True
    await asyncio.sleep(FENCE_AFTER - ABORT_AFTER)
    assert lease.has_lease is False
    assert len(aborts) == 1
    await lease.close()


async def test_recovery_before_abort_line_is_a_noop():
    lease, aborts = make_lease()
    lease.renewed()
    await asyncio.sleep(ABORT_AFTER * 0.6)
    lease.renewed()
    await asyncio.sleep(ABORT_AFTER * 0.8)
    assert aborts == []
    assert lease.has_lease is True
    await lease.close()


async def test_renewal_after_abort_restores_lease():
    lease, aborts = make_lease()
    lease.renewed()
    await asyncio.sleep(ABORT_AFTER * 1.2)
    assert len(aborts) == 1
    lease.renewed()
    await asyncio.sleep(ABORT_AFTER * 0.5)
    assert lease.has_lease is True
    assert len(aborts) == 1
    await lease.close()


async def test_revoke_aborts_and_fences_immediately():
    lease, aborts = make_lease()
    lease.renewed()
    lease.revoke()
    assert lease.has_lease is False
    assert len(aborts) == 1
    await asyncio.sleep(FENCE_AFTER * 1.2)
    assert len(aborts) == 1
    await lease.close()
