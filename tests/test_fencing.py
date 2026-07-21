import pytest

from pinned.fencing import FencedSaver, LeaseLostError


class FakeSaver:
    def __init__(self):
        self.calls = []

    def put(self, config, checkpoint, metadata, new_versions):
        self.calls.append("put")
        return config

    def put_writes(self, config, writes, task_id, task_path=""):
        self.calls.append("put_writes")

    def delete_thread(self, thread_id):
        self.calls.append("delete_thread")

    async def aput(self, config, checkpoint, metadata, new_versions):
        self.calls.append("aput")
        return config

    async def aput_writes(self, config, writes, task_id, task_path=""):
        self.calls.append("aput_writes")

    async def adelete_thread(self, thread_id):
        self.calls.append("adelete_thread")

    def get(self, config):
        self.calls.append("get")
        return {"checkpoint": 1}

    def get_tuple(self, config):
        self.calls.append("get_tuple")
        return ("tuple",)

    def list(self, config):
        self.calls.append("list")
        yield "item"

    async def aget(self, config):
        self.calls.append("aget")
        return {"checkpoint": 1}

    async def aget_tuple(self, config):
        self.calls.append("aget_tuple")
        return ("tuple",)

    async def alist(self, config):
        self.calls.append("alist")
        yield "item"

    def get_next_version(self, current, channel):
        self.calls.append("get_next_version")
        return 2


class FakeLease:
    def __init__(self, has_lease):
        self.has_lease = has_lease


async def test_writes_pass_through_while_lease_held():
    inner = FakeSaver()
    saver = FencedSaver(inner, FakeLease(True))
    assert saver.put({}, {}, {}, {}) == {}
    saver.put_writes({}, [], "t")
    saver.delete_thread("t")
    await saver.aput({}, {}, {}, {})
    await saver.aput_writes({}, [], "t")
    await saver.adelete_thread("t")
    assert inner.calls == [
        "put",
        "put_writes",
        "delete_thread",
        "aput",
        "aput_writes",
        "adelete_thread",
    ]


async def test_writes_fenced_after_lease_loss():
    inner = FakeSaver()
    lease = FakeLease(True)
    saver = FencedSaver(inner, lease)
    lease.has_lease = False
    with pytest.raises(LeaseLostError):
        saver.put({}, {}, {}, {})
    with pytest.raises(LeaseLostError):
        saver.put_writes({}, [], "t")
    with pytest.raises(LeaseLostError):
        saver.delete_thread("t")
    with pytest.raises(LeaseLostError):
        await saver.aput({}, {}, {}, {})
    with pytest.raises(LeaseLostError):
        await saver.aput_writes({}, [], "t")
    with pytest.raises(LeaseLostError):
        await saver.adelete_thread("t")
    assert inner.calls == []


async def test_reads_pass_through_regardless_of_lease():
    inner = FakeSaver()
    saver = FencedSaver(inner, FakeLease(False))
    assert saver.get({}) == {"checkpoint": 1}
    assert saver.get_tuple({}) == ("tuple",)
    assert list(saver.list({})) == ["item"]
    assert await saver.aget({}) == {"checkpoint": 1}
    assert await saver.aget_tuple({}) == ("tuple",)
    assert [item async for item in saver.alist({})] == ["item"]


async def test_absent_lease_fails_safe():
    inner = FakeSaver()
    saver = FencedSaver(inner, None)
    with pytest.raises(LeaseLostError):
        saver.put({}, {}, {}, {})
    with pytest.raises(LeaseLostError):
        await saver.aput_writes({}, [], "t")
    assert saver.get({}) == {"checkpoint": 1}


async def test_unknown_attributes_delegate_to_inner():
    inner = FakeSaver()
    saver = FencedSaver(inner, FakeLease(True))
    assert saver.get_next_version(1, None) == 2
