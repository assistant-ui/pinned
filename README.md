# pinned

One live instance of your class per id, with an HTTP surface. Inspired by Cloudflare
Durable Objects: all traffic for id X reaches the one object living for X. Inside, an
instance is an ordinary single-threaded asyncio server: handlers interleave at awaits,
state is plain attributes, no locks.

This repo currently ships local mode only: a single process with an in-memory
directory. The control plane (pinboard: cluster-wide placement, leases, fencing) is
designed in `DESIGN.md` but not built yet. `has_lease` is always `True` in local mode.

## Usage

```python
from fastapi import FastAPI
from pinned import PinnedAPI, PinnedRouter, route

class Thread(PinnedAPI):
    async def lifespan(self):
        self.messages = []
        yield
        # teardown on idle eviction or shutdown

    @route.post("/messages")
    async def send(self, body: dict) -> dict:
        self.messages.append(body)
        return {"count": len(self.messages)}

    @route.get("/messages")
    async def list(self) -> list:
        return self.messages

app = FastAPI()
app.include_router(PinnedRouter(Thread), prefix="/threads")
```

Clients are plain HTTP: `POST /threads/{id}/messages` activates the instance for that
id on first request; later requests for the same id hit the same live instance.
Instances are evicted after an idle timeout (`PinnedRouter(Thread, idle_timeout=300)`),
running lifespan teardown. Handlers must be `async def`. `self.abort()` cancels
in-flight handlers and open streams, tears the instance down, and lets the next
request reconstruct it.
