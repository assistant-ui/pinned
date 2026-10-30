# pinboard

The pinned control plane: a stateless Node service that owns the directory (which backend holds which id) and proxies all data-plane traffic to the right backend. Any number of pinboard nodes can run behind one LB URL; all coordination goes through Redis.

Plain Node, no framework: `node:http` for the server, `undici` for upstream requests, `ws` for the registration WebSocket, `ioredis` for Redis. ioredis was chosen over `node-redis` for its first-class Lua scripting (`defineCommand` backs the epoch-guarded delete), battle-tested auto-reconnect, and simple duplicated connections for pub/sub.

## Running

Needs Node.js 26.13.0 (pinned in `.node-version`).

```sh
npm install
npm run build
PINBOARD_TOKEN=... REDIS_URL=redis://localhost:6379 npm start
```

| Env var | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8080` | HTTP listen port |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL; the special value `memory:` uses an in-process store (single node only, dev and smoke tests) |
| `PINBOARD_TOKEN` | required | Deployment token; workers must present it as `Authorization: Bearer <token>` on the registration WS (and on `/registry/*` introspection calls) |

Tests need no Redis server: `npm test` runs Vitest against the in-memory store, and `npm run test:coverage` adds a v8 coverage report checked against the thresholds in `vitest.config.ts`. The Python integration test (`tests/test_pinboard_integration.py`) starts the built `dist/src/main.js`, so run `npm run build` first or it skips.

## Request routing and placement

Every non-`/registry` request goes through the catch-all proxy:

1. Match the path against the registered namespaces (longest prefix wins, on segment boundaries). Unknown namespace: 404.
2. The next path segment is the id. The placement key is `place:{ns}:{sha256(id)}` (hashing keeps arbitrarily long ids Redis-safe).
3. `GET place:{ns}:{key}`:
   - Points at a live worker: forward `advertise + /{id} + remainder` and stream the response through (SSE-safe: no body idle timeout, per-chunk flush, hop-by-hop headers stripped, client disconnect aborts the upstream request).
   - Points at a worker whose WS dropped but whose lease has not yet expired (the blackout window, at most 15s from its last renewal): 503 with `Retry-After`. Never double-place inside the window; the dead worker self-fences on its own monotonic timeline before the window ends.
   - Points at an epoch with no worker record (lease expired): the entry is stale. Delete it with an epoch-guarded delete (Lua: DEL only if the value still matches) and fall through to fresh placement.
4. Fresh placement: among the live, non-dead workers registered for the namespace, pick two at random and take the one with more reported memory headroom (power of two choices), then claim with `SET place:{ns}:{key} <epoch> NX`. The atomic SET NX is the singleton guarantee: racing pinboard nodes are safe, the loser re-reads and forwards to the winner's choice. Placement is sticky; there is no rebalancing.
5. No live workers for the namespace: 503.

Worker state in Redis (shared by all pinboard nodes):

- `epoch`: INCR counter; every WS connection gets a fresh monotonic epoch.
- `worker:{epoch}`: JSON worker record `{epoch, url, namespaces, headroom, dead?}` with a 15s TTL refreshed on each lease renewal. WS disconnect rewrites it with `dead: true` while preserving the remaining TTL: the record's remaining lifetime is exactly the blackout window.
- `namespaces`: set of all registered namespace prefixes (namespaces are dynamic, learned from registrations).
- `ns:{ns}`: set of worker epochs serving that namespace (pruned on disconnect and lazily when records expire).
- `workers`: set of all worker epochs, for introspection.
- `push:{epoch}`: pub/sub channel; lets any pinboard node push to a worker whose WS terminates on another node.

## Registration WebSocket

Workers dial out to `GET /registry/ws` with `Authorization: Bearer <PINBOARD_TOKEN>`; a missing or non-matching token is rejected with 401 before the upgrade. All messages are JSON with a `v: 1` field.

Worker to pinboard:

```jsonc
{ "v": 1, "type": "register", "namespaces": ["/threads"], "advertise": "http://10.0.3.7:8000/threads", "headroom": 512 }
{ "v": 1, "type": "renew" }
{ "v": 1, "type": "headroom", "headroom": 384 }
```

- `register` must be the first message. `namespaces` is one or more path prefixes (a leading slash is added, trailing slashes stripped); one worker process can serve several namespaces over the same WS. `advertise` must uniquely address this worker process; pinboard appends `/{id}/...`. `headroom` is the worker's reported memory headroom (relative units, higher means more room).
- `renew` should be sent roughly every 3s; each grant re-arms the 15s lease TTL.
- `headroom` updates the reported headroom without touching the lease.

Pinboard to worker:

```jsonc
{ "v": 1, "type": "registered", "epoch": 42, "leaseTtlMs": 15000 }
{ "v": 1, "type": "renewed", "ttlMs": 15000 }
{ "v": 1, "type": "expired" }
{ "v": 1, "type": "error", "message": "..." }
{ "v": 1, "type": "wind-down" }
{ "v": 1, "type": "evict", "ns": "/threads", "id": "t1" }
```

- `registered` acks registration with the connection's fresh epoch.
- `renewed` acks a lease grant; the worker should base its self-fencing timers on receipt of this message (monotonic clocks on both sides, no wall-clock skew dependence).
- `expired` is sent (followed by a close) when a renewal arrives after the lease already lapsed; the worker has already self-fenced and must reconnect to register under a fresh epoch.
- `wind-down` and `evict` are operator-initiated pushes (see below); the data channel stays plain HTTP, the WS is registration, liveness, and push only.

Liveness: a WS disconnect (FIN or RST) marks the worker dead immediately (no new placements, existing placements 503 through the blackout); missed renewals past the 15s TTL mark it dead too, which is what a network partition falls back on. After the window, placements to that epoch are lazily evicted and re-placed on the next request.

## Introspection

Bearer-token protected:

- `GET /registry/workers`: live worker records.
- `POST /registry/push` with `{"epoch": 42, "message": {"type": "wind-down"}}`: push a message to that worker's WS from any pinboard node (delivered via Redis pub/sub).
- `GET /healthz`: liveness probe, no auth.
