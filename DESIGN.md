# Pinned

One live instance of your class per id, cluster-wide, with an HTTP surface; the platform routes every request for that id to it. Inside, an instance is an ordinary single-threaded asyncio webserver: handlers interleave at awaits, state is plain attributes, no locks. Inspired by Cloudflare Durable Objects, but not an actor framework: no mailboxes or turn gates, no cross-instance calls (an instance reaches another one through the front door, as a regular HTTP client), no supervision trees. The product is the placement guarantee: all traffic for id X reaches the one object living for X.

## Control plane: pinboard (Directory + Placement)

**pinboard** is the control plane service: it receives HTTP requests and forwards them to the right backend based on the id in the URL.

- Backends register a **namespace** (one implementation per namespace). A namespace is a path prefix, not a bare word: pinboard routes by longest registered prefix, the next segment is the id, the remainder goes to the route decorators. Multiple apps therefore share one pinboard by owning different prefixes (`a/threads`, `b/threads`), and one backend process can host several apps by mounting several `PinnedRouter`s, each registering its own namespace over the same WS (leases and headroom stay per worker). Apps live on paths, not subdomains. A backend is considered connected and open for requests while it holds an open WS connection to the control plane service.
- **Namespace config (decided): dynamic.** Namespaces are learned from registrations; any holder of the deployment token can claim a prefix, so projects sharing a pinboard are trusted siblings. Static namespace allowlists and per-namespace tokens (project isolation inside one pinboard) are a later concern.
- Placement is memory-aware: backends report memory headroom over the registration WS and the CP prefers backends with room when placing new ids. Memory-pressure force-eviction on the backend (shedding activations under pressure, not just idle timeout) is a later concern.
- The WS is registration/liveness only, not a data channel: the control plane forwards requests via plain HTTP to the backend's advertised address.
- **The CP is a proxy (decided)**: all traffic, including minutes-long SSE bodies, flows through it (keeps backends private, one TLS/auth story; redirect stays a possible later optimization since EventSource follows 307). That makes proxy robustness a first-class requirement:
  - Unbuffered pass-through: pipe bytes as they arrive, flush per chunk, strip hop-by-hop headers only, forward the rest untouched; send anti-buffering headers downstream.
  - Backpressure end-to-end: respect client-side write backpressure by pausing the upstream read; bound per-connection buffering and disconnect laggards instead of queueing unboundedly (the athena-fork bug).
  - Disconnect propagation: when the client goes away, abort the upstream request immediately so the backend frees the stream; when the backend dies mid-stream, end the client response cleanly (no hangs).
  - Timeouts that understand SSE: connect + response-header timeouts to the backend, but no idle timeout on established stream bodies (heartbeat comments distinguish alive-idle from dead).
  - Fault isolation: one connection's error never takes the process down; map backend failures to clean 502/504; never auto-retry non-idempotent requests.
  - Load shedding: cap concurrent connections and memory, reject over capacity rather than degrade everyone.
  - Graceful deploys: SIGTERM drains (stop accepting, grace period for streams, then cut); dropped streams are survivable because clients snapshot-reconnect.
  - Keep-alive connection pooling to backends; connection counts and per-backend health as core metrics.
- Redis as storage; nodes can autoscale freely.
- **Registration channel (decided): WS**, for liveness + push only. The ~3s lease renewal rides it; it buys ms-level disconnect detection on clean process death (FIN/RST vs waiting out a missed-renewal boundary) and the CP->backend push channel that the wind-down ping (which outlives any SIGTERM grace) and operator-initiated eviction require. Backend->CP RPCs (release id, headroom updates) stay plain HTTP. Auth: a deployment token presented as a Bearer header on the WS upgrade (one shared token per pinboard deployment for MVP; per-namespace tokens arrive with static namespace config). Registration auth is the SSRF boundary: the CP forwards traffic to whatever URL was registered. (A k8s pod-discovery mode, the iris approach, could join later as an alternative registration source.)
- **Implementation (decided)**: ships as `pinboard`; plain Node, no framework. `node:http` + undici + `ws` + a Redis client; the surface is one catch-all proxy route, the registration WS, and introspection. Not bun.

## Data plane

Python library that binds the class to a subpath on any FastAPI/Starlette app, registers with the control plane, and manages instance lifecycles: activate on first request, evict after idle timeout (lifespan teardown runs).

## API sketch (prototype)

```python
from pinned import PinnedAPI, PinnedRouter, route

class Thread(PinnedAPI):
    # self.id is the key the control plane routed on

    async def lifespan(self):
        # activation: rebuild from the framework's own persistence
        self.graph = build_graph()
        yield
        # deactivation (idle eviction or shutdown)
        await self.graph.aclose()

    # HTTP surface of one instance. `route` is our own decorator, signature-identical
    # to FastAPI's (kwargs forwarded); unmatched requests fall back to an optional
    # `fetch(request)` method. The SSE + commands protocol is a prebuilt subclass
    # on top, not part of the core.
    @route.post("/commands")
    async def send(self, body: Command) -> Ack:      # ack with the run's seq, never a stream
        ...

    @route.get("/stream")
    async def stream(self) -> EventSourceResponse:   # SSE: state snapshot, then live tail
        ...
```

Hosting (ride-along on any FastAPI/Starlette app, no YAML):

```python
app = FastAPI()
app.include_router(
    PinnedRouter(
        Thread,
        control_plane="wss://cp.internal",           # omit -> local mode, in-memory directory
        advertise="http://10.0.3.7:8000/threads",    # must uniquely address THIS process; CP appends /{id}/...
    ),
    prefix="/threads",
)
```

Clients are plain HTTP, no SDK: any service talks to the control plane (or, in local dev, the app directly) at `.../threads/{id}/...`.

## Decided semantics

- **Concurrency = plain asyncio.** One instance per id on one event loop; handlers must be `async def` (sync handlers unsupported: FastAPI's threadpool offload would break single-threaded atomicity). No implicit turn gate; deliberate divergence from DO's input gates, since our turns are minutes-long LLM runs (gating would block reads for minutes and invite reentrancy deadlocks). The one serialization that must exist, one active run per instance with an arrival policy (reject / enqueue / interrupt: the double-texting knob), lives in the prebuilt Thread subclass, mirroring LangGraph's queue-layer enforcement.
- **Ids**: client-generated (first request activates, no create step), in the URL segment (`/threads/{id}/...`; EventSource can't set headers), validated, names not capabilities. Internally hashed to fixed-length directory keys so arbitrarily long ids are fine (Redis-level concern).
- **Routing surface**: `PinnedRouter` is an APIRouter subclass; it carries `/{id}`, dispatches to the instance for the routed key, and bakes in the host lifespan (registration WS, eviction sweep); `include_router` merges router lifespans on current FastAPI; a `.lifespan` attribute stays exposed for plain Starlette. Route decorators see the path remainder after the bound subpath. Assign it to a variable when you need the handle (client, introspection).
- **Idle cleanup**: evict after idle timeout (sweep loop + passivation handshake so in-flight requests never race the eviction); the backend releases the id->backend directory entry via a ping to the control plane; open streams count as activity. No polite self-deactivation; the one escape hatch is `self.abort()`, Python-only, no HTTP surface (cancel in-flight work, best-effort teardown, release lease, reconstruct on next request), which doubles as the runtime's force-end primitive.
- **Streaming**: canonical `GET /stream` subscription; `POST /commands` returns an ack with the enqueued seq, never a second stream system (POST+Accept:event-stream sugar can proxy the log for AssistantTransport interop). Per-connection queues are bounded and laggards get disconnected (the unbounded-queue bug in athena's fork); eviction closes streams via lifespan teardown.
- **Reconnect (MVP)**: no SSE resume, no replay buffer; every (re)connect restarts from scratch (state snapshot, then live tail). Later: per-instance ring buffer of the seq log with cursor resume.
- **Auth**: transport trust only. User authn/authz stays app code (control plane sits behind the app's edge, forwards headers untouched; ownership checks are FastAPI dependencies on routes). Control plane -> backend via per-registration secret; backend -> control plane WS via deployment token (never the end-user credential).
- **Advertise = one process (by design)**: the registering unit is a single worker process, and `advertise` must be uniquely routable to it. `uvicorn --workers N` behind one port breaks the singleton guarantee; run one worker per container/port, or give each worker its own advertised port. Local mode likewise assumes a single process.
- **Local dev**: same code with no control plane; single process, in-memory directory. Works because the control plane and the underlying servers speak the same external API (`/threads/{id}/...`): clients can't tell whether they're talking to the control plane or a backend directly.
- **DO features we drop**: storage/alarms (durability lives in the framework, e.g. LangGraph's checkpointer), cross-instance stubs/RPC (go through the front door), input/output gates ("reply only after checkpoint" is app code), WS hibernation.
- **Observability (second step, post-MVP)**: OTel hooks only (metrics by type: run queue depth, activation counts by eviction reason, invariant counters like double-placement; traceparent forwarded) + a control-plane introspection endpoint (per-type counts, per-backend distribution, lookup id -> backend/state). No observability system of our own.
- **Deferred**: `@route.websocket` (needs control-plane handling of long-lived tunneled connections; SSE + POST covers v1).
- **Lease + self-fencing (10/13/15)**: the worker renews its lease over the registration WS every ~3s. At 10s unrenewed it begins graceful shutdown of all instances (`.abort()` on each; an aborting run may still legally persist its own cancellation, e.g. a final aborted-run checkpoint). At 13s `self.has_lease = False`: the hard line the checkpointer wrapper checks before every write. 2s buffer, then at 15s the CP re-places the ids elsewhere; a backend whose WS later reconnects registers under a fresh epoch (it already self-fenced, and its stale placements are gone). Both timers run on monotonic clocks (worker from renewal receipt, CP from renewal grant), so the worker's deadlines provably fire first; no wall-clock skew dependence. Numbers sit in the Chubby / k8s-leader-election range and are configurable, since the TTL is also the crash-recovery blackout. Streams learn about abort via plain asyncio cancellation of handler tasks (finally blocks run; the prebuilt Thread subclass emits the terminal gorp frame from finally). Known edge cases, accepted: GC pauses stretching past a deadline, teardown overrunning its grace; the 13s hard line plus the checkpointer wrapper bounds the damage either way.
- **Fencing scope (honest claim)**: at most one live instance per id except during a failure window bounded by the lease timeline. The framework's own checkpointer can't be forced to obey our lease, so hard single-writer needs a write-time check: for LangGraph we ship a `FencedSaver(inner)` wrapper that checks `self.has_lease` locally before every put/put_writes/delete (0 ms, no per-thread plumbing; raising aborts the zombie's run) and fails safe (starved renewal denies writes rather than duplicating execution). Still soft fencing (at most one in-flight superstep can land in the check-to-write gap); atomic fencing needs a Postgres-specific saver subclass, or the managed-SQLite store where we get it for free.
- **Configuration coupling**: dial-out (backend registers with the CP: NAT-friendly, enables cold-start); `control_plane`/`advertise` sourced from env (`CONTROL_PLANE_URL`, `ADVERTISE_URL`) with kwargs as overrides, so app code stays topology-free.
- **Upgrades / namespace takeover**: very long drain grace period. The outgoing deployment's instances get a userland ping ("wind down"); userland calls `self.abort()` when convenient; freed ids activate on the new deployment, so the cutover completes at userland's pace.

## Future

- Environments as a first-class dimension next to the namespace: registration carries `{env, namespace, ...}` with env-scoped deployment tokens, directory keys become `place:{env}:{ns}:{key}` (env-scoped introspection and bulk teardown for free), request env resolved via wildcard hostname mapping with a path-prefix fallback, defaulting to `"default"` so single-env deployments never see it. The payoff is the preview fleet: a PR preview becomes "register with env=pr-123 against the shared pinboard" instead of deploying a full stack per PR. Prod and staging still run separate pinboards for blast radius.
- Static namespace allowlist + per-namespace tokens: project isolation inside one shared pinboard (a project's token can only register prefixes it owns).
- Scale-to-zero: namespace config carries a stable cold-start URL; with zero connected backends, pinboard forwards there anyway (Cloud Run/Fly cold-start for free) or fires a wake webhook and returns 503 + Retry-After while it boots (Knative-activator pattern, with pinboard already being the proxy). On k8s this needs an external scaler (KEDA on a pending-requests metric); MVP posture is minReplicas 1.
- Managed per-instance SQLite, persisted to R2: pinned provides each instance a local SQLite DB, restored on activation and streamed/snapshotted to object storage (Litestream/LiteFS pattern; Cloudflare DO's own SQLite-backed storage is the model). Natural fit: one writer per id is exactly SQLite's constraint, and owning the store lets us enforce lease fencing at the storage layer, closing the zombie-write gap that an app-owned checkpointer leaves open.
