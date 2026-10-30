import { test } from "vitest";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { WebSocket } from "ws";
import { MemoryRedis } from "../src/memory-redis.js";
import { createPinboard, type Pinboard } from "../src/server.js";
import { resolvePlacement } from "../src/placement.js";
import { workerKey } from "../src/keys.js";

const TOKEN = "secret-token";
const NS = "/threads";

interface Harness {
  redis: MemoryRedis;
  pinboard: Pinboard;
  port: number;
  clock: { now: number };
  close(): Promise<void>;
}

async function startHarness(): Promise<Harness> {
  const clock = { now: 1_000_000 };
  const redis = new MemoryRedis({ now: () => clock.now });
  const pinboard = createPinboard({ redis, token: TOKEN });
  pinboard.server.listen(0);
  await once(pinboard.server, "listening");
  const address = pinboard.server.address();
  assert.ok(address !== null && typeof address === "object");
  return {
    redis,
    pinboard,
    port: address.port,
    clock,
    close: () => pinboard.close(),
  };
}

class WsClient {
  readonly ws: WebSocket;
  private readonly queue: Record<string, unknown>[] = [];
  private waiter: (() => void) | null = null;

  constructor(port: number, token: string | null) {
    this.ws = new WebSocket(`ws://127.0.0.1:${port}/registry/ws`, {
      headers: token === null ? {} : { authorization: `Bearer ${token}` },
    });
    this.ws.on("message", (data) => {
      this.queue.push(JSON.parse(String(data)) as Record<string, unknown>);
      this.waiter?.();
    });
  }

  async next(): Promise<Record<string, unknown>> {
    while (this.queue.length === 0) {
      await new Promise<void>((resolve) => {
        this.waiter = resolve;
        this.ws.once("close", () => resolve());
      });
      this.waiter = null;
      if (this.queue.length === 0 && this.ws.readyState === this.ws.CLOSED) {
        throw new Error("ws closed without message");
      }
    }
    return this.queue.shift()!;
  }

  send(msg: Record<string, unknown>): void {
    this.ws.send(JSON.stringify(msg));
  }

  async register(headroom = 500, advertise = "http://127.0.0.1:1/threads") {
    await once(this.ws, "open");
    this.send({
      v: 1,
      type: "register",
      namespaces: [NS],
      advertise,
      headroom,
    });
    const msg = await this.next();
    assert.equal(msg["type"], "registered");
    return msg["epoch"] as number;
  }
}

test("rejects a registration WS without a matching token", async () => {
  const h = await startHarness();
  try {
    for (const token of [null, "wrong-token"]) {
      const ws = new WebSocket(`ws://127.0.0.1:${h.port}/registry/ws`, {
        headers: token === null ? {} : { authorization: `Bearer ${token}` },
      });
      const [err] = (await once(ws, "error")) as [Error];
      assert.match(err.message, /401/);
    }
  } finally {
    await h.close();
  }
});

test("registration creates a worker record and namespace entries", async () => {
  const h = await startHarness();
  try {
    const client = new WsClient(h.port, TOKEN);
    const epoch = await client.register(700, "http://10.0.0.1:8000/threads/");
    assert.equal(typeof epoch, "number");
    const raw = await h.redis.get(workerKey(epoch));
    assert.ok(raw !== null);
    const record = JSON.parse(raw) as Record<string, unknown>;
    assert.equal(record["url"], "http://10.0.0.1:8000/threads");
    assert.deepEqual(record["namespaces"], [NS]);
    assert.equal(record["headroom"], 700);
    assert.deepEqual(await h.redis.smembers("namespaces"), [NS]);
    client.ws.close();
  } finally {
    await h.close();
  }
});

test("renewals keep the worker alive past the base TTL", async () => {
  const h = await startHarness();
  try {
    const client = new WsClient(h.port, TOKEN);
    const epoch = await client.register();

    for (let i = 0; i < 4; i++) {
      h.clock.now += 6_000;
      client.send({ v: 1, type: "renew" });
      const ack = await client.next();
      assert.equal(ack["type"], "renewed");
      assert.notEqual(await h.redis.get(workerKey(epoch)), null);
    }

    h.clock.now += 16_000;
    assert.equal(await h.redis.get(workerKey(epoch)), null);
    client.send({ v: 1, type: "renew" });
    const expired = await client.next();
    assert.equal(expired["type"], "expired");
  } finally {
    await h.close();
  }
});

test("disconnect blacks out placements, then frees them after the window", async () => {
  const h = await startHarness();
  try {
    const a = new WsClient(h.port, TOKEN);
    const epochA = await a.register(900);
    const placed = await resolvePlacement(h.redis, NS, "t1");
    assert.equal(placed.kind === "forward" && placed.worker.epoch, epochA);

    a.ws.close();
    await once(a.ws, "close");
    await new Promise((resolve) => setTimeout(resolve, 50));

    const during = await resolvePlacement(h.redis, NS, "t1");
    assert.equal(during.kind, "blackout");

    const b = new WsClient(h.port, TOKEN);
    const epochB = await b.register(500);
    assert.ok(epochB > epochA);

    for (let i = 0; i < 2; i++) {
      h.clock.now += 8_000;
      b.send({ v: 1, type: "renew" });
      assert.equal((await b.next())["type"], "renewed");
    }

    const after = await resolvePlacement(h.redis, NS, "t1");
    assert.equal(after.kind === "forward" && after.worker.epoch, epochB);
    b.ws.close();
  } finally {
    await h.close();
  }
});

test("headroom updates steer power-of-two placement", async () => {
  const h = await startHarness();
  try {
    const client = new WsClient(h.port, TOKEN);
    const epoch = await client.register(100);
    client.send({ v: 1, type: "headroom", headroom: 12_345 });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const raw = await h.redis.get(workerKey(epoch));
    assert.ok(raw !== null);
    assert.equal((JSON.parse(raw) as { headroom: number }).headroom, 12_345);
    client.ws.close();
  } finally {
    await h.close();
  }
});

test("push reaches the worker over its WS via pub/sub", async () => {
  const h = await startHarness();
  try {
    const client = new WsClient(h.port, TOKEN);
    const epoch = await client.register();
    await h.pinboard.registry.push(epoch, { type: "wind-down" });
    const msg = await client.next();
    assert.deepEqual(msg, { v: 1, type: "wind-down" });
    client.ws.close();
  } finally {
    await h.close();
  }
});

function startBackend(): Promise<{ server: Server; port: number; seen: string[] }> {
  const seen: string[] = [];
  const server = createServer((req, res) => {
    seen.push(`${req.method} ${req.url}`);
    res.writeHead(200, { "content-type": "text/plain" });
    res.end(`hello from backend: ${req.url}`);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      assert.ok(address !== null && typeof address === "object");
      resolve({ server, port: address.port, seen });
    });
  });
}

test("proxies a request to the placed backend, 404 on unknown namespace", async () => {
  const h = await startHarness();
  const backend = await startBackend();
  try {
    const client = new WsClient(h.port, TOKEN);
    await client.register(500, `http://127.0.0.1:${backend.port}/threads`);

    const miss = await fetch(`http://127.0.0.1:${h.port}/nope/t1`);
    assert.equal(miss.status, 404);

    const hit = await fetch(`http://127.0.0.1:${h.port}/threads/t1/stream?x=1`);
    assert.equal(hit.status, 200);
    assert.equal(await hit.text(), "hello from backend: /threads/t1/stream?x=1");
    assert.deepEqual(backend.seen, ["GET /threads/t1/stream?x=1"]);
    client.ws.close();
  } finally {
    backend.server.close();
    await h.close();
  }
});
