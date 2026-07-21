import { test } from "node:test";
import assert from "node:assert/strict";
import { MemoryRedis } from "../src/memory-redis.js";
import { evictPlacement, resolvePlacement } from "../src/placement.js";
import {
  nsWorkersKey,
  placementKey,
  workerKey,
  type WorkerRecord,
} from "../src/keys.js";

const NS = "/threads";

async function addWorker(
  redis: MemoryRedis,
  epoch: number,
  headroom: number,
  opts?: { dead?: boolean; ttlMs?: number },
): Promise<WorkerRecord> {
  const record: WorkerRecord = {
    epoch,
    url: `http://backend-${epoch}/threads`,
    namespaces: [NS],
    headroom,
    ...(opts?.dead ? { dead: true } : {}),
  };
  await redis.set(workerKey(epoch), JSON.stringify(record), {
    pxMs: opts?.ttlMs ?? 15_000,
  });
  await redis.sadd(nsWorkersKey(NS), String(epoch));
  return record;
}

test("fast path forwards to the placed live worker", async () => {
  const redis = new MemoryRedis();
  await addWorker(redis, 1, 100);
  await addWorker(redis, 2, 900);
  await redis.set(placementKey(NS, "t1"), "1");
  const resolution = await resolvePlacement(redis, NS, "t1");
  assert.equal(resolution.kind, "forward");
  assert.equal(resolution.kind === "forward" && resolution.worker.epoch, 1);
});

test("two concurrent placements pick exactly one winner", async () => {
  const redis = new MemoryRedis();
  await addWorker(redis, 1, 100);
  await addWorker(redis, 2, 900);
  const results = await Promise.all([
    resolvePlacement(redis, NS, "t1"),
    resolvePlacement(redis, NS, "t1"),
  ]);
  for (const r of results) assert.equal(r.kind, "forward");
  const epochs = results.map((r) => (r.kind === "forward" ? r.worker.epoch : -1));
  assert.equal(epochs[0], epochs[1]);
  const placed = await redis.get(placementKey(NS, "t1"));
  assert.equal(placed, String(epochs[0]));
});

test("power-of-two-choices prefers headroom", async () => {
  const redis = new MemoryRedis();
  await addWorker(redis, 1, 100);
  await addWorker(redis, 2, 900);
  const resolution = await resolvePlacement(redis, NS, "t1", () => 0);
  assert.equal(resolution.kind === "forward" && resolution.worker.epoch, 2);
});

test("dead worker inside the blackout window yields 503, not a re-place", async () => {
  let clock = 1_000_000;
  const redis = new MemoryRedis({ now: () => clock });
  await addWorker(redis, 1, 100, { dead: true, ttlMs: 9_000 });
  await addWorker(redis, 2, 900);
  await redis.set(placementKey(NS, "t1"), "1");

  const resolution = await resolvePlacement(redis, NS, "t1");
  assert.equal(resolution.kind, "blackout");
  assert.ok(resolution.kind === "blackout" && resolution.retryAfterMs <= 9_000);
  assert.equal(await redis.get(placementKey(NS, "t1")), "1");

  clock += 10_000;
  const after = await resolvePlacement(redis, NS, "t1");
  assert.equal(after.kind === "forward" && after.worker.epoch, 2);
  assert.equal(await redis.get(placementKey(NS, "t1")), "2");
});

test("dead workers are never placement candidates", async () => {
  const redis = new MemoryRedis();
  await addWorker(redis, 1, 900, { dead: true });
  await addWorker(redis, 2, 100);
  for (let i = 0; i < 10; i++) {
    const resolution = await resolvePlacement(redis, NS, `t${i}`);
    assert.equal(resolution.kind === "forward" && resolution.worker.epoch, 2);
  }
});

test("no live workers yields no-backends", async () => {
  const redis = new MemoryRedis();
  const resolution = await resolvePlacement(redis, NS, "t1");
  assert.equal(resolution.kind, "no-backends");
});

test("epoch-guarded delete only removes a matching placement", async () => {
  const redis = new MemoryRedis();
  await redis.set(placementKey(NS, "t1"), "7");
  assert.equal(await evictPlacement(redis, NS, "t1", 8), false);
  assert.equal(await redis.get(placementKey(NS, "t1")), "7");
  assert.equal(await evictPlacement(redis, NS, "t1", 7), true);
  assert.equal(await redis.get(placementKey(NS, "t1")), null);
});

test("stale placement past the window is evicted and re-placed", async () => {
  const redis = new MemoryRedis();
  await redis.set(placementKey(NS, "t1"), "99");
  await addWorker(redis, 2, 900);
  const resolution = await resolvePlacement(redis, NS, "t1");
  assert.equal(resolution.kind === "forward" && resolution.worker.epoch, 2);
  assert.equal(await redis.get(placementKey(NS, "t1")), "2");
});
