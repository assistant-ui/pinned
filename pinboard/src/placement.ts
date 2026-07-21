import type { RedisKV } from "./redis.js";
import {
  nsWorkersKey,
  parseWorkerRecord,
  placementKey,
  workerKey,
  type WorkerRecord,
} from "./keys.js";

export type Resolution =
  | { kind: "forward"; worker: WorkerRecord }
  | { kind: "blackout"; retryAfterMs: number }
  | { kind: "no-backends" }
  | { kind: "contended" };

export async function liveWorkers(
  redis: RedisKV,
  ns: string,
): Promise<WorkerRecord[]> {
  const epochs = await redis.smembers(nsWorkersKey(ns));
  if (epochs.length === 0) return [];
  const raws = await redis.mget(epochs.map((epoch) => workerKey(epoch)));
  const live: WorkerRecord[] = [];
  for (let i = 0; i < epochs.length; i++) {
    const raw = raws[i];
    const epoch = epochs[i]!;
    if (raw === null || raw === undefined) {
      await redis.srem(nsWorkersKey(ns), epoch);
      continue;
    }
    const record = parseWorkerRecord(raw);
    if (record === null || record.dead) continue;
    live.push(record);
  }
  return live;
}

export function pickPowerOfTwo(
  candidates: readonly WorkerRecord[],
  random: () => number = Math.random,
): WorkerRecord {
  if (candidates.length === 1) return candidates[0]!;
  const a = Math.floor(random() * candidates.length);
  let b = Math.floor(random() * (candidates.length - 1));
  if (b >= a) b += 1;
  const first = candidates[a]!;
  const second = candidates[b]!;
  return first.headroom >= second.headroom ? first : second;
}

export async function resolvePlacement(
  redis: RedisKV,
  ns: string,
  id: string,
  random: () => number = Math.random,
): Promise<Resolution> {
  const key = placementKey(ns, id);
  for (let attempt = 0; attempt < 5; attempt++) {
    const placedEpoch = await redis.get(key);
    if (placedEpoch !== null) {
      const raw = await redis.get(workerKey(placedEpoch));
      if (raw !== null) {
        const record = parseWorkerRecord(raw);
        if (record !== null && !record.dead) {
          return { kind: "forward", worker: record };
        }
        const pttl = await redis.pttl(workerKey(placedEpoch));
        return { kind: "blackout", retryAfterMs: Math.max(pttl, 1000) };
      }
      await redis.delIfEquals(key, placedEpoch);
      continue;
    }
    const candidates = await liveWorkers(redis, ns);
    if (candidates.length === 0) return { kind: "no-backends" };
    const chosen = pickPowerOfTwo(candidates, random);
    const claimed = await redis.set(key, String(chosen.epoch), { nx: true });
    if (claimed === "OK") return { kind: "forward", worker: chosen };
  }
  return { kind: "contended" };
}

export async function evictPlacement(
  redis: RedisKV,
  ns: string,
  id: string,
  epoch: number,
): Promise<boolean> {
  return (await redis.delIfEquals(placementKey(ns, id), String(epoch))) === 1;
}
