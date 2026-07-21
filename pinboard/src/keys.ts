import { createHash } from "node:crypto";

export const LEASE_TTL_MS = 15_000;

export interface WorkerRecord {
  epoch: number;
  url: string;
  namespaces: string[];
  headroom: number;
  dead?: boolean;
}

export function hashId(id: string): string {
  return createHash("sha256").update(id, "utf8").digest("hex");
}

export function placementKey(ns: string, id: string): string {
  return `place:${ns}:${hashId(id)}`;
}

export function workerKey(epoch: string | number): string {
  return `worker:${epoch}`;
}

export function nsWorkersKey(ns: string): string {
  return `ns:${ns}`;
}

export function pushChannel(epoch: string | number): string {
  return `push:${epoch}`;
}

export const NAMESPACES_KEY = "namespaces";
export const WORKERS_KEY = "workers";
export const EPOCH_KEY = "epoch";

export function parseWorkerRecord(raw: string): WorkerRecord | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) return null;
    const record = parsed as WorkerRecord;
    if (typeof record.epoch !== "number" || typeof record.url !== "string") {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}
