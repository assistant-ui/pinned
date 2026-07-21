import type { WebSocket } from "ws";
import type { RedisKV } from "./redis.js";
import { normalizeNamespace } from "./router.js";
import {
  EPOCH_KEY,
  LEASE_TTL_MS,
  NAMESPACES_KEY,
  WORKERS_KEY,
  nsWorkersKey,
  parseWorkerRecord,
  pushChannel,
  workerKey,
  type WorkerRecord,
} from "./keys.js";

interface RegisterMessage {
  v: 1;
  type: "register";
  namespaces: string[];
  advertise: string;
  headroom: number;
}

function parseMessage(data: unknown): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(String(data));
    if (typeof parsed !== "object" || parsed === null) return null;
    const msg = parsed as Record<string, unknown>;
    if (msg["v"] !== 1 || typeof msg["type"] !== "string") return null;
    return msg;
  } catch {
    return null;
  }
}

function parseRegister(msg: Record<string, unknown>): RegisterMessage | null {
  if (msg["type"] !== "register") return null;
  const namespaces = msg["namespaces"];
  const advertise = msg["advertise"];
  const headroom = msg["headroom"];
  if (
    !Array.isArray(namespaces) ||
    namespaces.length === 0 ||
    !namespaces.every((ns) => typeof ns === "string") ||
    typeof advertise !== "string" ||
    typeof headroom !== "number" ||
    !Number.isFinite(headroom)
  ) {
    return null;
  }
  return { v: 1, type: "register", namespaces, advertise, headroom };
}

export class Registry {
  constructor(
    private readonly redis: RedisKV,
    private readonly leaseTtlMs: number = LEASE_TTL_MS,
  ) {}

  handleConnection(ws: WebSocket): void {
    let record: WorkerRecord | null = null;
    let unsubscribe: (() => Promise<void>) | null = null;

    const send = (msg: Record<string, unknown>): void => {
      if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ v: 1, ...msg }));
    };

    const fail = (message: string): void => {
      send({ type: "error", message });
      ws.close(4400, message);
    };

    ws.on("message", (data) => {
      void (async () => {
        const msg = parseMessage(data);
        if (msg === null) return fail("malformed message");

        if (record === null) {
          const register = parseRegister(msg);
          if (register === null) return fail("first message must be register");
          let namespaces: string[];
          try {
            namespaces = register.namespaces.map(normalizeNamespace);
          } catch (err) {
            return fail(err instanceof Error ? err.message : "invalid namespace");
          }
          const epoch = await this.redis.incr(EPOCH_KEY);
          record = {
            epoch,
            url: register.advertise.replace(/\/+$/, ""),
            namespaces,
            headroom: register.headroom,
          };
          await this.redis.set(workerKey(epoch), JSON.stringify(record), {
            pxMs: this.leaseTtlMs,
          });
          await this.redis.sadd(WORKERS_KEY, String(epoch));
          for (const ns of namespaces) {
            await this.redis.sadd(NAMESPACES_KEY, ns);
            await this.redis.sadd(nsWorkersKey(ns), String(epoch));
          }
          unsubscribe = await this.redis.subscribe(pushChannel(epoch), (raw) => {
            if (ws.readyState === ws.OPEN) ws.send(raw);
          });
          send({ type: "registered", epoch, leaseTtlMs: this.leaseTtlMs });
          return;
        }

        if (msg["type"] === "renew") {
          const refreshed = await this.redis.pexpire(
            workerKey(record.epoch),
            this.leaseTtlMs,
          );
          if (refreshed === 0) {
            send({ type: "expired" });
            ws.close(4408, "lease expired");
            return;
          }
          send({ type: "renewed", ttlMs: this.leaseTtlMs });
          return;
        }

        if (msg["type"] === "headroom") {
          const headroom = msg["headroom"];
          if (typeof headroom !== "number" || !Number.isFinite(headroom)) {
            return fail("headroom must be a number");
          }
          record.headroom = headroom;
          const pttl = await this.redis.pttl(workerKey(record.epoch));
          if (pttl <= 0) {
            send({ type: "expired" });
            ws.close(4408, "lease expired");
            return;
          }
          await this.redis.set(workerKey(record.epoch), JSON.stringify(record), {
            pxMs: pttl,
          });
          return;
        }

        fail(`unknown message type: ${String(msg["type"])}`);
      })();
    });

    ws.on("close", () => {
      void this.markDisconnected(record, unsubscribe);
    });
    ws.on("error", () => {
      ws.terminate();
    });
  }

  private async markDisconnected(
    record: WorkerRecord | null,
    unsubscribe: (() => Promise<void>) | null,
  ): Promise<void> {
    if (unsubscribe !== null) await unsubscribe();
    if (record === null) return;
    await this.redis.srem(WORKERS_KEY, String(record.epoch));
    for (const ns of record.namespaces) {
      await this.redis.srem(nsWorkersKey(ns), String(record.epoch));
    }
    const key = workerKey(record.epoch);
    const pttl = await this.redis.pttl(key);
    if (pttl <= 0) return;
    // The dead record must keep its remaining lease TTL: its lifetime IS the
    // re-place blackout window, so it is never extended and never made immortal.
    await this.redis.set(key, JSON.stringify({ ...record, dead: true }), {
      pxMs: pttl,
    });
  }

  async listWorkers(): Promise<WorkerRecord[]> {
    const epochs = await this.redis.smembers(WORKERS_KEY);
    if (epochs.length === 0) return [];
    const raws = await this.redis.mget(epochs.map((epoch) => workerKey(epoch)));
    const workers: WorkerRecord[] = [];
    for (let i = 0; i < epochs.length; i++) {
      const raw = raws[i];
      if (raw === null || raw === undefined) {
        await this.redis.srem(WORKERS_KEY, epochs[i]!);
        continue;
      }
      const record = parseWorkerRecord(raw);
      if (record !== null) workers.push(record);
    }
    return workers;
  }

  async push(epoch: number, message: Record<string, unknown>): Promise<number> {
    return this.redis.publish(
      pushChannel(epoch),
      JSON.stringify({ v: 1, ...message }),
    );
  }
}
