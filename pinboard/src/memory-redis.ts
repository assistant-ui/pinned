import type { RedisKV, SetOptions } from "./redis.js";

interface StringEntry {
  kind: "string";
  value: string;
  expiresAt: number | null;
}

interface SetEntry {
  kind: "set";
  members: Set<string>;
  expiresAt: number | null;
}

type Entry = StringEntry | SetEntry;

export class MemoryRedis implements RedisKV {
  private readonly entries = new Map<string, Entry>();
  private readonly channels = new Map<string, Set<(message: string) => void>>();
  private readonly now: () => number;

  constructor(opts?: { now?: () => number }) {
    this.now = opts?.now ?? (() => Date.now());
  }

  private live(key: string): Entry | undefined {
    const entry = this.entries.get(key);
    if (entry === undefined) return undefined;
    if (entry.expiresAt !== null && entry.expiresAt <= this.now()) {
      this.entries.delete(key);
      return undefined;
    }
    return entry;
  }

  async get(key: string): Promise<string | null> {
    const entry = this.live(key);
    return entry?.kind === "string" ? entry.value : null;
  }

  async set(key: string, value: string, opts?: SetOptions): Promise<"OK" | null> {
    const existing = this.live(key);
    if (opts?.nx && existing !== undefined) return null;
    let expiresAt: number | null = null;
    if (opts?.pxMs !== undefined) expiresAt = this.now() + opts.pxMs;
    else if (opts?.keepTtl && existing !== undefined) expiresAt = existing.expiresAt;
    this.entries.set(key, { kind: "string", value, expiresAt });
    return "OK";
  }

  async del(key: string): Promise<number> {
    return this.live(key) !== undefined && this.entries.delete(key) ? 1 : 0;
  }

  async delIfEquals(key: string, value: string): Promise<number> {
    const entry = this.live(key);
    if (entry?.kind === "string" && entry.value === value) {
      this.entries.delete(key);
      return 1;
    }
    return 0;
  }

  async incr(key: string): Promise<number> {
    const entry = this.live(key);
    const current = entry?.kind === "string" ? Number(entry.value) : 0;
    const next = current + 1;
    this.entries.set(key, {
      kind: "string",
      value: String(next),
      expiresAt: entry?.expiresAt ?? null,
    });
    return next;
  }

  async pexpire(key: string, ms: number): Promise<number> {
    const entry = this.live(key);
    if (entry === undefined) return 0;
    entry.expiresAt = this.now() + ms;
    return 1;
  }

  async pttl(key: string): Promise<number> {
    const entry = this.live(key);
    if (entry === undefined) return -2;
    if (entry.expiresAt === null) return -1;
    return Math.max(0, entry.expiresAt - this.now());
  }

  private setEntry(key: string): SetEntry {
    const entry = this.live(key);
    if (entry?.kind === "set") return entry;
    const fresh: SetEntry = { kind: "set", members: new Set(), expiresAt: null };
    this.entries.set(key, fresh);
    return fresh;
  }

  async sadd(key: string, member: string): Promise<number> {
    const entry = this.setEntry(key);
    if (entry.members.has(member)) return 0;
    entry.members.add(member);
    return 1;
  }

  async srem(key: string, member: string): Promise<number> {
    const entry = this.live(key);
    if (entry?.kind !== "set") return 0;
    return entry.members.delete(member) ? 1 : 0;
  }

  async smembers(key: string): Promise<string[]> {
    const entry = this.live(key);
    return entry?.kind === "set" ? [...entry.members] : [];
  }

  async mget(keys: string[]): Promise<(string | null)[]> {
    return Promise.all(keys.map((key) => this.get(key)));
  }

  async publish(channel: string, message: string): Promise<number> {
    const handlers = this.channels.get(channel);
    if (handlers === undefined) return 0;
    for (const handler of handlers) {
      queueMicrotask(() => handler(message));
    }
    return handlers.size;
  }

  async subscribe(
    channel: string,
    handler: (message: string) => void,
  ): Promise<() => Promise<void>> {
    let handlers = this.channels.get(channel);
    if (handlers === undefined) {
      handlers = new Set();
      this.channels.set(channel, handlers);
    }
    handlers.add(handler);
    return async () => {
      handlers.delete(handler);
    };
  }

  async quit(): Promise<void> {
    this.entries.clear();
    this.channels.clear();
  }
}
