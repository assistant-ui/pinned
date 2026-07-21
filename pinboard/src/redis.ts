import { Redis } from "ioredis";

export interface SetOptions {
  pxMs?: number;
  nx?: boolean;
  keepTtl?: boolean;
}

export interface RedisKV {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: SetOptions): Promise<"OK" | null>;
  del(key: string): Promise<number>;
  delIfEquals(key: string, value: string): Promise<number>;
  incr(key: string): Promise<number>;
  pexpire(key: string, ms: number): Promise<number>;
  pttl(key: string): Promise<number>;
  sadd(key: string, member: string): Promise<number>;
  srem(key: string, member: string): Promise<number>;
  smembers(key: string): Promise<string[]>;
  mget(keys: string[]): Promise<(string | null)[]>;
  publish(channel: string, message: string): Promise<number>;
  subscribe(
    channel: string,
    handler: (message: string) => void,
  ): Promise<() => Promise<void>>;
  quit(): Promise<void>;
}

const DEL_IF_EQUALS_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export class IoredisKV implements RedisKV {
  private readonly client: Redis;
  private readonly subscribers: Redis[] = [];

  constructor(url: string) {
    this.client = new Redis(url);
    this.client.defineCommand("delIfEquals", {
      numberOfKeys: 1,
      lua: DEL_IF_EQUALS_LUA,
    });
  }

  get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  async set(key: string, value: string, opts?: SetOptions): Promise<"OK" | null> {
    const args: (string | number)[] = [];
    if (opts?.pxMs !== undefined) args.push("PX", opts.pxMs);
    if (opts?.keepTtl) args.push("KEEPTTL");
    if (opts?.nx) args.push("NX");
    const result = await this.client.call("SET", key, value, ...args);
    return result === "OK" ? "OK" : null;
  }

  del(key: string): Promise<number> {
    return this.client.del(key);
  }

  async delIfEquals(key: string, value: string): Promise<number> {
    const client = this.client as Redis & {
      delIfEquals(key: string, value: string): Promise<number>;
    };
    return client.delIfEquals(key, value);
  }

  incr(key: string): Promise<number> {
    return this.client.incr(key);
  }

  pexpire(key: string, ms: number): Promise<number> {
    return this.client.pexpire(key, ms);
  }

  pttl(key: string): Promise<number> {
    return this.client.pttl(key);
  }

  sadd(key: string, member: string): Promise<number> {
    return this.client.sadd(key, member);
  }

  srem(key: string, member: string): Promise<number> {
    return this.client.srem(key, member);
  }

  smembers(key: string): Promise<string[]> {
    return this.client.smembers(key);
  }

  mget(keys: string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return Promise.resolve([]);
    return this.client.mget(...keys);
  }

  publish(channel: string, message: string): Promise<number> {
    return this.client.publish(channel, message);
  }

  async subscribe(
    channel: string,
    handler: (message: string) => void,
  ): Promise<() => Promise<void>> {
    const sub = this.client.duplicate();
    this.subscribers.push(sub);
    sub.on("message", (ch: string, message: string) => {
      if (ch === channel) handler(message);
    });
    await sub.subscribe(channel);
    return async () => {
      const idx = this.subscribers.indexOf(sub);
      if (idx !== -1) this.subscribers.splice(idx, 1);
      await sub.quit().catch(() => {});
    };
  }

  async quit(): Promise<void> {
    await Promise.all(this.subscribers.map((s) => s.quit().catch(() => {})));
    await this.client.quit().catch(() => {});
  }
}
