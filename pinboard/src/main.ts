import process from "node:process";
import { createPinboard } from "./server.js";
import { IoredisKV, type RedisKV } from "./redis.js";
import { MemoryRedis } from "./memory-redis.js";

function createRedis(url: string): RedisKV {
  if (url === "memory:") return new MemoryRedis();
  return new IoredisKV(url);
}

function main(): void {
  const port = Number(process.env["PORT"] ?? 8080);
  const redisUrl = process.env["REDIS_URL"] ?? "redis://localhost:6379";
  const token = process.env["PINBOARD_TOKEN"];
  if (token === undefined || token === "") {
    console.error("PINBOARD_TOKEN is required");
    process.exit(1);
  }

  const redis = createRedis(redisUrl);
  const pinboard = createPinboard({ redis, token });
  pinboard.server.listen(port, () => {
    console.log(`pinboard listening on :${port} (redis: ${redisUrl})`);
  });

  let shuttingDown = false;
  const shutdown = (): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("pinboard draining");
    const timer = setTimeout(() => process.exit(0), 10_000);
    timer.unref();
    void pinboard
      .close()
      .then(() => redis.quit())
      .then(() => process.exit(0));
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main();
