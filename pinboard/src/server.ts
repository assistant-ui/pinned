import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { createHash, timingSafeEqual } from "node:crypto";
import { WebSocketServer } from "ws";
import type { RedisKV } from "./redis.js";
import { Registry } from "./registry.js";
import { createProxy, type Proxy } from "./proxy.js";
import { matchRoute } from "./router.js";
import { resolvePlacement } from "./placement.js";
import { NAMESPACES_KEY } from "./keys.js";

export interface PinboardOptions {
  redis: RedisKV;
  token: string;
  leaseTtlMs?: number;
}

export interface Pinboard {
  server: Server;
  registry: Registry;
  close(): Promise<void>;
}

function sha256(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

function authorized(req: IncomingMessage, token: string): boolean {
  const header = req.headers.authorization;
  if (header === undefined || !header.startsWith("Bearer ")) return false;
  return timingSafeEqual(sha256(header.slice(7)), sha256(token));
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json" });
  res.end(payload);
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

export function createPinboard(options: PinboardOptions): Pinboard {
  const { redis, token } = options;
  const registry = new Registry(redis, options.leaseTtlMs);
  const proxy: Proxy = createProxy();
  const wss = new WebSocketServer({ noServer: true });

  wss.on("connection", (ws) => {
    registry.handleConnection(ws);
  });

  const handleRegistry = async (
    req: IncomingMessage,
    res: ServerResponse,
    pathname: string,
  ): Promise<void> => {
    if (!authorized(req, token)) {
      return sendJson(res, 401, { error: "unauthorized" });
    }
    if (pathname === "/registry/workers" && req.method === "GET") {
      return sendJson(res, 200, { workers: await registry.listWorkers() });
    }
    if (pathname === "/registry/push" && req.method === "POST") {
      let body: unknown;
      try {
        body = await readJsonBody(req);
      } catch {
        return sendJson(res, 400, { error: "invalid json" });
      }
      const msg = body as { epoch?: unknown; message?: unknown };
      if (
        typeof msg.epoch !== "number" ||
        typeof msg.message !== "object" ||
        msg.message === null
      ) {
        return sendJson(res, 400, { error: "expected { epoch, message }" });
      }
      const delivered = await registry.push(
        msg.epoch,
        msg.message as Record<string, unknown>,
      );
      return sendJson(res, 200, { delivered });
    }
    sendJson(res, 404, { error: "not found" });
  };

  const handleRequest = async (
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> => {
    const url = new URL(req.url ?? "/", "http://pinboard.invalid");
    const pathname = url.pathname;

    if (pathname === "/healthz") {
      return sendJson(res, 200, { ok: true });
    }
    if (pathname.startsWith("/registry/")) {
      return handleRegistry(req, res, pathname);
    }

    const namespaces = await redis.smembers(NAMESPACES_KEY);
    const match = matchRoute(pathname, namespaces);
    if (match === null) {
      return sendJson(res, 404, { error: "unknown namespace" });
    }

    const resolution = await resolvePlacement(redis, match.ns, match.id);
    switch (resolution.kind) {
      case "forward": {
        const target =
          resolution.worker.url + "/" + match.id + match.rest + url.search;
        return proxy.forward(req, res, target);
      }
      case "blackout": {
        res.writeHead(503, {
          "content-type": "application/json",
          "retry-after": String(Math.ceil(resolution.retryAfterMs / 1000)),
        });
        res.end(JSON.stringify({ error: "placement in blackout, retry" }));
        return;
      }
      case "no-backends":
        return sendJson(res, 503, { error: "no live backends for namespace" });
      case "contended":
        return sendJson(res, 503, { error: "placement contention, retry" });
    }
  };

  const server = createServer((req, res) => {
    handleRequest(req, res).catch(() => {
      if (!res.headersSent) {
        sendJson(res, 500, { error: "internal error" });
      } else {
        res.destroy();
      }
    });
  });

  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://pinboard.invalid");
    if (url.pathname !== "/registry/ws") {
      socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    if (!authorized(req, token)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  return {
    server,
    registry,
    async close() {
      for (const client of wss.clients) client.terminate();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve())),
      );
      await proxy.close();
    },
  };
}
