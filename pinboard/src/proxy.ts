import type { IncomingMessage, ServerResponse } from "node:http";
import { pipeline } from "node:stream/promises";
import { Agent, request } from "undici";

const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "host",
]);

export interface Proxy {
  forward(
    req: IncomingMessage,
    res: ServerResponse,
    targetUrl: string,
  ): Promise<void>;
  close(): Promise<void>;
}

function requestHeaders(req: IncomingMessage): Record<string, string | string[]> {
  const headers: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(req.headers)) {
    if (value === undefined || HOP_BY_HOP.has(name)) continue;
    headers[name] = value;
  }
  const remote = req.socket.remoteAddress;
  if (remote !== undefined) {
    const prior = req.headers["x-forwarded-for"];
    headers["x-forwarded-for"] = prior ? `${String(prior)}, ${remote}` : remote;
  }
  return headers;
}

export function createProxy(): Proxy {
  // bodyTimeout 0: SSE bodies legitimately idle for minutes between events.
  const agent = new Agent({
    keepAliveTimeout: 30_000,
    connectTimeout: 5_000,
    headersTimeout: 30_000,
    bodyTimeout: 0,
  });

  return {
    async forward(req, res, targetUrl) {
      const method = req.method ?? "GET";
      const hasBody =
        req.headers["content-length"] !== undefined ||
        req.headers["transfer-encoding"] !== undefined;
      const abort = new AbortController();
      res.on("close", () => {
        if (!res.writableEnded) abort.abort();
      });

      let upstream;
      try {
        upstream = await request(targetUrl, {
          method: method as "GET",
          headers: requestHeaders(req),
          ...(hasBody ? { body: req } : {}),
          signal: abort.signal,
          dispatcher: agent,
        });
      } catch (err) {
        if (res.headersSent || res.writableEnded) {
          res.destroy();
          return;
        }
        const timedOut =
          err instanceof Error &&
          (err.name === "HeadersTimeoutError" || err.name === "ConnectTimeoutError");
        res.writeHead(timedOut ? 504 : 502, { "content-type": "text/plain" });
        res.end(timedOut ? "upstream timeout" : "bad gateway");
        return;
      }

      const headers: Record<string, string | string[]> = {};
      for (const [name, value] of Object.entries(upstream.headers)) {
        if (value === undefined || HOP_BY_HOP.has(name.toLowerCase())) continue;
        headers[name] = value;
      }
      headers["x-accel-buffering"] = "no";
      res.writeHead(upstream.statusCode, headers);
      try {
        await pipeline(upstream.body, res);
      } catch {
        res.destroy();
        upstream.body.destroy();
      }
    },
    async close() {
      await agent.close();
    },
  };
}
