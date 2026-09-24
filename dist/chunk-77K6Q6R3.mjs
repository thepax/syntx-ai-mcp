import {
  logSecurityEvent
} from "./chunk-B2QIVNSM.mjs";

// src/config/schema.ts
var DEFAULT_CONFIG = {
  baseURL: "https://api.syntx.ai",
  timeout: 3e4,
  lang: "en",
  defaultAI: "chatgpt",
  pollInterval: 5e3,
  pollTimeout: 6e5,
  transport: "stdio",
  httpPort: 3e3,
  httpHostname: "127.0.0.1",
  streamMode: "auto",
  wsURL: "wss://api.syntx.ai/api/v1",
  llmSseBaseUrl: "https://sse.syntx.ai",
  legacyTextTransport: false,
  listLlmModelsCacheMs: 6e4
};
var ENV_KEYS = {
  token: "SYNTX_TOKEN",
  baseURL: "SYNTX_BASE_URL",
  timeout: "SYNTX_TIMEOUT",
  lang: "SYNTX_LANG",
  defaultAI: "SYNTX_DEFAULT_AI",
  defaultModel: "SYNTX_DEFAULT_MODEL",
  pollInterval: "SYNTX_POLL_INTERVAL",
  pollTimeout: "SYNTX_POLL_TIMEOUT",
  transport: "MCP_TRANSPORT",
  httpPort: "MCP_HTTP_PORT",
  httpHostname: "MCP_HTTP_HOSTNAME",
  httpToken: "MCP_HTTP_TOKEN",
  streamMode: "SYNTX_STREAM_MODE",
  wsURL: "SYNTX_WS_URL",
  llmSseBaseUrl: "SYNTX_LLM_SSE_BASE_URL",
  legacyTextTransport: "SYNTX_LEGACY_TEXT_TRANSPORT",
  listLlmModelsCacheMs: "SYNTX_LLM_MODELS_CACHE_MS"
};

// src/config/index.ts
function loadConfig(env = process.env) {
  return {
    ...DEFAULT_CONFIG,
    token: env[ENV_KEYS.token] || void 0,
    baseURL: env[ENV_KEYS.baseURL] || DEFAULT_CONFIG.baseURL,
    timeout: parseNumber(env[ENV_KEYS.timeout], DEFAULT_CONFIG.timeout),
    lang: env[ENV_KEYS.lang] || DEFAULT_CONFIG.lang,
    defaultAI: env[ENV_KEYS.defaultAI] || DEFAULT_CONFIG.defaultAI,
    defaultModel: env[ENV_KEYS.defaultModel] || void 0,
    pollInterval: parseNumber(env[ENV_KEYS.pollInterval], DEFAULT_CONFIG.pollInterval),
    pollTimeout: parseNumber(env[ENV_KEYS.pollTimeout], DEFAULT_CONFIG.pollTimeout),
    transport: parseTransport(env[ENV_KEYS.transport], DEFAULT_CONFIG.transport),
    httpPort: parseNumber(env[ENV_KEYS.httpPort], DEFAULT_CONFIG.httpPort),
    httpHostname: env[ENV_KEYS.httpHostname] || DEFAULT_CONFIG.httpHostname,
    httpToken: env[ENV_KEYS.httpToken] || void 0,
    streamMode: parseStreamMode(env[ENV_KEYS.streamMode], DEFAULT_CONFIG.streamMode),
    wsURL: env[ENV_KEYS.wsURL] || DEFAULT_CONFIG.wsURL,
    llmSseBaseUrl: env[ENV_KEYS.llmSseBaseUrl] || DEFAULT_CONFIG.llmSseBaseUrl,
    legacyTextTransport: parseBool(env[ENV_KEYS.legacyTextTransport], DEFAULT_CONFIG.legacyTextTransport),
    listLlmModelsCacheMs: parseNumber(env[ENV_KEYS.listLlmModelsCacheMs], DEFAULT_CONFIG.listLlmModelsCacheMs)
  };
}
function parseNumber(raw, fallback) {
  if (raw === void 0 || raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}
function parseTransport(raw, fallback) {
  if (raw === "stdio" || raw === "http") return raw;
  return fallback;
}
function parseStreamMode(raw, fallback) {
  if (raw === "auto" || raw === "stream" || raw === "poll" || raw === "off") return raw;
  return fallback;
}
function parseBool(raw, fallback) {
  if (raw === void 0 || raw === "") return fallback;
  const v = raw.trim().toLowerCase();
  if (v === "1" || v === "true" || v === "yes" || v === "on") return true;
  if (v === "0" || v === "false" || v === "no" || v === "off") return false;
  return fallback;
}

// src/transport/stdio.ts
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
async function startStdio(server) {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

// src/transport/http.ts
import http from "http";
import { timingSafeEqual } from "crypto";
import { Buffer } from "buffer";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
var DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024;
var ABSOLUTE_MAX_BODY_BYTES = 100 * 1024 * 1024;
var DEFAULT_MAX_SSE_CLIENTS = 100;
var DEFAULT_SSE_IDLE_TIMEOUT_MS = 6e4;
function resolveMaxBodyBytes() {
  const raw = process.env.MCP_HTTP_MAX_BODY_BYTES;
  if (!raw) return DEFAULT_MAX_BODY_BYTES;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_MAX_BODY_BYTES;
  return Math.min(n, ABSOLUTE_MAX_BODY_BYTES);
}
function resolveMaxSseClients() {
  const raw = process.env.MCP_HTTP_MAX_SSE_CLIENTS;
  if (!raw) return DEFAULT_MAX_SSE_CLIENTS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_MAX_SSE_CLIENTS;
  return Math.floor(n);
}
function resolveSseIdleTimeoutMs() {
  const raw = process.env.MCP_HTTP_SSE_IDLE_TIMEOUT_MS;
  if (!raw) return DEFAULT_SSE_IDLE_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return DEFAULT_SSE_IDLE_TIMEOUT_MS;
  return Math.floor(n);
}
async function startHttp(opts) {
  const { serverFactory, port } = opts;
  const hostname = opts.hostname ?? "127.0.0.1";
  const expectedToken = opts.httpToken?.trim() || void 0;
  const maxBodyBytes = opts.maxBodyBytes ?? resolveMaxBodyBytes();
  const maxSseClients = opts.maxSseClients ?? resolveMaxSseClients();
  const sseIdleTimeoutMs = opts.sseIdleTimeoutMs ?? resolveSseIdleTimeoutMs();
  let activeSseClients = 0;
  const allowedHosts = /* @__PURE__ */ new Set([
    "127.0.0.1",
    "localhost",
    "::1",
    hostname.toLowerCase()
  ]);
  const isLoopbackBind = ["127.0.0.1", "localhost", "::1"].includes(hostname.toLowerCase());
  if (!expectedToken) {
    if (isLoopbackBind) {
      console.error(
        "[syntx-mcp] WARNING: MCP_HTTP_TOKEN is not set. The HTTP transport is running unauthenticated on loopback. Any local process or website (via loopback / DNS-rebinding) could access it. Set MCP_HTTP_TOKEN for production use. Do not run in untrusted environments."
      );
    } else {
      throw new Error(
        `[syntx-mcp] Refusing to start: HTTP transport is bound to a non-loopback address (${hostname}) without MCP_HTTP_TOKEN. Set MCP_HTTP_TOKEN before exposing the server, or bind to 127.0.0.1.`
      );
    }
  } else if (!isLoopbackBind) {
    console.error(
      "[syntx-mcp] HTTP transport bound to non-loopback address (" + hostname + ") \u2014 ensure MCP_HTTP_TOKEN and a network firewall are in place."
    );
  }
  const httpServer = http.createServer(
    {
      // Keep header sizes bounded so a single huge header line can't OOM
      // the parser before our handler runs.
      maxHeaderSize: 16 * 1024,
      // Per-socket timeouts: a slow loris client cannot pin a worker
      // indefinitely.
      requestTimeout: 3e4,
      headersTimeout: 1e4
    },
    async (req, res) => {
      const clientAddr = req.socket.remoteAddress ?? "unknown";
      if (req.url === "/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "ok" }));
        return;
      }
      if (!req.url?.startsWith("/mcp")) {
        res.writeHead(404, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "Not found" }));
        return;
      }
      const hostOk = checkHostHeader(req.headers.host, allowedHosts);
      const originOk = checkHostHeader(req.headers.origin, allowedHosts, true);
      if (!hostOk || !originOk) {
        logSecurityEvent({
          kind: "transport.host.rejected",
          transport: "http",
          clientAddr,
          reason: !hostOk ? "host-not-allowed" : "origin-not-allowed"
        });
        res.writeHead(403, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: { code: -32e3, message: "Forbidden: Host/Origin not allowed." },
            id: null
          })
        );
        return;
      }
      if (req.method === "OPTIONS") {
        res.writeHead(200);
        res.end();
        return;
      }
      if (req.method === "POST") {
      } else if (req.method === "GET") {
        const accept = (req.headers.accept ?? "").toString();
        if (!accept.includes("text/event-stream")) {
          logSecurityEvent({
            kind: "transport.method.rejected",
            transport: "http",
            clientAddr,
            reason: "get-without-sse-accept",
            meta: { method: "GET" }
          });
          res.writeHead(405, {
            "content-type": "application/json",
            allow: "POST, OPTIONS, GET"
          });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: { code: -32e3, message: "Method not allowed." },
              id: null
            })
          );
          return;
        }
      } else {
        logSecurityEvent({
          kind: "transport.method.rejected",
          transport: "http",
          clientAddr,
          reason: "method-not-allowed",
          meta: { method: req.method ?? "unknown" }
        });
        res.writeHead(405, {
          "content-type": "application/json",
          allow: "POST, OPTIONS, GET"
        });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: { code: -32e3, message: "Method not allowed." },
            id: null
          })
        );
        return;
      }
      const requestToken = expectedToken ? void 0 : extractBearerToken(req.headers.authorization);
      if (req.method === "GET") {
        const cl = Number(req.headers["content-length"] ?? "0");
        if (!Number.isFinite(cl) || cl !== 0) {
          logSecurityEvent({
            kind: "transport.method.rejected",
            transport: "http",
            clientAddr,
            reason: "get-with-body",
            meta: { method: "GET" }
          });
          res.writeHead(400, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: { code: -32e3, message: "Bad Request: SSE GET must not carry a body." },
              id: null
            })
          );
          return;
        }
      }
      if (expectedToken && !isAuthorized(req.headers.authorization, expectedToken)) {
        logSecurityEvent({
          kind: "transport.auth.missing",
          transport: "http",
          clientAddr,
          reason: "missing-or-invalid-bearer"
        });
        res.writeHead(401, { "content-type": "application/json", "www-authenticate": "Bearer" });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: { code: -32001, message: "Unauthorized: missing or invalid bearer token." },
            id: null
          })
        );
        return;
      }
      if (req.method === "POST") {
        const contentEncoding = (req.headers["content-encoding"] ?? "").toString().toLowerCase();
        if (contentEncoding && contentEncoding !== "identity") {
          logSecurityEvent({
            kind: "transport.content_encoding.rejected",
            transport: "http",
            clientAddr,
            reason: contentEncoding
          });
          res.writeHead(415, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: { code: -32e3, message: "Unsupported Media Type: Content-Encoding not supported." },
              id: null
            })
          );
          return;
        }
        const contentType = (req.headers["content-type"] ?? "").toString().toLowerCase();
        if (!contentType.includes("application/json")) {
          logSecurityEvent({
            kind: "transport.content_type.rejected",
            transport: "http",
            clientAddr,
            reason: contentType || "missing",
            meta: { mime: contentType || "missing" }
          });
          res.writeHead(415, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: { code: -32e3, message: "Unsupported Media Type: expected application/json." },
              id: null
            })
          );
          return;
        }
        const contentLength = Number(req.headers["content-length"] ?? "0");
        if (Number.isFinite(contentLength) && contentLength > maxBodyBytes) {
          logSecurityEvent({
            kind: "transport.body.too_large",
            transport: "http",
            clientAddr,
            reason: "content-length",
            meta: { limitBytes: maxBodyBytes, observedBytes: contentLength }
          });
          res.writeHead(413, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: { code: -32e3, message: "Payload too large." },
              id: null
            })
          );
          return;
        }
        const chunks = [];
        let total = 0;
        let aborted = false;
        req.on("data", (chunk) => {
          if (aborted) return;
          total += chunk.length;
          if (total > maxBodyBytes) {
            aborted = true;
            logSecurityEvent({
              kind: "transport.body.too_large",
              transport: "http",
              clientAddr,
              reason: "chunked-accumulator",
              meta: { limitBytes: maxBodyBytes, observedBytes: total }
            });
            res.writeHead(413, { "content-type": "application/json" });
            res.end(
              JSON.stringify({
                jsonrpc: "2.0",
                error: { code: -32e3, message: "Payload too large." },
                id: null
              }),
              () => req.destroy()
            );
            return;
          }
          chunks.push(chunk);
        });
        req.on("end", () => {
          if (aborted) return;
          let parsedBody;
          try {
            parsedBody = total > 0 ? JSON.parse(Buffer.concat(chunks, total).toString("utf8")) : void 0;
          } catch {
            logSecurityEvent({
              kind: "transport.method.rejected",
              transport: "http",
              clientAddr,
              reason: "invalid-json-body"
            });
            res.writeHead(400, { "content-type": "application/json" });
            res.end(
              JSON.stringify({
                jsonrpc: "2.0",
                error: { code: -32700, message: "Parse error: Invalid JSON" },
                id: null
              })
            );
            return;
          }
          runRequest(serverFactory, req, res, parsedBody, requestToken).catch((err) => {
            if (!res.headersSent) {
              res.writeHead(500, { "content-type": "application/json" });
              res.end(
                JSON.stringify({
                  jsonrpc: "2.0",
                  error: {
                    code: -32603,
                    message: err instanceof Error ? err.message : "Internal error"
                  },
                  id: null
                })
              );
            }
          });
        });
        req.on("error", () => {
        });
        return;
      }
      if (activeSseClients >= maxSseClients) {
        logSecurityEvent({
          kind: "transport.sse.limit",
          transport: "http",
          clientAddr,
          reason: "max-concurrent-sse-clients",
          meta: { limit: maxSseClients }
        });
        res.writeHead(429, {
          "content-type": "application/json",
          "retry-after": "5"
        });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: { code: -32e3, message: "Too many concurrent SSE streams." },
            id: null
          })
        );
        return;
      }
      activeSseClients++;
      const releaseSseIdle = sseIdleTimeoutMs > 0 ? armSseIdleReaper(res, sseIdleTimeoutMs) : () => {
      };
      res.on("close", () => {
        activeSseClients--;
        releaseSseIdle();
      });
      runRequest(serverFactory, req, res, void 0, requestToken).catch((err) => {
        if (!res.headersSent) {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32603,
                message: err instanceof Error ? err.message : "Internal error"
              },
              id: null
            })
          );
        }
      });
    }
  );
  await new Promise((resolve) => httpServer.listen(port, hostname, resolve));
  return () => new Promise((resolve) => {
    httpServer.close(() => resolve());
  });
}
async function runRequest(serverFactory, req, res, parsedBody, requestToken) {
  const server = serverFactory(requestToken);
  try {
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: void 0 });
    await server.connect(transport);
    await transport.handleRequest(req, res, parsedBody);
    res.on("close", () => {
      transport.close();
      server.close();
    });
  } catch (err) {
    if (!res.headersSent) {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32603,
            message: err instanceof Error ? err.message : "Internal error"
          },
          id: null
        })
      );
    }
  }
}
function checkHostHeader(headerValue, allowed, optional = false) {
  if (headerValue === void 0) return optional;
  const raw = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (!raw) return optional;
  if (Array.isArray(headerValue) && headerValue.length > 1) return false;
  if (raw.includes(",") || raw.includes("\0")) return false;
  if (/[\x00-\x1f\x7f\s]/.test(raw)) return false;
  if (/[^\x00-\x7f]/.test(raw)) return false;
  let host = raw.trim().toLowerCase();
  if (host.endsWith(".")) host = host.slice(0, -1);
  if (!host) return false;
  const schemeMatch = host.match(/^[a-z]+:\/\/([^/:]+)/);
  if (schemeMatch) host = schemeMatch[1];
  if (!host) return false;
  if (host.startsWith("[")) {
    const end = host.indexOf("]");
    host = end !== -1 ? host.slice(1, end) : host;
  } else {
    const firstColon = host.indexOf(":");
    if (firstColon !== -1) {
      const lastColon = host.lastIndexOf(":");
      if (firstColon === lastColon) {
        host = host.slice(0, firstColon);
      }
    }
  }
  if (/^::ffff:/i.test(host)) {
    return false;
  }
  const ipv4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipv4) {
    const canonical = ipv4.slice(1, 5).map((oct) => String(Number(oct))).join(".");
    if (canonical !== host) host = canonical;
  }
  return allowed.has(host);
}
function extractBearerToken(headerValue) {
  if (!headerValue) return void 0;
  const raw = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (!raw) return void 0;
  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 2) return void 0;
  if (parts[0].toLowerCase() !== "bearer") return void 0;
  return parts[1] || void 0;
}
function armSseIdleReaper(res, idleTimeoutMs) {
  let timer;
  const arm = () => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      res.destroy();
    }, idleTimeoutMs);
    timer.unref();
  };
  const originalWrite = res.write.bind(res);
  res.write = ((chunk, ...rest) => {
    arm();
    return originalWrite(chunk, ...rest);
  });
  arm();
  return () => {
    if (timer) clearTimeout(timer);
    timer = void 0;
  };
}
function isAuthorized(headerValue, expected) {
  if (!headerValue) return false;
  const raw = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (!raw) return false;
  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 2) return false;
  if (parts[0].toLowerCase() !== "bearer") return false;
  const provided = Buffer.from(parts[1]);
  const expectedBuf = Buffer.from(expected);
  if (provided.length !== expectedBuf.length) {
    timingSafeEqual(provided, provided);
    return false;
  }
  return timingSafeEqual(provided, expectedBuf);
}

// src/transport/index.ts
async function runTransport(serverFactory, kind, httpPort, httpOptions = {}) {
  if (kind === "http") {
    const stop = await startHttp({
      serverFactory,
      port: httpPort,
      hostname: httpOptions.hostname,
      httpToken: httpOptions.httpToken
    });
    return { stop };
  }
  await startStdio(serverFactory());
  return {};
}

export {
  DEFAULT_CONFIG,
  ENV_KEYS,
  loadConfig,
  startStdio,
  startHttp,
  runTransport
};
