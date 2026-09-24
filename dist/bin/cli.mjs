#!/usr/bin/env node
import {
  loadConfig,
  runTransport
} from "../chunk-77K6Q6R3.mjs";
import {
  createMcpServer
} from "../chunk-B2QIVNSM.mjs";

// src/bin/cli.ts
function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const next = argv[i + 1];
    switch (arg) {
      case "--help":
      case "-h":
        out.help = true;
        break;
      case "--transport":
        if (next === "stdio" || next === "http") {
          out.transport = next;
          i++;
        }
        break;
      case "--token":
        out.token = next;
        i++;
        break;
      case "--base-url":
        out.baseURL = next;
        i++;
        break;
      case "--http-port":
        out.httpPort = Number(next);
        i++;
        break;
      default:
        if (arg.startsWith("--")) {
          console.error(`Unknown option: ${arg}`);
          process.exit(2);
        }
    }
  }
  return out;
}
var HELP = `
syntx-mcp \u2014 MCP server for the syntx.ai platform

Options:
  --transport <stdio|http>   MCP transport (default: stdio, or MCP_TRANSPORT env)
  --token <token>            syntx.ai bearer token (default: SYNTX_TOKEN env)
  --base-url <url>           Override the API base URL (default: https://api.syntx.ai)
  --http-port <port>         Port for the HTTP transport (default: 3000)
  -h, --help                 Show this help

Environment variables:
  SYNTX_TOKEN, SYNTX_BASE_URL, SYNTX_TIMEOUT, SYNTX_LANG,
  SYNTX_DEFAULT_AI, SYNTX_DEFAULT_MODEL,
  SYNTX_POLL_INTERVAL, SYNTX_POLL_TIMEOUT,
  MCP_TRANSPORT, MCP_HTTP_PORT, MCP_HTTP_HOSTNAME, MCP_HTTP_TOKEN
`.trim();
async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(HELP);
    return;
  }
  const config = {
    ...loadConfig(),
    ...args.transport ? { transport: args.transport } : {},
    ...args.token ? { token: args.token } : {},
    ...args.baseURL ? { baseURL: args.baseURL } : {},
    ...args.httpPort ? { httpPort: args.httpPort } : {}
  };
  if (!config.token) {
    console.error(
      '[syntx-mcp] No token configured. Set SYNTX_TOKEN or pass --token, or supply one at runtime via the "set-token" tool.'
    );
  }
  const serverFactory = (requestToken) => createMcpServer(config, requestToken).server;
  try {
    await runTransport(serverFactory, config.transport, config.httpPort, {
      hostname: config.httpHostname,
      httpToken: config.httpToken
    });
  } catch (err) {
    console.error(
      `[syntx-mcp] Failed to start ${config.transport} transport:`,
      err instanceof Error ? err.message : err
    );
    process.exit(1);
  }
  if (config.transport === "http") {
    console.error(`[syntx-mcp] HTTP transport listening on http://${config.httpHostname}:${config.httpPort}/mcp`);
    console.error("[syntx-mcp] Health check at /health");
    if (config.httpToken) {
      console.error("[syntx-mcp] Bearer auth enabled (MCP_HTTP_TOKEN).");
    }
  } else {
    console.error("[syntx-mcp] stdio transport ready.");
  }
}
main().catch((err) => {
  console.error("[syntx-mcp] Fatal error:", err);
  process.exit(1);
});
