import { M as McpServerConfig, a as McpContext, S as SyntxToolExtra, b as SyntxTool, T as TransportKind } from './server-D7qHlG9k.mjs';
export { A as AIModel, c as AIModelSettings, d as AIResource, e as AIService, f as AppResource, g as AppSettings, h as AudioResource, i as AudioSettings, j as AuthStart, k as AuthTokenStatus, B as Balance, l as BaseClient, C as Chat, m as ChatsResource, n as CompletedMedia, o as CompletedMessage, p as CreateChatParams, q as CreateFolderParams, r as CreatedFolder, D as DEFAULT_CONFIG, s as DesignResource, t as DesignSettings, E as ENV_KEYS, u as EmailOtpOptions, v as EmailOtpSendResult, w as EmailOtpVerifyResult, F as Folder, x as FoldersResource, G as GenerateAudioParams, y as GenerateDesignParams, z as GenerateVideoParams, H as GetModelInfoParams, I as GoogleTokenResponse, J as InProgressItem, K as InProgressResponse, L as ListChatsParams, N as ListMessagesParams, O as ListNotificationsParams, P as ListVoiceExamplesParams, Q as LlmGenerateParams, R as LlmGenerateResponse, U as LlmLimitWindow, V as LlmLimits, W as LlmListModelsParams, X as LlmModel, Y as LlmResource, Z as LlmStreamJob, _ as Locale, $ as MaintenanceStatus, a0 as Message, a1 as MessageAttachment, a2 as MessageObject, a3 as MessageObjectItem, a4 as MessagesResponse, a5 as ModelInfoV2, a6 as Notification, a7 as NotificationsResource, a8 as NotificationsResponse, a9 as OAuthProvider, aa as Pagination, ab as PlanCard, ac as PlansResource, ad as PlansResponse, ae as PromoBanner, af as PublicUser, ag as RawLlmLimitWindow, ah as ReferralInfo, ai as SendChatMessageParams, aj as SendMessageParams, ak as SettingsResource, al as StreamMode, am as StreamResponseOptions, an as StreamResponseResult, ao as StreamingMessage, ap as Subscription, aq as SyntxAuth, ar as SyntxClient, as as SyntxClientConfig, at as SyntxToolResult, au as SyntxWebSocket, av as SyntxWebSocketOptions, aw as UnreadCount, ax as UploadFileInput, ay as UploadResult, az as User, aA as UserInternal, aB as UserResource, aC as UserSettings, aD as VersionInfo, aE as VideoResource, aF as VideoSettings, aG as VoiceExample, aH as VoiceExamplesResponse, aI as WSSMessage, aJ as WaitForResponseOptions, aK as collectCompletedObjects, aL as createMcpServer, aM as toPublicUser } from './server-D7qHlG9k.mjs';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import '@modelcontextprotocol/sdk/types.js';
import '@modelcontextprotocol/sdk/shared/protocol.js';

/**
 * Custom error thrown by the Syntx SDK on API failures.
 */
declare class SyntxAPIError extends Error {
    readonly status: number;
    readonly code?: string | undefined;
    readonly responseBody?: unknown | undefined;
    /**
     * Parsed `Retry-After` header (ms) for 429 responses, when present.
     * Used by the retry layer to honour server-mandated backoff.
     */
    readonly retryAfterMs?: number | undefined;
    constructor(message: string, status: number, code?: string | undefined, responseBody?: unknown | undefined, 
    /**
     * Parsed `Retry-After` header (ms) for 429 responses, when present.
     * Used by the retry layer to honour server-mandated backoff.
     */
    retryAfterMs?: number | undefined);
}
/**
 * Error thrown when authentication is missing or invalid.
 */
declare class SyntxAuthError extends Error {
    constructor(message?: string);
}
/**
 * Error thrown when a polling wait exceeds its time budget.
 *
 * Carries the structured context an MCP client needs for self-service
 * recovery: the chat keeps existing on the server after a timeout, so the
 * caller can resume with `get-messages` / `wait-for-response` on
 * {@link chatId} instead of re-sending the prompt (which would duplicate
 * the chat and double the token spend).
 */
declare class SyntxTimeoutError extends Error {
    readonly chatId: string | undefined;
    readonly elapsedMs: number;
    readonly timeoutMs: number;
    constructor(message: string, chatId: string | undefined, elapsedMs: number, timeoutMs: number);
}
/**
 * Error thrown when a wait/poll is cancelled via AbortSignal — typically
 * because the MCP client disconnected or its request-level timeout fired.
 * Distinct from {@link SyntxTimeoutError}: cancellation is not a failure
 * of the upstream API and must not be retried or counted as a poll error.
 */
declare class SyntxAbortError extends Error {
    constructor(message?: string);
}

/**
 * Load server configuration from environment variables, merged on top of
 * {@link DEFAULT_CONFIG}. Unknown/invalid values fall back to defaults
 * rather than throwing, so the server always boots.
 */
declare function loadConfig(env?: NodeJS.ProcessEnv): McpServerConfig;

/**
 * Build the shared {@link McpContext} for a server instance.
 *
 * Token precedence (M2, v0.3.0):
 *   1. `requestToken` — HTTP request `Authorization`-header scope
 *      (multi-tenant / credential-passthrough mode). When present the
 *      credential is immutable for the lifetime of the context: `setToken`
 *      throws, so no request can ever overwrite another tenant's token.
 *   2. Runtime `set-token` — stdio only (H4 invariant blocks it over HTTP).
 *   3. `config.token` (env `SYNTX_TOKEN`) — startup-immutable fallback.
 *
 * The context owns the {@link SyntxClient} used by every tool/resource.
 * Over the stateless HTTP transport a fresh context is built per request,
 * so a request-scoped token never leaks across connections.
 *
 * `config` is a *live, mutable* object internally, but the public field is
 * typed `Readonly<McpServerConfig>` — callers should always go through the
 * provided mutators (`setDefaultModel`, `setDefaultAI`) rather than mutating
 * the object directly.
 */
declare function createMcpContext(config: McpServerConfig, requestToken?: string): McpContext;
/**
 * Return a shallow-cloned context enriched with `sendProgress` / `sendLog`
 * bound to the current MCP request extra. Tool handlers receive the cloned
 * context via the second argument.
 *
 * Progress notifications are no-ops when the client did not supply a
 * `progressToken` in the request `_meta`.
 */
declare function withRequestContext(base: McpContext, extra: SyntxToolExtra | undefined): McpContext;

/**
 * Central registry of all MCP tools exposed by syntx-ai-mcp.
 * New tool modules must be imported and spread here to become available.
 */
declare const allTools: SyntxTool[];

/**
 * Connect the MCP server over the stdio transport.
 *
 * stdio is the default and recommended transport for local MCP clients
 * (Claude Desktop, IDE agents): the server runs as a child process and
 * communicates over its standard streams.
 */
declare function startStdio(server: Server): Promise<void>;

/**
 * Options for the HTTP/SSE transport.
 */
interface HttpTransportOptions {
    /**
     * Builds a brand-new, connected-ready MCP server per request.
     *
     * `requestToken` (M2, v0.3.0) is the bearer credential extracted from the
     * request's own `Authorization` header — only present when the transport-
     * level `httpToken` gate is NOT configured (credential-passthrough mode).
     * When `httpToken` IS configured, the header authenticates the transport
     * gate and is never forwarded to the MCP layer.
     */
    serverFactory: (requestToken?: string) => Server;
    /** TCP port to listen on. */
    port: number;
    /** Bind address. Defaults to loopback (127.0.0.1) for safety. */
    hostname?: string;
    /**
     * Optional bearer token MCP clients must present. When set, requests
     * without a matching `Authorization: Bearer <token>` header are rejected
     * with 401. When unset, the transport is loopback-only and a security
     * warning is printed at startup.
     */
    httpToken?: string;
    /**
     * Hard limit on the request body in bytes. Default 1 MB; configurable via
     * `MCP_HTTP_MAX_BODY_BYTES`; absolute hard cap is 100 MB to keep the
     * server immune from OOM-DoS via giant payloads (M1).
     */
    maxBodyBytes?: number;
    /**
     * Maximum concurrent standalone SSE (GET) streams. Default 100;
     * configurable via `MCP_HTTP_MAX_SSE_CLIENTS`. Excess connections are
     * rejected with 429 before any server work happens (M6).
     */
    maxSseClients?: number;
    /**
     * Idle timeout (ms) for standalone SSE streams: a stream with no bytes
     * written for this long is torn down. Default 60 000; configurable via
     * `MCP_HTTP_SSE_IDLE_TIMEOUT_MS`; `0` disables the idle reaper (M6).
     */
    sseIdleTimeoutMs?: number;
}
/**
 * Run the MCP server over HTTP with SSE streaming, using the canonical
 * **stateless** pattern: a fresh transport (and a fresh server instance)
 * is created per request. This avoids the "server already connected" error
 * and suits remote / serverless MCP clients.
 *
 * Security (v0.2.1+):
 *  - **Host/Origin allow-list** — always on; rejects DNS-rebinding requests
 *    whose `Host`/`Origin` header is not a loopback/expected host.
 *  - **Bearer auth** — when `httpToken` is set, requests must carry a matching
 *    `Authorization: Bearer <token>` header (timing-safe compare).
 *  - **CORS preflight** — `OPTIONS` is answered 200 without a token check; no
 *    wildcard `Access-Control-Allow-Origin` is ever emitted.
 *  - **Method allow-list** — only `POST`, `OPTIONS`, and (for SSE) `GET`
 *    against the MCP endpoint are accepted. `HEAD`/`TRACE`/`PUT`/`DELETE`
 *    return 405. The MCP spec uses GET for the standalone SSE stream; we
 *    permit it but require `Accept: text/event-stream` and a zero-length
 *    body. (L1 + L2)
 *  - **Body size limit** — `MCP_HTTP_MAX_BODY_BYTES` (default 1 MB,
 *    hard-capped at 100 MB) prevents OOM DoS. (M1)
 *  - **Content-Encoding** — `gzip`/`br` are rejected; MCP JSON payloads do
 *    not require compression and decoding a multi-GB compressed stream
 *    would defeat the body limit. (M1)
 *  - **Adversarial Host/Origin** — `checkHostHeader` normalises case,
 *    rejects IDN/Unicode, IPv4-mapped IPv6, trailing dots, and embedded
 *    `Host` lists; `X-Forwarded-*` headers are ignored by default. (L4)
 *
 * @returns a function to stop the HTTP server.
 */
declare function startHttp(opts: HttpTransportOptions): Promise<() => Promise<void>>;

interface TransportRunResult {
    /** Stops the running transport (closes the HTTP server; stdio is a no-op). */
    stop?: () => Promise<void>;
}
/** HTTP transport options threaded through {@link runTransport}. */
interface RunTransportHttpOptions {
    hostname?: string;
    httpToken?: string;
}
/**
 * Connect MCP servers to the requested transport and keep them running.
 *
 * - **stdio**: a single long-lived server instance serves the whole process.
 * - **http**: stateless — a fresh server is built from `serverFactory` for
 *   each request (see {@link startHttp}). `httpOptions` carries the bind
 *   hostname and optional bearer token. The optional `requestToken` argument
 *   carries the M2 request-scoped credential (HTTP Authorization passthrough);
 *   stdio callers simply ignore it.
 */
declare function runTransport(serverFactory: (requestToken?: string) => Server, kind: TransportKind, httpPort: number, httpOptions?: RunTransportHttpOptions): Promise<TransportRunResult>;

export { McpContext, McpServerConfig, SyntxAPIError, SyntxAbortError, SyntxAuthError, SyntxTimeoutError, SyntxTool, SyntxToolExtra, TransportKind, allTools, createMcpContext, loadConfig, runTransport, startHttp, startStdio, withRequestContext };
