import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { Tool, ServerRequest, ServerNotification, CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { RequestHandlerExtra } from '@modelcontextprotocol/sdk/shared/protocol.js';

interface SyntxClientConfig {
    /** Base URL for the API. Defaults to https://api.syntx.ai */
    baseURL?: string;
    /** API token or session token */
    token?: string;
    /** Request timeout in ms. Defaults to 30000 */
    timeout?: number;
    /**
     * Max attempts for idempotent GET requests on transient failures
     * (429 / 5xx / network errors / own 408 timeout). Defaults to 3
     * (1 initial try + 2 retries). Mutating requests (POST/PATCH/DELETE)
     * are never retried automatically — e.g. re-sending a chat message
     * would double the token spend.
     */
    maxRetries?: number;
}
declare class BaseClient {
    readonly baseURL: string;
    private token;
    readonly timeout: number;
    readonly maxRetries: number;
    constructor(config?: SyntxClientConfig);
    setToken(token: string | undefined): void;
    getToken(): string | undefined;
    isAuthenticated(): boolean;
    private requestWithTimeout;
    /**
     * Execute a request, retrying transient failures with exponential
     * backoff + jitter when `retryable` is true. A `Retry-After` hint from
     * a 429 response overrides the computed delay.
     */
    private requestWithRetry;
    private handleResponse;
    private buildUrl;
    /** Shared `Accept` + bearer-token header block used by every request. */
    private baseHeaders;
    private jsonHeaders;
    get<T>(path: string, params?: Record<string, string | number | boolean | undefined>): Promise<T>;
    /**
     * Open a streaming response without consuming its body.
     *
     * Returns the raw {@link Response} so the caller can read the body as a
     * `ReadableStream<Uint8Array>` (e.g. for Server-Sent Events). The
     * response body is intentionally NOT consumed — `handleResponse` would
     * buffer it and break the stream.
     *
     * Goes through the same timeout/abort/error plumbing as `get`, but skips
     * the retry layer (an open stream cannot be replayed). Status errors are
     * surfaced as `Response` objects so the caller can inspect `ok`; abort is
     * surfaced as `SyntxAPIError(408)` like the other methods.
     *
     * `init.headers` are merged on top of {@link baseHeaders}, so callers can
     * override `Accept` (e.g. to `text/event-stream`) without rebuilding the
     * auth headers.
     */
    stream(path: string, init?: {
        headers?: Record<string, string>;
        signal?: AbortSignal;
        timeoutMs?: number;
    }): Promise<Response>;
    /**
     * Shared implementation for the JSON-bodied HTTP verbs (POST / PATCH /
     * PUT / DELETE): identical pipeline, only the method differs. `body`
     * is falsy-skipped so body-less calls (e.g. toggles) send no payload.
     */
    private jsonRequest;
    post<T>(path: string, body?: unknown, params?: Record<string, string | number | boolean | undefined>): Promise<T>;
    patch<T>(path: string, body?: unknown, params?: Record<string, string | number | boolean | undefined>): Promise<T>;
    put<T>(path: string, body?: unknown, params?: Record<string, string | number | boolean | undefined>): Promise<T>;
    delete<T>(path: string, body?: unknown, params?: Record<string, string | number | boolean | undefined>): Promise<T>;
    /**
     * POST a `FormData` body through the same timeout / auth / error-mapping
     * pipeline as JSON requests.
     *
     * Unlike JSON POSTs, `Content-Type` is intentionally NOT set — fetch
     * fills in the multipart boundary. `timeoutOverride` defaults to 5 min
     * because uploads/transcriptions routinely exceed the 30 s API default.
     */
    postForm<T>(path: string, formData: FormData, timeoutOverride?: number): Promise<T>;
}

/**
 * WebSocket-based real-time messaging for syntx.ai.
 *
 * @deprecated The syntx.ai API does not expose a WebSocket endpoint. The
 *   `wss://api.syntx.ai/api/v1/chats/stream` path is parsed by the server as
 *   `/chats/{chat_uuid}` (returning HTTP 422 for the non-UUID string "stream").
 *   This class is retained for potential future API support but is no longer
 *   used by {@link ChatsResource.streamResponse} or
 *   {@link ChatsResource.waitForResponse}, both of which now poll via REST.
 *   Do not rely on this module for production use.
 *
 * Not expected to ever emit (against the live API):
 *   - tokenized / incremental `content` frames — the REST endpoint delivers
 *     each `message_object` atomically, so the transport has no per-token
 *     granularity.
 *   - per-object stream updates — there is one `message_object[i].completed`
 *     flag per object, surfaced only by the polling endpoint; the SDK polls
 *     it directly.
 *   - synthetic `onChunk` typewriter output — `ChatsResource.streamResponse`
 *     invokes `onChunk` once with the full reply text, not per token.
 *
 * Security note (H3): the bearer token is no longer sent as a URL query
 * parameter (it leaked into access logs, browser history, and proxy traces).
 * In Node it is passed via the `Authorization: Bearer …` header instead.
 * The browser `WebSocket` constructor cannot set custom headers, so this
 * class is Node-only by definition.
 *
 * Migration: prefer {@link ChatsResource.waitForResponse} (REST polling) for
 * any production usage. The `wss://…/api/v1/chats/stream` endpoint has never
 * been functional against the live syntx.ai API.
 */
interface WSSMessage {
    type: string;
    [key: string]: unknown;
}
/**
 * Shape of an incoming WSS frame. The server is permissive — extra fields are
 * preserved on the object so callers can read provider-specific metadata
 * (model_type, usage, etc.).
 */
interface StreamingMessage {
    /** Server-issued message type, e.g. `session`, `message`, `done`, `error`. */
    type?: string;
    /** Incremental text fragment (when type === 'message'). */
    content?: string;
    /** Cumulative text assembled so far (provided by some providers). */
    text?: string;
    role?: 'user' | 'assistant' | 'system';
    /** Chat session UUID (set on `session` messages). */
    uuid?: string;
    /** Message UUID for the streamed assistant reply. */
    id?: string;
    /** True on the terminal `done` frame. */
    done?: boolean;
    error?: string;
    [key: string]: unknown;
}
type MessageHandler = (msg: StreamingMessage) => void;
type ConnectionHandler = () => void;
type ErrorHandler = (error: Error) => void;
/** Options accepted by {@link SyntxWebSocket}. */
interface SyntxWebSocketOptions {
    token?: string;
    lang?: string;
    /** Override the WSS base URL (defaults to `wss://api.syntx.ai/api/v1`). */
    baseURL?: string;
    /** Send a ping frame every N ms; 0 disables. Default 30000. */
    pingIntervalMs?: number;
    /** Reconnect automatically when the socket closes unexpectedly. Default false. */
    autoReconnect?: boolean;
}
/**
 * Lightweight WSS client for the syntx.ai streaming endpoint.
 *
 * The class intentionally keeps a single connection — `connect()` will throw
 * if the socket is already open, since reusing a WSS across sessions is
 * fragile and not supported by the syntx backend.
 */
declare class SyntxWebSocket {
    private ws;
    private baseURL;
    private token;
    private lang;
    private endpoint;
    private handlers;
    private connectHandlers;
    private disconnectHandlers;
    private errorHandlers;
    private pingTimer;
    private pingIntervalMs;
    private autoReconnect;
    private closedByUser;
    constructor(tokenOrOptions?: string | SyntxWebSocketOptions, lang?: string);
    /**
     * Build the WSS URL for an endpoint. The bearer token is intentionally
     * NOT placed in the query string (H3) — only the public `lang` parameter
     * remains. The token travels in the `Authorization: Bearer …` header set
     * by `connect()`.
     */
    private buildUrl;
    /**
     * Open a new WSS connection. Throws if the socket is already open.
     *
     * The bearer token is passed via the `Authorization` header (Node `ws`)
     * rather than as a query parameter (H3). The browser `WebSocket` global
     * cannot set custom headers and will silently drop the auth header; this
     * class is therefore Node-only.
     *
     * @param endpoint - Path under the base URL, e.g. `chats/stream`.
     */
    connect(endpoint: string): void;
    /**
     * Send a JSON frame over the open socket. No-op if the socket is not yet open.
     */
    send(data: Record<string, unknown>): boolean;
    /**
     * Create a new chat session via WSS and resolve with the chat UUID.
     *
     * Sends `{ action: 'create', scope, model? }` and waits for the
     * `session` message carrying `uuid`.
     */
    createSession(scope?: string, model?: string, timeoutMs?: number): Promise<string>;
    /**
     * Send a prompt to an existing chat session. The server will respond with
     * a stream of `message` frames and a final `done` frame.
     */
    sendPrompt(chatUuid: string, prompt: string, settings?: Record<string, unknown>): boolean;
    /** Close the connection and release all handlers. Idempotent. */
    close(): void;
    /** Register a handler for every incoming message. */
    onMessage(handler: MessageHandler): void;
    /** Register a one-shot handler for messages of a given `type`. */
    once(type: string, handler: MessageHandler): void;
    /** Listen for connection-open events. */
    onConnect(handler: ConnectionHandler): void;
    /** Listen for connection-close events. */
    onDisconnect(handler: ConnectionHandler): void;
    /** Listen for transport-level errors. */
    onError(handler: ErrorHandler): void;
    get readyState(): number;
    get isConnected(): boolean;
    /** The endpoint this socket is (or was last) connected to. */
    get currentEndpoint(): string | null;
    private dispatch;
    private startPing;
    private stopPing;
}

/**
 * Pagination metadata returned by list endpoints
 */
interface Pagination {
    limit: number;
    offset: number;
    total: number;
}
/**
 * OAuth provider configuration from /api/v1/settings
 */
interface OAuthProvider {
    name: string;
    client_id: string | null;
    scope: string | null;
    redirect_uri: string | null;
    bot_id: string | null;
    active: boolean;
}
/**
 * Application-wide settings
 */
interface AppSettings {
    oauth: OAuthProvider[];
    ai_list: Record<string, boolean>;
    client_ip: string;
    country_code: string;
}
/**
 * AI service entry (e.g. Midjourney, Sora, Flux)
 */
interface AIService {
    value: string;
    label: string;
    scope: string;
    active: boolean;
    description: string | null;
}
/**
 * Model settings (upload constraints, accepted types, etc.)
 */
interface AIModelSettings {
    uploadable?: boolean;
    max_file_size?: number;
    get_cost_params?: string[];
    max_frame_count?: number;
    attach_info_text?: string | null;
    file_count_limit?: number;
    hdr_video_support?: boolean;
    max_video_duration?: number;
    accepted_file_types?: string[];
    allowed_media_types?: string[];
    width?: number;
    height?: number;
    scale_factor?: number;
}
/**
 * Detailed AI model from /api/v1/ai/models
 */
interface AIModel {
    value: string;
    label: string;
    ai_name: string;
    active: boolean;
    default: boolean;
    description: string | null;
    type: string | null;
    settings: AIModelSettings;
    features: unknown | null;
}
/**
 * User profile from /api/v1/user.
 *
 * `UserInternal` is the raw wire shape including internal identifiers that
 * must never be exposed to MCP clients (e.g. `chatwoot_hmac`, `ym_client_id`
 * — see security advisory H2). The narrow `PublicUser` projection is the
 * only shape safe to surface through MCP tools/resources.
 */
interface UserInternal {
    id: number;
    user_id: number;
    created_at: number;
    name: string | null;
    username: string | null;
    email: string | null;
    avatar: string | null;
    auth_services: string[];
    ym_client_id: string | null;
    chatwoot_hmac: string | null;
}
/**
 * Public projection of a syntx.ai user profile — the only shape that should
 * be serialised to MCP clients. Internal identifiers (`chatwoot_hmac`,
 * `ym_client_id`, `created_at`) are intentionally excluded.
 */
interface PublicUser {
    id: number;
    user_id: number;
    name: string | null;
    username: string | null;
    email: string | null;
    avatar: string | null;
    auth_services: string[];
}
/**
 * @deprecated Use {@link UserInternal} for raw SDK responses and {@link PublicUser}
 *   for the sanitised projection. Retained as a type alias so existing callers
 *   continue to compile; new code must pick the appropriate concrete type.
 */
type User = UserInternal;
/**
 * Token balance from /api/v1/user/balance
 */
interface Balance {
    balance: number;
    user_id: string;
}
/**
 * Referral info nested in subscription
 */
interface ReferralInfo {
    link: string;
    token_balance: string;
    total_sales: number;
    sales_amount: {
        rub: number | null;
        usd: number | null;
        eur: number | null;
        xtr: number | null;
    };
}
/**
 * Active subscription from /api/v1/user/subscription
 */
interface Subscription {
    active: boolean;
    auto_renewal: boolean;
    type: string;
    gateway: string;
    tokens: string;
    canceled: string | null;
    start_date: string;
    end_date: string;
    refferal: ReferralInfo;
}
/**
 * Result of POST /api/v1/auth/startauth.
 * Server creates a pending auth session and returns its UUID.
 * The user must complete auth via the chosen provider (e.g. by pressing Start
 * in the Telegram bot at `https://telegram.me/<bot>?start=auth_<uuid>`).
 */
interface AuthStart {
    uuid: string;
}
/**
 * Result of GET /api/v1/auth/token/{uuid}.
 * - `valid: false, complete: false` — unknown / expired session
 * - `valid: true, complete: false`  — pending; user has not finished yet
 * - `valid: true, complete: true, token` — auth done, JWT included
 */
interface AuthTokenStatus {
    valid: boolean;
    complete: boolean;
    token?: string;
}
/**
 * Result of POST /api/v1/auth/email/send-otp.
 *
 * The server's exact response shape is not pinned by the public API; only the
 * "request accepted" status matters to the SDK. The shape is intentionally
 * loose so we can surface unexpected fields back to the caller (and to logs)
 * without a contract churn.
 */
interface EmailOtpSendResult {
    ok?: boolean;
    [key: string]: unknown;
}
/**
 * Result of POST /api/v1/auth/email/verify-otp.
 *
 * On success the server returns a JWT somewhere in this object — typically
 * as `token`. If the actual contract nests it (e.g. `data.token` or sets it
 * via a cookie) `token` will be `undefined` and the SDK will skip auto-install;
 * callers can still read the raw fields via `[key: string]: unknown`.
 */
interface EmailOtpVerifyResult {
    token?: string;
    [key: string]: unknown;
}
/**
 * Options accepted by the email-OTP auth methods.
 *
 * Both fields map directly onto the JSON body the syntx.ai API expects:
 *   `{ email, otp_code, ref_uuid?, utm? }`
 */
interface EmailOtpOptions {
    /** Referral UUID, forwarded as-is. Mirrors the `ref_uuid` field in `requests.js`. */
    ref_uuid?: string | null;
    /** UTM tag for attribution. Mirrors the `utm` field in `requests.js`. */
    utm?: string;
}
/**
 * Token response from Google's OAuth 2.0 token endpoint
 * (`POST https://oauth2.googleapis.com/token`) for the
 * Authorization Code + PKCE exchange (M3).
 */
interface GoogleTokenResponse {
    access_token: string;
    expires_in: number;
    token_type: string;
    scope?: string;
    refresh_token?: string;
    id_token?: string;
}
/**
 * User settings from /api/v1/user/settings
 */
interface UserSettings {
    settings: {
        user: unknown | null;
        readonly: unknown | null;
    };
    updated_at: string | null;
}
/**
 * Notification item
 */
interface Notification {
    id: string;
    title: string;
    body: string;
    read: boolean;
    created_at: string;
}
/**
 * Notifications response from /api/v1/notification/global
 */
interface NotificationsResponse {
    notifications: Notification[];
    pagination: Pagination;
}
/**
 * Unread count from /api/v1/notification/unread/count
 */
interface UnreadCount {
    count: number;
}
/**
 * Plan description block
 */
interface PlanCard {
    title: string;
    ai_includes: string;
    possibilities: string;
    possibilities_withoutInt?: string;
    possibilities_annual?: string;
    info_annual: string;
    info_monthly: string;
    tokenUsage_annual: string;
    tokenUsage_monthly: string;
    head?: string;
}
/**
 * Plans response wrapper
 */
interface PlansResponse {
    status: string;
    message: Record<string, PlanCard>;
}
/**
 * Folder item
 */
interface Folder {
    id: string;
    name: string;
    type: string;
    created_at: string;
}
/**
 * Chat item (from list endpoint)
 */
interface Chat {
    id: string;
    title: string | null;
    scope: string;
    created_at: string;
    updated_at: string;
    folder_id: string | null;
    model: string | null;
    pinned: boolean;
    /** Present in detailed / create response */
    uuid?: string;
    owner_id?: number;
    deleted?: boolean;
    is_favorite?: boolean;
    folder_uuids?: string[];
    message_count?: number;
    message_limit?: number;
}
/**
 * Message attachment
 */
interface MessageAttachment {
    id: string;
    type: string;
    url: string;
    name: string;
    size: number;
    mime_type: string;
}
/**
 * Single object inside a message (text, file, image, etc.)
 */
interface MessageObjectItem {
    id: number;
    message_id: number;
    object_type: string;
    object_url: string | null;
    object_text: string;
    completed: boolean;
    created_at: string;
    updated_at: string;
    model_type: string | null;
    metadata: unknown | null;
}
/**
 * Message in a chat (as returned by GET /api/v1/chats/{id}/messages)
 *
 * Note: The API returns `author_id` instead of `role`.
 * `author_id === -1` means assistant; user's own id means user message.
 * Content is inside `message_object[]`, not a flat `content` string.
 */
interface Message {
    id: string;
    chat_id: string;
    author_id: number;
    created_at: string;
    updated_at: string;
    is_favorite: boolean;
    /** Message content is an array of objects (text, filetext, image, etc.) */
    message_object: MessageObjectItem[];
}
/**
 * Messages list response
 */
interface MessagesResponse {
    messages: Message[];
    pagination: Pagination;
}
/**
 * Locale entry
 */
interface Locale {
    code: string;
    name: string;
    native_name: string;
    active: boolean;
}
/**
 * Promo banner
 */
interface PromoBanner {
    id: string;
    title: string;
    description: string;
    image_url: string;
    link: string;
    active: boolean;
}
/**
 * v2 model info response
 */
interface ModelInfoV2 {
    ai_name: string;
    model_type: string;
    info: unknown;
}
/**
 * Message object sent in chat messages
 */
interface MessageObject {
    object_type: string;
    object_url: string | null;
    object_text: string;
    model_type?: string;
}
/**
 * Design generation settings
 */
interface DesignSettings {
    n?: number;
    image_url?: string[];
    model_type?: string;
    resolution?: string;
    quality?: string;
    [key: string]: unknown;
}
/**
 * Audio generation settings (TTS, voice change, music, etc.).
 *
 * Mirrors the SPA's `audio.js:sendMessage` `settings` argument. The exact
 * field set is model-specific (see `list-models` scope=audio); only the
 * most common keys are typed, with an open index signature so callers can
 * pass provider-specific options without casts.
 */
interface AudioSettings {
    voice_id?: string;
    model_type?: string;
    /** Synthesis duration in seconds, when the model supports it. */
    duration?: number;
    /** Sample rate override in Hz (e.g. 22050, 44100). */
    sample_rate?: number;
    /** Music generation style / mood hint (e.g. "pop, sad, rainy night"). */
    prompt?: string;
    [key: string]: unknown;
}
/**
 * Video generation settings (e.g. wan_video, runway, kling).
 *
 * Mirrors the SPA's `video.js:sendMessage` `settings` argument. The SPA
 * rewrites `<<<url>>>` references inside the prompt into per-frame input
 * URLs; `wan_video` reads `settings.file_urls` for the same purpose. The
 * exact field set is model-specific (see `list-models` scope=video); only
 * the most common keys are typed, with an open index signature so callers
 * can pass provider-specific options without casts.
 */
interface VideoSettings {
    model_type?: string;
    /** Target duration in seconds. Most video models cap at 5–30 s.
     *
     * Note: some providers expose the field under a different name on the
     * wire. `grok_video` (any model: `grok_t2v`, `grok_i2v`, `grok_15_i2v`,
     * `grok_v2v`) requires `video_duration` (NOT `duration`) and accepts only
     * the literal values `"6"` or `"10"`. `kling_*` also reads `video_duration`.
     * Use the `[key: string]: unknown` index signature below to set
     * provider-specific keys without casts.
     */
    duration?: number;
    /** Output resolution. Most providers accept "1280x720" or "720x1280".
     *
     * `grok_video` is the exception — it requires the literal enum
     * `"480p"` or `"720p"`.
     */
    resolution?: string;
    /** Aspect ratio, e.g. "16:9", "9:16", "1:1". */
    aspect_ratio?: string;
    /** Frame rate override (e.g. 24, 30). */
    fps?: number;
    /** Quality preset (e.g. "low", "medium", "high"). */
    quality?: string;
    /** Input media URLs for image-to-video / video-to-video flows. */
    file_urls?: string[];
    /** Seed for deterministic sampling, when supported. */
    seed?: number;
    /** Provider-specific settings passthrough.
     *
     * Use this for fields the typed surface above does not expose. Common
     * examples:
     * - `grok_video`: `{ video_duration: 6, resolution: '720p' }`
     * - `kling_*`: `{ version: '1.6', mode: 'pro', native_audio: true }`
     * - `veo3`: `{ upscale: true }`
     *
     * Values are sent verbatim. Numeric values are not coerced.
     */
    [key: string]: unknown;
}
/**
 * URL-bearing object surfaced from a completed assistant reply.
 *
 * Populated from `message_object[]` entries whose `object_type` is one of
 * `image`, `video`, `audio`, or `file`. `object_text` for media objects is
 * typically empty (the URL is the payload) but is preserved verbatim so a
 * future "captioned image" generation can be surfaced without a contract
 * churn. `metadata` is the original `MessageObjectItem.metadata` value —
 * loosely typed, passed through unchanged.
 */
interface CompletedMedia {
    object_type: 'image' | 'video' | 'audio' | 'file';
    object_url: string;
    object_text: string;
    metadata: unknown | null;
}
/**
 * Final shape returned by {@link ChatsResource.waitForResponse} /
 * {@link ChatsResource.pollForResponse}.
 *
 * `text` is the concatenation of all `text` / `filetext` objects in the
 * completed reply (separated by `\n\n` when more than one); it may be the
 * empty string when the assistant turn was 100% media.
 *
 * `media` is the list of URL-bearing objects (`image`, `video`, `audio`,
 * `file`) with non-null `object_url`. Empty when the reply was text-only.
 *
 * `message` is the original wire `Message` so callers can still reach the
 * raw `message_object[]` / `created_at` / etc. without a second round-trip.
 */
interface CompletedMessage {
    text: string;
    media: CompletedMedia[];
    message: Message;
}
/**
 * In-progress status response
 */
/**
 * Item returned by the `/chats/{id}/inprogress` endpoint.
 * The endpoint returns an array of these; an empty array means nothing
 * is currently generating.
 */
interface InProgressItem {
    message_id: number;
    message_object_id: number;
    object_type: string;
    model_type: string;
    created_at: string;
    task_id: string | null;
    [key: string]: unknown;
}
/** Response from `/chats/{id}/inprogress` — an array of in-progress items. */
type InProgressResponse = InProgressItem[];
/**
 * Options for waitForResponse polling
 */
interface WaitForResponseOptions$1 {
    timeout?: number;
    /**
     * Polling ceiling in milliseconds. The poll loop is adaptive: it starts
     * near `0.4 × pollInterval` and backs off geometrically (×1.5) up to this
     * value, so quick replies are seen fast while long generations cost few
     * requests.
     *
     * **Breaking change in 0.3.0:** prior versions used a fixed delay
     * between polls. The option name and units are unchanged but the value
     * is now an upper bound on the (growing) interval, not a constant.
     */
    pollInterval?: number;
    boundary?: string;
    pageSize?: number;
    preWaitTimeout?: number;
    /**
     * Cancellation signal. Checked between poll ticks and during sleeps —
     * when aborted, the wait rejects promptly with `SyntxAbortError` instead
     * of polling until the timeout. MCP tools wire this to the request
     * cancellation signal so a disconnected client stops server-side polling.
     */
    signal?: AbortSignal;
    /**
     * Heartbeat fired once per poll tick with the elapsed time and total
     * budget. MCP tools forward this as `notifications/progress`, which lets
     * clients using `resetTimeoutOnProgress` keep the request alive through
     * long generations instead of dying with an MCP-layer timeout.
     */
    onProgress?: (elapsedMs: number, timeoutMs: number) => void;
    /**
     * Response strategy:
     *   - `'stream'` — open a WSS connection and consume token-by-token
     *   - `'poll'`   — fall back to REST polling (default pre-streaming behaviour)
     *   - `'auto'`   — try WSS first; fall back to polling on connect / protocol error
     */
    mode?: StreamMode$1;
    /**
     * Optional callback fired on each incremental chunk during streaming.
     * Receives the new fragment and the cumulative text assembled so far.
     */
    onChunk?: (chunk: string, accumulated: string) => void;
    /**
     * Override the WSS base URL (defaults to `wss://api.syntx.ai/api/v1`).
     */
    wsURL?: string;
    /**
     * Preferred language code for the WSS endpoint (defaults to `'en'`).
     */
    lang?: string;
    /**
     * Max time to wait on the SSE stream before falling back to REST polling.
     * Defaults to `60% × timeout` so polling always gets at least 40 % of the
     * budget. Applies to text-flow (`llm/*`) waits only.
     */
    sseTimeoutMs?: number;
}
/**
 * Strategy for receiving an assistant reply.
 *  - `stream` — real-time via WebSocket (recommended)
 *  - `poll`   — periodic REST polling (legacy, robust on weak networks)
 *  - `auto`   — try stream, fall back to poll
 */
type StreamMode$1 = 'stream' | 'poll' | 'auto';
/**
 * Options accepted by {@link ChatsResource.streamResponse}.
 */
interface StreamResponseOptions {
    /**
     * Total wall-clock budget in milliseconds. Resolved/rejected when exceeded.
     * Default 600000 (10 minutes).
     */
    timeout?: number;
    /**
     * Override the WSS base URL.
     */
    wsURL?: string;
    /**
     * Preferred language code passed to the WSS endpoint.
     */
    lang?: string;
    /**
     * Per-chunk callback. Called with the raw delta and the cumulative text.
     */
    onChunk?: (chunk: string, accumulated: string) => void;
    /**
     * Per-message callback. Called once per complete server `message` frame
     * (in addition to onChunk). Useful for callers that need the full frame
     * metadata (model_type, etc.).
     */
    onMessage?: (msg: StreamingMessage) => void;
    /**
     * Provider (AI service) name to route the prompt to, e.g. `'gemini'`,
     * `'chatgpt'`, `'claude'`. Forwarded to the REST `sendMessage` call so
     * the server picks the right backend.
     */
    aiName?: string;
    /**
     * Fired exactly once when the chat has been created and its UUID is known.
     * Lets callers capture the UUID for follow-up messages or polling.
     */
    onSession?: (chatUuid: string) => void;
    /**
     * Cancellation signal honoured by the internal poll loop — see
     * {@link WaitForResponseOptions.signal}.
     */
    signal?: AbortSignal;
    /**
     * Heartbeat fired once per poll tick while waiting for the reply — see
     * {@link WaitForResponseOptions.onProgress}.
     */
    onProgress?: (elapsedMs: number, timeoutMs: number) => void;
}
/**
 * Final result of a streamed response.
 */
interface StreamResponseResult {
    /** Full assistant text (concatenation of all chunks). */
    text: string;
    /**
     * Last `message` frame received from the server. May carry metadata
     * (model_type, usage, etc.). `null` if no message was received.
     */
    message: StreamingMessage | null;
    /** Time elapsed between prompt submission and completion, in milliseconds. */
    elapsedMs: number;
    /**
     * Chat UUID of the session. Use it for follow-up `send-message` /
     * `wait-for-response` calls.
     */
    chatUuid?: string;
}
/**
 * Voice example item
 */
interface VoiceExample {
    id: string;
    name: string;
    url: string;
    [key: string]: unknown;
}
/**
 * Voice examples paginated response
 */
interface VoiceExamplesResponse {
    items: VoiceExample[];
    total: number;
    page: number;
    page_size: number;
}
/**
 * Frontend app version info from /version.json
 */
interface VersionInfo {
    version: string;
    [key: string]: unknown;
}
/**
 * Maintenance status from /maintenance-status.json
 */
interface MaintenanceStatus {
    maintenance: boolean;
    message?: string;
    [key: string]: unknown;
}
/**
 * One LLM-usage quota window (6h or 7d).
 *
 * `expires_at === null` OR `expires_at` already in the past means the window
 * is not currently active; the SDK normalises that shape to
 * `{ percent_left: 100, started_at: null, expires_at: null }` so consumers
 * never need to re-implement the rule.
 */
interface LlmLimitWindow {
    percent_left: number;
    started_at: string | null;
    expires_at: string | null;
}
/** Raw window shape as returned by `GET /api/v1/llm/limits`. */
interface RawLlmLimitWindow {
    percent_left?: number;
    started_at?: string | null;
    expires_at?: string | null;
}
/**
 * LLM-usage limits from `GET /api/v1/llm/limits`.
 *
 * Either window may be `null` when the user/model has no such limit.
 */
interface LlmLimits {
    window_6h: LlmLimitWindow | null;
    window_7d: LlmLimitWindow | null;
}
/**
 * Response from `POST /api/v1/llm/generate?ai_name=…`. The server returns
 * a job identifier and a relative `stream_url` (joined with `llmSseBaseUrl`
 * from {@link McpServerConfig}) plus the assistant message id.
 */
interface LlmGenerateResponse {
    job_id: string;
    stream_url: string;
    message_id: string;
}
/** One in-flight stream job as returned by `GET /api/v1/llm/chats/{id}/stream`. */
interface LlmStreamJob {
    message_id: string;
    stream_url: string;
}
/**
 * Text-scope LLM model from `GET /api/v1/llm/models`. Only the text-scope
 * subset is typed here; other scopes return raw passthrough via
 * `[key: string]: unknown`.
 */
interface LlmModel {
    value: string;
    label: string;
    ai_name: string;
    scope: 'text';
    active: boolean;
    description?: string | null;
    [key: string]: unknown;
}
/**
 * Parameters accepted by {@link LlmResource.generate}. Mirrors the
 * `POST /api/v1/llm/generate` body shape captured from live SPA traffic
 * (syntx.ai browser DevTools, 2026-09-21):
 *
 *   body: {
 *     chat_uuid?: string,
 *     text: string,
 *     model: string,
 *     thinking?: boolean,
 *     plan?: boolean,
 *     deep_research?: boolean,
 *     tools?: string[],
 *   }
 *   query: ai_name=…
 *
 * Note: the chat-binding field is `chat_uuid`, NOT `chat_id`. Sending
 * `chat_id` returns 422 from the server. Earlier SPA bundles that sent
 * `{ objects, chat_id, model_type }` are no longer compatible.
 */
interface LlmGenerateParams {
    prompt: string;
    aiName: string;
    modelType?: string;
    chatUuid?: string;
    thinking?: boolean;
    plan?: boolean;
    deepResearch?: boolean;
    tools?: string[];
}
/** Filters accepted by {@link LlmResource.listModels}. */
interface LlmListModelsParams {
    enabled_only?: boolean;
    lang?: string;
}

/**
 * Authentication module for syntx.ai.
 *
 * Supports multiple OAuth providers (Telegram, Google, Email).
 * The exact token mechanism may vary; this module provides the
 * standard Bearer token flow and placeholders for OAuth redirects.
 */
declare class SyntxAuth {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Set the API token directly (e.g. after obtaining it via OAuth).
     */
    setToken(token: string): void;
    /**
     * Get the current token.
     */
    getToken(): string | undefined;
    /**
     * Check if a token is set.
     */
    isAuthenticated(): boolean;
    /**
     * Clear the current token.
     */
    logout(): void;
    /**
     * Placeholder: Initiate Telegram OAuth login.
     * In a browser, this typically opens a Telegram login widget popup
     * or redirects to the Telegram OAuth page.
     */
    getTelegramLoginUrl(redirectUri?: string, botId?: string): string;
    /**
     * Generate a PKCE key pair (RFC 7636): a high-entropy `code_verifier`
     * and its `S256` `code_challenge`. Use the challenge when building the
     * authorization URL ({@link getGoogleLoginUrl}) and keep the verifier
     * secret until {@link exchangeGoogleCode}.
     */
    static generatePkcePair(): {
        codeVerifier: string;
        codeChallenge: string;
    };
    /**
     * Build a Google OAuth 2.0 authorization URL using the
     * **Authorization Code + PKCE** flow (M3, v0.3.0).
     *
     * The legacy Implicit Grant (`response_type=token`) is deprecated by
     * OAuth 2.0 Security Best Current Practice — access tokens must no longer
     * travel through the browser front-channel. Pass the `codeChallenge` from
     * {@link generatePkcePair} and, after the redirect, call
     * {@link exchangeGoogleCode} with the matching verifier.
     *
     * @param clientId  Google OAuth client ID (public / installed-app client).
     * @param redirectUri  Registered redirect URI.
     * @param options.state  Optional CSRF token round-tripped through Google.
     * @param options.codeChallenge  PKCE S256 challenge. REQUIRED in practice —
     *   omit only when talking to a legacy authorization server; Google
     *   accepts the flow without it but you lose the interception defence.
     */
    getGoogleLoginUrl(clientId: string, redirectUri: string, options?: {
        state?: string;
        codeChallenge?: string;
    }): string;
    /**
     * Exchange an authorization `code` (plus the PKCE `codeVerifier`) for
     * tokens at Google's token endpoint. Intended for public clients
     * (installed apps / CLI tools) where no `client_secret` exists — the
     * verifier proves continuity with the original authorization request.
     *
     * This call bypasses the syntx.ai API base URL on purpose: it talks to
     * `https://oauth2.googleapis.com/token` directly. If your deployment
     * instead routes the exchange through a syntx.ai endpoint that accepts
     * `code_verifier`, prefer that endpoint and keep this method for local
     * development.
     *
     * Throws `SyntxAuthError` when Google rejects the exchange.
     */
    exchangeGoogleCode(options: {
        clientId: string;
        redirectUri: string;
        code: string;
        codeVerifier: string;
    }): Promise<GoogleTokenResponse>;
    /**
     * Request an OTP code to be mailed to `email`.
     *
     * Calls `POST /api/v1/auth/email/send-otp` with `{ email, ref_uuid, utm }`.
     * Does NOT install a token — this method only kicks off delivery. Pair
     * with {@link verifyEmailOtp} (or {@link loginWithEmail} for the
     * callback-driven one-shot) once the user has read the code from their
     * inbox.
     *
     * Throws `SyntxAPIError` on transport / 4xx / 5xx failures.
     */
    sendEmailOtp(email: string, options?: EmailOtpOptions): Promise<EmailOtpSendResult>;
    /**
     * Exchange an OTP code for a JWT bearer token.
     *
     * Calls `POST /api/v1/auth/email/verify-otp` with
     * `{ email, otp_code, ref_uuid, utm }`. When the response carries a `token`
     * field, it is installed as the active bearer via {@link setToken} so the
     * caller can immediately use authenticated endpoints.
     *
     * Pass `options.install === false` to peek at the response without
     * committing the token to the in-process bearer store (useful for UIs that
     * want to confirm before swapping identity).
     *
     * If the server returns the JWT under a different key, `token` will be
     * `undefined` and the SDK will not install anything — the caller can
     * inspect the raw fields via the returned `EmailOtpVerifyResult` and call
     * {@link setToken} manually.
     */
    verifyEmailOtp(email: string, otpCode: string, options?: EmailOtpOptions & {
        install?: boolean;
    }): Promise<EmailOtpVerifyResult>;
    /**
     * One-shot email-OTP login.
     *
     * Convenience wrapper around {@link sendEmailOtp} + {@link verifyEmailOtp}.
     * Use this when you have a way to obtain the OTP from the user without
     * surfacing a separate tool call (e.g. an interactive CLI prompt).
     *
     * The OTP must be supplied by `options.otpProvider` — an async callback
     * that resolves with the code the user read from their inbox. This avoids
     * blocking forever: if no provider is given, the method throws
     * `SyntxAuthError` immediately.
     */
    loginWithEmail(email: string, options?: EmailOtpOptions & {
        otpProvider?: () => Promise<string>;
    }): Promise<EmailOtpVerifyResult>;
    /**
     * Validate the current token by calling /api/v1/user.
     * Throws SyntxAuthError if no token is set.
     */
    validateToken(): Promise<boolean>;
    /**
     * Start an authentication session on syntx.ai.
     *
     * Calls `POST /api/v1/auth/startauth` and returns the session UUID.
     * The user must then complete the auth flow through one of the supported
     * providers — for Telegram this means opening the deep-link returned by
     * {@link getTelegramAuthLink} and pressing Start in the bot.
     *
     * Pair with {@link pollAuthToken} (or {@link loginWithTelegram} for the
     * all-in-one flow) to obtain the JWT bearer token.
     */
    startAuth(): Promise<AuthStart>;
    /**
     * Poll the status of an auth session.
     *
     * Calls `GET /api/v1/auth/token/{uuid}`. Typical responses:
     * - `{ valid: false, complete: false }` — unknown/expired UUID
     * - `{ valid: true,  complete: false }` — waiting for the user
     * - `{ valid: true,  complete: true, token }` — auth finished, JWT present
     *
     * Does NOT mutate the local token — call {@link setToken} once `complete`
     * becomes true.
     */
    pollAuthToken(uuid: string): Promise<AuthTokenStatus>;
    /**
     * Build a `t.me` deep-link that opens the syntx.ai Telegram bot with a
     * pre-filled `start` payload. When the user presses Start, the bot
     * receives `auth_<uuid>` and binds the session to the user's Telegram
     * identity, which unblocks the polling endpoint.
     *
     * `botUsername` defaults to `syntxaibot` (the public bot used by
     * syntx.ai). Override only if you are pointing at a custom bot.
     */
    getTelegramAuthLink(uuid: string, botUsername?: string): string;
    /**
     * Full Telegram device-auth flow:
     * 1. Create a session via {@link startAuth}.
     * 2. Return the bot deep-link — the caller is expected to open it
     *    (browser tab, `open()` from a UI, or hand it to the user).
     * 3. Poll `GET /api/v1/auth/token/{uuid}` every `pollIntervalMs`
     *    until `complete === true` or `valid === false`.
     * 4. Persist the JWT via {@link setToken}.
     *
     * The returned object is intentionally explicit so callers can render
     * the link separately from the polling loop and decide what to do on
     * cancellation / timeout.
     */
    loginWithTelegram(options?: {
        botUsername?: string;
        pollIntervalMs?: number;
        timeoutMs?: number;
        /** Called every poll with the latest status — useful for UIs. */
        onPoll?: (status: AuthTokenStatus, elapsedMs: number) => void;
        /** Called once when the bot link is ready — receives the deep-link. */
        onLink?: (deepLink: string, uuid: string) => void;
    }): Promise<{
        uuid: string;
        deepLink: string;
        token: string;
        status: AuthTokenStatus;
        elapsedMs: number;
    }>;
}

interface GetModelInfoParams {
    ai_name: string;
    model_type: string;
    batch_size?: number;
    quality?: string;
    /** Some providers validate this as a string enum (e.g. sora: "4"…"25"), others as a number. */
    video_duration?: number | string;
    chars_count?: number;
    mode?: string;
    [key: string]: string | number | boolean | undefined;
}
/**
 * Resource for AI services and models.
 */
declare class AIResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * List all available AI services (e.g. Midjourney, Sora, Flux).
     * GET /api/v1/ai
     */
    listServices(): Promise<AIService[]>;
    /**
     * List detailed AI models with upload constraints and features.
     * GET /api/v1/ai/models
     */
    listModels(): Promise<AIModel[]>;
    /**
     * Get detailed info about a specific model (v2 endpoint).
     * GET /api/v2/get_model_info
     */
    getModelInfo(params: GetModelInfoParams): Promise<ModelInfoV2>;
}

/**
 * Resource for user profile, balance, subscription and settings.
 */
declare class UserResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Get current user profile (raw internal shape).
     * GET /api/v1/user
     *
     * The returned object includes internal identifiers (`chatwoot_hmac`,
     * `ym_client_id`) that must never be exposed via MCP tool surfaces.
     * Prefer {@link mePublic} for MCP-facing code.
     */
    me(): Promise<UserInternal>;
    /**
     * Public projection of the current user profile — safe to expose via MCP.
     *
     * Strips internal identifiers (`chatwoot_hmac`, `ym_client_id`) before
     * returning. The underlying network call still fetches the full payload;
     * the projection happens on the SDK boundary so callers downstream never
     * see the sensitive fields even if a future feature forgets to scrub.
     */
    mePublic(): Promise<PublicUser>;
    /**
     * Get token balance.
     * GET /api/v1/user/balance
     */
    getBalance(): Promise<Balance>;
    /**
     * Get active subscription details.
     * GET /api/v1/user/subscription
     */
    getSubscription(): Promise<Subscription>;
    /**
     * Get user-specific settings.
     * GET /api/v1/user/settings
     */
    getSettings(): Promise<UserSettings>;
}
/**
 * Project a raw internal user payload onto the public shape.
 * Exported so MCP modules that already have a `UserInternal` in hand (e.g.
 * after calling `me()` directly) can sanitise without a second fetch.
 */
declare function toPublicUser(user: UserInternal): PublicUser;

/**
 * Pure projection from a wire {@link Message} to the curated reply shape
 * returned by {@link ChatsResource.waitForResponse}.
 *
 * Readiness rule: `ready === true` iff every `message_object[i]` has
 * `completed === true` AND the message contains at least one object. This
 * replaces the previous single-object check (`message_object[0].completed &&
 * object_text non-empty`), which silently hung on image / video / audio /
 * file-only replies and returned early on multi-object text+media replies.
 *
 * `text` collects `object_type === 'text' | 'filetext'` objects joined with
 * `\n\n` when there is more than one such object (single-object replies stay
 * unseparated). `media` collects URL-bearing objects whose `object_type` is
 * one of `image`, `video`, `audio`, `file`; `metadata` is passed through
 * verbatim (no clone — `unknown` may hold non-serializable values).
 */
declare function collectCompletedObjects(message: Message): {
    text: string;
    media: CompletedMedia[];
    ready: boolean;
};
interface ListChatsParams {
    scope?: string;
    search?: string;
    direction?: 'older' | 'newer';
    page_size?: number;
    [key: string]: string | number | boolean | undefined;
}
interface ListMessagesParams {
    page_size?: number;
    direction?: 'older' | 'newer';
    [key: string]: string | number | boolean | undefined;
}
interface CreateChatParams {
    scope?: string;
    title?: string;
    model?: string;
}
interface SendMessageParams {
    chat_uuid: string;
    prompt: string;
    settings?: Record<string, unknown>;
    attachments?: unknown[];
}
interface SendChatMessageParams {
    message_object: MessageObject;
}
interface UploadResult {
    files: Array<{
        url: string;
        filename: string;
        size: number;
        mime_type: string;
    }>;
}
/**
 * One input item accepted by {@link ChatsResource.uploadFiles}.
 *
 * `Blob` works in both browser and Node 18+ globals. The plain-object form
 * avoids requiring the global `File` constructor (available only from
 * Node 20), keeping the SDK compatible with the package's `engines.node>=18`.
 */
type UploadFileInput = Blob | {
    buffer: Uint8Array;
    filename: string;
    mimeType?: string;
};
/**
 * Resource for chats and messages.
 * Supports both REST API and WebSocket real-time messaging.
 */
declare class ChatsResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * List user chats.
     * GET /api/v1/chats
     */
    list(params?: ListChatsParams): Promise<{
        chats: Chat[];
        pagination: Pagination;
    }>;
    /**
     * Get messages for a specific chat.
     * GET /api/v1/chats/{chatId}/messages
     */
    getMessages(chatId: string, params?: ListMessagesParams): Promise<MessagesResponse>;
    /**
     * Get favorite messages for a specific chat.
     * GET /api/v1/chats/favorite/{chatId}/messages
     */
    getFavoriteMessages(chatId: string, params?: ListMessagesParams): Promise<MessagesResponse>;
    /**
     * Create a new chat/session.
     * POST /api/v1/chats
     *
     * Note: the API requires at least `title` to be present, otherwise
     * it returns 422 Unprocessable Entity.
     */
    create(data?: CreateChatParams): Promise<{
        id: number;
        uuid: string;
        title: string;
        scope: string;
        created_at: string;
        updated_at: string;
        owner_id: number;
        deleted: boolean;
        is_favorite: boolean;
        folder_uuids: string[];
        messages: unknown[];
        message_count: number;
        message_limit: number;
    }>;
    /**
     * Send a message (or multiple objects) to a chat.
     * POST /api/v1/chats/{chatId}/messages?ai_name={aiName}
     *
     * The real API expects `{ objects: MessageObject[] }`.
     * Each object can have object_type "text", "filetext", "image", etc.
     */
    sendMessage(chatId: string, aiName: string, objects: MessageObject[]): Promise<unknown>;
    /**
     * Check if a chat has in-progress operations.
     * GET /api/v1/chats/{chatId}/inprogress
     */
    getInProgress(chatId: string): Promise<InProgressResponse>;
    /**
     * Get the largest numeric message id from the chat, skipping assistant
     * (`author_id === -1`) messages. Returned as a string for backward
     * compatibility with the previous timestamp-typed boundary; the polling
     * filter compares it as a number.
     *
     * Why ids and not `created_at`: the live API (observed 2026-09-21) sometimes
     * records the assistant reply placeholder **before** the user prompt —
     * `created_at(assistant) < created_at(user)` by ~10–15 ms — so a
     * `max(created_at)` boundary silently misses the reply. Message ids are
     * monotonically assigned by the server at request intake, so the latest
     * non-assistant id is always strictly less than the next assistant id we
     * are waiting for.
     *
     * Returns `'0'` for empty chats / on API error so the polling filter
     * falls through to "any assistant message is new".
     */
    getLatestBoundary(chatId: string): Promise<string>;
    /**
     * Receive an assistant reply for an existing chat.
     *
     * Polls the REST endpoint until a new completed message appears, optionally
     * bounded by a `created_at` boundary to ignore stale messages from previous
     * requests.
     *
     * For real-time token-by-token delivery (without first creating a chat via
     * REST), use {@link ChatsResource.streamResponse} instead.
     */
    waitForResponse(chatId: string, options?: WaitForResponseOptions$1): Promise<CompletedMessage>;
    /**
     * Stream a reply from the syntx.ai API (REST-polling based).
     *
     * The syntx.ai API does not expose a WebSocket or SSE endpoint. The
     * assistant reply is generated asynchronously and only appears (in full)
     * once the model finishes. This method provides a streaming-compatible
     * interface on top of REST polling:
     *
     *  1. Creates a chat via REST.
     *  2. Sends the prompt via REST (`POST /chats/{uuid}/messages`).
     *  3. Fires {@link StreamResponseOptions.onSession} with the chat UUID.
     *  4. Polls the messages endpoint until the assistant reply appears.
     *  5. Fires {@link StreamResponseOptions.onChunk} with the complete text
     *     (the API delivers it atomically — there is no incremental growth).
     *  6. Resolves with the full result, including `chatUuid` for follow-ups.
     *
     * @param prompt - The user prompt text.
     * @param options - Streaming options. `scope`, `model`, and `aiName`
     *   control chat/message creation. `timeout` bounds the poll loop.
     */
    streamResponse(prompt: string, options?: StreamResponseOptions & {
        scope?: string;
        model?: string;
    }): Promise<StreamResponseResult>;
    /**
     * Poll a chat until a new assistant message is completed.
     * Uses `created_at` boundary to ignore messages from previous requests.
     *
     * Behaviour notes:
     *  - **Adaptive interval** — the first tick fires at ~`0.4 × pollInterval`
     *    and backs off geometrically (×1.5) up to `pollInterval`, so quick
     *    replies surface fast and long generations cost few requests.
     *  - **Single budget** — the in-progress pre-wait and the reply poll share
     *    one wall-clock budget (`timeout`); previously the pre-wait came on
     *    top, doubling the worst case.
     *  - **Heartbeat** — `onProgress(elapsed, timeout)` fires once per tick;
     *    MCP tools forward it as `notifications/progress` so clients with
     *    `resetTimeoutOnProgress` survive long generations.
     *  - **Cancellation** — `signal` aborts the wait promptly with
     *    {@link SyntxAbortError} (never counted as a poll error).
     *  - **Timeout** — rejects with {@link SyntxTimeoutError} carrying the
     *    chatId so callers can recover the reply later instead of re-sending.
     */
    pollForResponse(chatId: string, options?: WaitForResponseOptions$1): Promise<CompletedMessage>;
    /**
     * Get a specific message by ID.
     * GET /api/v1/chats/{chatId}/{messageId}
     */
    getMessage(chatId: string, messageId: string): Promise<unknown>;
    /**
     * Upload files to a chat.
     * POST /api/v1/chats/upload-files
     *
     * Accepts {@link UploadFileInput} — either a `Blob` (browser & Node 18+)
     * or a plain object describing a `Uint8Array` with a filename. Plain-object
     * form is the recommended cross-environment input.
     *
     * `modelType` mirrors the SPA's per-upload form field
     * (`settings.model_type ?? ""`). The server uses it to scope the upload
     * to a model; the SDK previously omitted the field entirely.
     */
    uploadFiles(files: UploadFileInput[], destination?: 'hidden', checkDuplicates?: boolean, modelType?: string): Promise<UploadResult>;
    /**
     * Delete a file.
     * DELETE /api/v1/files/delete
     *
     * The SPA accepts either `{ file_id }` or `{ url }` as the body. Pass a
     * string to delete by id (the historical SDK behaviour), or a `{ url }`
     * object to delete by the uploaded R2 URL.
     */
    deleteFile(target: string | {
        url: string;
    }): Promise<void>;
    /**
     * Get uploaded files.
     * GET /api/v1/files/uploaded
     */
    getUploadedFiles(scope?: string, page?: number, pageSize?: number): Promise<{
        items: Array<{
            url: string;
            filename: string;
            size: number;
            created_at: string;
        }>;
        total: number;
        page: number;
        pageSize: number;
    }>;
    /**
     * Transcribe audio to text.
     * POST /api/v1/audio/transcribe
     */
    transcribe(file: File): Promise<{
        text: string;
    }>;
    /**
     * Generate a session title using AI.
     * POST /api/v1/chats/by-uuid/{chatUuid}/generate-title
     */
    generateTitle(chatUuid: string): Promise<void>;
    /**
     * Fetch a single chat's metadata by id or uuid.
     *
     * `GET /api/v1/chats/{chatId}` → `200 Chat` if the chat exists,
     * `404 {"detail":"Chat not found"}` otherwise (the latter surfaces as
     * `SyntxAPIError { status: 404 }`).
     *
     * The server accepts both numeric ids (`20872358`) and uuids
     * (`968e99a3-…`) in the path. Useful as a pre-flight existence check
     * before `sendMessage` / `streamMessage` when the caller may be holding
     * a stale reference (e.g. a soft-deleted chat that no longer appears
     * in `list` but is still accessible for read/write).
     */
    get(chatId: string): Promise<Chat>;
    /**
     * Lightweight existence probe. Returns `true` iff `GET /api/v1/chats/{chatId}`
     * responds `200`. Any non-2xx response (404 in particular) returns `false`.
     * Other errors (network, 5xx) propagate so the caller can distinguish
     * "definitely missing" from "could not tell".
     */
    exists(chatId: string): Promise<boolean>;
    /**
     * Delete a chat.
     * DELETE /api/v1/chats/{chatId}
     */
    delete(chatId: string): Promise<void>;
    /**
     * Pin/unpin a chat.
     * POST /api/v1/chats/{chatId}/pin
     */
    pin(chatId: string): Promise<void>;
    /**
     * Move chat to folder.
     * POST /api/v1/chats/{chatId}/move
     */
    moveToFolder(chatId: string, folderId: string): Promise<void>;
    /**
     * Rename a chat.
     *
     * `PUT /api/v1/chats/{chatId}` with body `{title}`.
     *
     * Mirrors the syntx.ai sessions store (SPA app bundle:
     * `fe.put(\`chats/${h}\`,{title:c})`). The response body is ignored by
     * the SPA; the raw upstream response is passed through unpinned.
     */
    rename(chatId: string, title: string): Promise<unknown>;
    /**
     * Toggle the favorite (bookmark) flag of a chat.
     *
     * `POST /api/v1/chats/{chatId}/favorite` — no request body.
     *
     * Mirrors the syntx.ai sessions store (SPA app bundle:
     * `fe.post(\`chats/${h}/favorite\`)`). The endpoint is a **toggle**:
     * every call flips `chat.is_favorite`. The response body is ignored by
     * the SPA; verify the resulting state via `list`.
     */
    toggleFavorite(chatId: string): Promise<unknown>;
    /**
     * Toggle the favorite (bookmark) flag of a single message.
     *
     * `POST /api/v1/chats/{chatId}/messages/{messageId}/favorite` — no
     * request body.
     *
     * Mirrors the syntx.ai SPA (app bundle:
     * `G.post(\`chats/${k}/messages/${h}/favorite\`)`). The endpoint is a
     * **toggle**: every call flips `message.is_favorite`. The response body
     * is ignored by the SPA; favorites are readable via
     * `getFavoriteMessages`.
     */
    toggleMessageFavorite(chatId: string, messageId: string): Promise<unknown>;
    /**
     * Delete a single message.
     *
     * `DELETE /api/v1/chats/messages/{messageId}` — note the path has **no
     * chatId segment** (the only message-deletion shape observed across the
     * SPA bundles: `G.delete(\`chats/messages/${h}\`)`; ids are numeric in
     * the SPA, strings here).
     *
     * Unlike {@link cancelMessage}, a 404 is not swallowed — it propagates
     * as `SyntxAPIError { status: 404 }`.
     */
    deleteMessage(messageId: string): Promise<void>;
    /**
     * Cancel an in-flight message generation.
     *
     * `POST /api/v1/chats/{chatId}/messages/{messageId}/cancel`
     *
     * The endpoint is idempotent in practice: a 404 means the message has
     * already finished (race with completion) and is treated as success so
     * caller code doesn't have to special-case it.
     */
    cancelMessage(chatId: string, messageId: string): Promise<void>;
}

/**
 * Resource for subscription plans and promo banners.
 */
declare class PlansResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Get all available plans with detailed descriptions.
     * GET /api/v1/plans/card_plans
     */
    list(lang?: string): Promise<PlansResponse>;
    /**
     * Get promo banners for a specific language.
     * GET /api/v1/promo_banners
     */
    getPromoBanners(lang?: string): Promise<PromoBanner[]>;
}

interface ListNotificationsParams {
    limit?: number;
    offset?: number;
    [key: string]: string | number | boolean | undefined;
}
/**
 * Resource for notifications.
 */
declare class NotificationsResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Get global notifications with pagination.
     * GET /api/v1/notification/global
     */
    list(params?: ListNotificationsParams): Promise<NotificationsResponse>;
    /**
     * Get unread notifications count.
     * GET /api/v1/notification/unread/count
     */
    getUnreadCount(): Promise<{
        count: number;
    }>;
    /**
     * Mark a single notification as read.
     * PATCH /api/v1/notification/mark/global/{id}
     *
     * The previous SDK implementation targeted `PATCH /api/v1/notifications/{id}/read`
     * but that endpoint returned 404 against api.syntx.ai. The SPA-observed
     * path (`notification/mark/global/{id}`) returns 403 (auth-gated) — same
     * pattern as the other working endpoints — and is therefore authoritative.
     */
    markAsRead(id: string): Promise<void>;
    /**
     * Mark every notification as read.
     * PATCH /api/v1/notification/mark/all
     *
     * The SPA fires this from `notification.js:markAllRead` whenever the
     * "mark all" UI action is invoked. No body is required.
     */
    markAll(): Promise<void>;
}

/**
 * Input accepted by {@link FoldersResource.create}.
 *
 * Mirrors the JSON body of `POST /api/v1/folders/create` observed in the
 * captured traffic. `title` is required; the server enforces non-empty
 * input and returns 422 otherwise.
 */
interface CreateFolderParams {
    title: string;
    scope?: string;
    color?: string;
    chat_uuids?: string[];
}
/**
 * Server response from `POST /api/v1/folders/create`.
 *
 * The wire shape is loose — only `uuid` is consistently observed, with the
 * remaining fields forwarded from upstream. Additional unknown fields are
 * preserved through the `unknown` passthrough below.
 */
interface CreatedFolder {
    uuid: string;
    title?: string;
    scope?: string;
    color?: string;
    chats?: unknown[];
    [key: string]: unknown;
}
/**
 * Resource for folders (chat organization).
 */
declare class FoldersResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * List text folders.
     * GET /api/v1/folders/text/list
     */
    listTextFolders(): Promise<Folder[]>;
    /**
     * List image folders.
     * GET /api/v1/folders/image/list
     */
    listImageFolders(): Promise<Folder[]>;
    /**
     * List video folders.
     * GET /api/v1/folders/video/list
     */
    listVideoFolders(): Promise<Folder[]>;
    /**
     * List audio folders.
     * GET /api/v1/folders/audio/list
     */
    listAudioFolders(): Promise<Folder[]>;
    /**
     * Create a folder (project) on syntx.ai.
     *
     * `POST /api/v1/folders/create`
     *
     * The server expects `{ title, scope, color, chat_uuids }` exactly; missing
     * `scope`/`color`/`chat_uuids` are filled with the same defaults the web
     * client uses (`text`, `#9C9C9C`, `[]`). Pass `chat_uuids` to seed the
     * folder with existing chats.
     */
    create(data: CreateFolderParams): Promise<CreatedFolder>;
    /**
     * Add one or more existing chats to a folder (project).
     *
     * `POST /api/v1/folders/{folderUuid}/add`
     *
     * The server consumes the body as a bare JSON array of chat UUIDs — the
     * SDK therefore serialises the array as-is, matching the captured request
     * payload. Returns the upstream response unchanged; the wire shape is not
     * pinned by the public docs.
     */
    addChats(folderUuid: string, chatUuids: string[]): Promise<unknown>;
    /**
     * Remove one or more chats from a folder (project).
     *
     * `POST /api/v1/folders/{folderUuid}/remove`
     *
     * Inverse of {@link addChats}: the server consumes the body as a bare
     * JSON array of chat UUIDs (captured `ai-folders` Pinia store call:
     * `qn.post(\`/folders/${_}/remove\`,S)` with `S=[]` default). Returns
     * the upstream response unchanged; the wire shape is not pinned.
     */
    removeChats(folderUuid: string, chatUuids: string[]): Promise<unknown>;
    /**
     * Update a folder's title and/or color.
     *
     * `PATCH /api/v1/folders/{folderUuid}/change`
     *
     * Mirrors the `updateFolder(uuid,{title,color})` action in the captured
     * `ai-folders` Pinia store: only the provided keys are serialized. The
     * SPA merges the response into the local folder object; the SDK
     * surfaces the raw response without pinning its schema.
     *
     * Contract: values are forwarded **verbatim** — empty strings are sent
     * as-is. The MCP tool `update-project` rejects empty `title`/`color`
     * before reaching this method; direct SDK callers must validate
     * themselves.
     */
    update(folderUuid: string, data: {
        title?: string;
        color?: string;
    }): Promise<unknown>;
    /**
     * Reorder a folder within its scope.
     *
     * `PATCH /api/v1/folders/{folderUuid}/move` with body
     * `{after_uuid: string | null}`.
     *
     * Mirrors the captured `ai-folders` Pinia store call
     * (`qn.patch(\`/folders/${_}/move\`,{after_uuid:S})`): pass the uuid of
     * the folder to position after, or `null` to move to the top. The
     * response has been observed to contain `sort_order`; the raw response
     * is passed through unpinned.
     */
    move(folderUuid: string, afterUuid: string | null): Promise<unknown>;
    /**
     * Permanently delete a folder (project).
     *
     * `DELETE /api/v1/folders/{folderUuid}/delete`
     *
     * Matches the endpoint the syntx.ai web client uses (see the captured
     * `ai-folders` Pinia store: `zr.delete(\`/folders/${R}/delete\`)`). The
     * server returns `{ success: boolean, ... }`; the SDK surfaces the raw
     * response without inventing an unverified schema.
     */
    delete(folderUuid: string): Promise<unknown>;
}
/**
 * Resource for application settings and localizations.
 */
declare class SettingsResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Get application-wide settings (OAuth providers, available AI list, IP, country).
     * GET /api/v1/settings
     */
    get(): Promise<AppSettings>;
    /**
     * Get available UI locales.
     * GET /api/v1/i18n/locales
     */
    getLocales(lang?: string, namespace?: string): Promise<Locale[]>;
}

interface GenerateDesignParams {
    chat_uuid: string;
    prompt: string;
    settings: DesignSettings;
    /**
     * Provider-specific settings merged into `body.settings` after the typed
     * surface above. Mirrors the SPA's `model_settings` passthrough for
     * `generate-image`. Last-wins over the typed keys above.
     */
    model_settings?: Record<string, unknown>;
}
/**
 * Resource for image/design generation.
 */
declare class DesignResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Generate an image/design.
     * POST /api/v1/design/generate?ai_name={aiName}
     *
     * Normalization: provider-specific rules from `./provider-rules` mutate
     * `settings` in place after the optional `model_settings` merge, so the
     * wire shape matches the SPA's `aiSettingsOnInput` payload (e.g. drop
     * `aspect_ratio` for `grok_i2i_pro`, coerce `seedream` resolutions).
     */
    generate(aiName: string, params: GenerateDesignParams): Promise<unknown>;
}

interface ListVoiceExamplesParams {
    page?: number;
    page_size?: number;
    [key: string]: string | number | boolean | undefined;
}
interface GenerateAudioParams {
    chat_uuid: string;
    prompt: string;
    settings: AudioSettings;
    file_urls?: string[];
    /**
     * Provider-specific settings merged into `body.settings` after the typed
     * surface above. Mirrors the SPA's `model_settings` passthrough for
     * `generate-audio`. Last-wins over the typed keys above.
     */
    model_settings?: Record<string, unknown>;
}
/**
 * Resource for audio generation (e.g. ElevenLabs voices).
 */
declare class AudioResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * List voice examples for ElevenLabs.
     * GET /api/v1/audio/elevenlabs/voice_examples
     */
    listVoiceExamples(params?: ListVoiceExamplesParams): Promise<VoiceExamplesResponse>;
    /**
     * Generate audio (TTS / music / voice-change) via syntx.ai.
     * POST /api/v1/audio/generate?ai_name={aiName}
     *
     * Mirrors `DesignResource.generate`. The SPA's `audio.js:sendMessage`
     * action sends `{chat_uuid, prompt, settings, file_urls?}`; we mirror that
     * exact body shape. Endpoint reachability confirmed against api.syntx.ai
     * (responds 403 without auth, matching the design endpoint's posture).
     *
     * Normalization: provider-specific rules from `./provider-rules` mutate
     * `settings` in place after the optional `model_settings` merge, so the
     * wire shape matches the SPA's `aiSettingsOnInput` payload (e.g. suno
     * strips `audio_url`/`continue_at`/source keys in `mode==='generate'`).
     */
    generate(aiName: string, params: GenerateAudioParams): Promise<unknown>;
}

interface GenerateVideoParams {
    chat_id: string;
    prompt: string;
    settings: VideoSettings;
    file_urls?: string[];
    audio_url?: string;
}
/**
 * Resource for video generation (e.g. wan_video, runway, kling).
 */
declare class VideoResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Generate a video via syntx.ai.
     * POST /api/v1/video/generate?ai_name={aiName}
     *
     * Mirrors `DesignResource.generate` / `AudioResource.generate`. The SPA's
     * `video.js:sendMessage` action sends `{chat_id, prompt, settings, file_urls?,
     * audio_url?}` per the catalog (#35). Note the body field is `chat_id`
     * (NOT `chat_uuid`) — the audio endpoint uses `chat_uuid` but video uses
     * `chat_id`. If a live request fails with 422, verify against the SPA
     * whether the server actually accepts the alternate key.
     *
     * Normalization: provider-specific rules from `./provider-rules` mutate
     * `settings` in place (after the user-supplied `model_settings` merge, when
     * applicable) so the wire shape matches the SPA's `aiSettingsOnInput`
     * payload. Callers do not need to know per-provider quirks (e.g. drop
     * `aspect_ratio` for `grok_i2v`); the rules encode them.
     */
    generate(aiName: string, params: GenerateVideoParams): Promise<unknown>;
}

/**
 * Resource for frontend application metadata.
 * Queries the public syntx.ai domain (not api.syntx.ai).
 */
declare class AppResource {
    private readonly baseURL;
    /**
     * Get the current deployed app version.
     * GET https://syntx.ai/version.json
     */
    getVersion(): Promise<VersionInfo>;
    /**
     * Check if the platform is under maintenance.
     * GET https://syntx.ai/maintenance-status.json
     */
    getMaintenanceStatus(): Promise<MaintenanceStatus>;
}

interface WaitForResponseOptions {
    timeout?: number;
    signal?: AbortSignal;
    sseTimeoutMs?: number;
    onProgress?: (elapsedMs: number, timeoutMs: number) => void;
    llmSseBaseUrl?: string;
    /**
     * REST-polling fallback invoked when SSE delivers nothing (no active job,
     * transport failure, or SSE-phase timeout). `opts.timeout` carries the
     * REMAINING wall-clock budget for the whole wait, not the original timeout.
     */
    fallbackPoll?: (chatId: string, opts: {
        timeout?: number;
        signal?: AbortSignal;
    }) => Promise<CompletedMessage>;
}
/**
 * Resource for the text-flow `llm/*` namespace used by the prod-SPA bundle.
 *
 * Endpoints:
 *  - `POST /api/v1/llm/generate?ai_name=…`
 *  - `GET  /api/v1/llm/models`
 *  - `GET  /api/v1/llm/chats/{chatId}/stream`
 *  - `GET  /api/v1/llm/limits`
 *
 * The companion SSE stream lives on `sse.syntx.ai`; see
 * {@link LlmResource.waitForResponse} for the full flow.
 */
declare class LlmResource {
    private readonly client;
    constructor(client: BaseClient);
    /**
     * Current LLM usage limits (6h and 7d windows). GET /api/v1/llm/limits.
     *
     * Windows with `expires_at === null` or `expires_at` already in the past
     * are normalised to `{ percent_left: 100, started_at: null, expires_at: null }`
     * so callers never have to re-implement that rule.
     */
    getLimits(): Promise<LlmLimits>;
    generate(params: LlmGenerateParams): Promise<LlmGenerateResponse>;
    listModels(params?: LlmListModelsParams): Promise<LlmModel[]>;
    getChatStream(chatId: string): Promise<{
        jobs: LlmStreamJob[];
    }>;
    waitForResponse(chatId: string, opts?: WaitForResponseOptions): Promise<CompletedMessage>;
}

/**
 * Main entry point for the Syntx AI SDK.
 *
 * Provides typed access to all syntx.ai API resources.
 *
 * @example
 * ```ts
 * const syntx = new SyntxClient({ token: 'your-api-token' });
 * const user = await syntx.user.me();
 * const models = await syntx.ai.listModels();
 * ```
 */
declare class SyntxClient {
    private readonly client;
    readonly auth: SyntxAuth;
    readonly ai: AIResource;
    readonly user: UserResource;
    readonly chats: ChatsResource;
    readonly plans: PlansResource;
    readonly notifications: NotificationsResource;
    readonly folders: FoldersResource;
    readonly settings: SettingsResource;
    readonly design: DesignResource;
    readonly audio: AudioResource;
    readonly video: VideoResource;
    readonly app: AppResource;
    readonly llm: LlmResource;
    constructor(config?: SyntxClientConfig);
    /**
     * Direct access to the underlying HTTP client for advanced use cases.
     */
    get http(): BaseClient;
}

/**
 * Typed configuration for the syntx-ai-mcp server.
 *
 * All values originate from environment variables (see {@link loadConfig}),
 * but can also be supplied programmatically to {@link createMcpServer}.
 */
type TransportKind = 'stdio' | 'http';
/** How the streaming chat endpoint should receive assistant replies. */
type StreamMode = 'auto' | 'stream' | 'poll' | 'off';
interface McpServerConfig {
    /** syntx.ai Bearer token. Optional — may be set at runtime via the `set-token` tool. */
    token?: string;
    /** Base URL of the syntx.ai API. */
    baseURL: string;
    /** HTTP request timeout in milliseconds. */
    timeout: number;
    /** Preferred language code (used by WebSocket streaming and locales). */
    lang: string;
    /** Default AI service name used when a tool omits `ai_name` (e.g. "chatgpt"). */
    defaultAI: string;
    /** Default model type used when a tool omits `model`. */
    defaultModel?: string;
    /** Polling interval (ms) for `wait-for-response` / `ask`. */
    pollInterval: number;
    /** Max wait time (ms) for a streamed/polling assistant response. */
    pollTimeout: number;
    /** MCP transport kind. */
    transport: TransportKind;
    /** Port for the HTTP transport. */
    httpPort: number;
    /**
     * Hostname the HTTP transport binds to. Defaults to `127.0.0.1` (loopback)
     * for safety. Set to `0.0.0.0` only behind a reverse proxy / firewall.
     */
    httpHostname: string;
    /**
     * Optional bearer token that MCP clients must present to the HTTP transport
     * itself (separate from the syntx.ai API token). When set, requests without
     * a matching `Authorization: Bearer <token>` header are rejected with 401.
     * When unset, the transport is loopback-only and prints a security warning.
     */
    httpToken?: string;
    /**
     * Default streaming strategy for chat tools.
     *  - `'auto'`   — try WSS, fall back to REST polling on error
     *  - `'stream'` — WSS only (failures surface to the caller)
     *  - `'poll'`   — REST polling only (legacy behaviour)
     *  - `'off'`    — disable `wait-for-response`/`ask` streaming helpers entirely
     */
    streamMode: StreamMode;
    /** Override the WSS base URL (used by streaming endpoints). */
    wsURL: string;
    /**
     * Base URL for the `sse.syntx.ai` Server-Sent Events stream used by the
     * text-flow `llm/*` namespace. `stream_url` values from the server are
     * joined onto this origin.
     */
    llmSseBaseUrl: string;
    /**
     * When true, the chat tools (`ask`, `stream-message`, `send-message`,
     * `wait-for-response`) bypass the `llm/*` text-flow and use the legacy
     * `chats/{id}/messages` path with REST polling. Defaults to `false`.
     */
    legacyTextTransport: boolean;
    /**
     * TTL for the in-memory cache used by `LlmResource.listModels`. The SPA
     * hits this endpoint frequently, so the SDK caches it for a short window.
     */
    listLlmModelsCacheMs: number;
}
/** Sensible defaults applied when an environment variable is absent. */
declare const DEFAULT_CONFIG: McpServerConfig;
/** Environment variable names → config keys mapping. */
declare const ENV_KEYS: {
    readonly token: "SYNTX_TOKEN";
    readonly baseURL: "SYNTX_BASE_URL";
    readonly timeout: "SYNTX_TIMEOUT";
    readonly lang: "SYNTX_LANG";
    readonly defaultAI: "SYNTX_DEFAULT_AI";
    readonly defaultModel: "SYNTX_DEFAULT_MODEL";
    readonly pollInterval: "SYNTX_POLL_INTERVAL";
    readonly pollTimeout: "SYNTX_POLL_TIMEOUT";
    readonly transport: "MCP_TRANSPORT";
    readonly httpPort: "MCP_HTTP_PORT";
    readonly httpHostname: "MCP_HTTP_HOSTNAME";
    readonly httpToken: "MCP_HTTP_TOKEN";
    readonly streamMode: "SYNTX_STREAM_MODE";
    readonly wsURL: "SYNTX_WS_URL";
    readonly llmSseBaseUrl: "SYNTX_LLM_SSE_BASE_URL";
    readonly legacyTextTransport: "SYNTX_LEGACY_TEXT_TRANSPORT";
    readonly listLlmModelsCacheMs: "SYNTX_LLM_MODELS_CACHE_MS";
};

/**
 * Extra context passed by the MCP server to every tool handler.
 *
 * Mirrors the shape of the underlying {@link RequestHandlerExtra} but exposes
 * just the fields our handlers care about, so tool authors don't need to
 * import MCP SDK internals.
 */
type SyntxToolExtra = Pick<RequestHandlerExtra<ServerRequest, ServerNotification>, 'sendNotification' | '_meta' | 'signal'>;
/**
 * Result of a tool handler. Aliased to the native {@link CallToolResult}:
 * handlers return data directly without constructing a full envelope, and the
 * server hands it straight back to the transport.
 */
type SyntxToolResult = CallToolResult;
/**
 * Capability inventory for a tool (I3, v0.3.0).
 *
 * Declares the security-relevant effects a tool can have so the server can
 * enforce policy generically (e.g. rejecting `localFileRead` arguments over
 * the HTTP transport) and so operators can audit the attack surface without
 * reading every handler. All flags default to `false` when omitted.
 *
 * After the v0.3.0 surface reduction the only enforced capability is
 * `localFileRead` (the I3 path-rejection in `server.ts`). The other flags
 * were documentation-only and have been removed — re-introduce them as
 * `boolean` members here if a future invariant needs the runtime check.
 */
interface SyntxToolCapability {
    /** Reads files from the MCP server's local filesystem (e.g. `path` input). */
    localFileRead?: boolean;
}
interface McpContext {
    /** Active SyntxClient. Token can be swapped at runtime via `setToken`. */
    readonly syntx: SyntxClient;
    /**
     * Resolved server configuration (live, read-only snapshot).
     */
    readonly config: Readonly<McpServerConfig>;
    /** Replace the active token (propagates to the underlying client). */
    setToken(token: string | undefined): void;
    /**
     * Send a `notifications/progress` frame to the client if it supplied a
     * `progressToken` for this request. No-op when the client opted out.
     *
     * Used by streaming tools to surface intermediate state without blocking
     * the final result.
     */
    sendProgress?: (progress: number, total?: number, message?: string) => Promise<void>;
    /**
     * Send a `notifications/message` (logging) frame to the client. Falls
     * back silently if the client does not support logging notifications.
     *
     * Used by `stream-message` for per-chunk log notifications (see
     * `chats.ts:streamAsk.onChunk`).
     */
    sendLog?: (level: 'debug' | 'info' | 'notice' | 'warning' | 'error' | 'critical' | 'alert' | 'emergency', data: unknown, logger?: string) => Promise<void>;
}
interface SyntxTool {
    name: string;
    description: string;
    /** JSON Schema for the tool arguments. */
    inputSchema: Tool['inputSchema'];
    /**
     * Security-relevant capability inventory (I3). Used by the server for
     * generic runtime enforcement and by operators for attack-surface audits.
     */
    capability?: SyntxToolCapability;
    /**
     * Tool handler. The optional {@link SyntxToolExtra} carries progress /
     * logging notifications; legacy callers can simply ignore it.
     */
    handler: (args: Record<string, unknown>, ctx: McpContext, extra?: SyntxToolExtra) => Promise<SyntxToolResult>;
}

/**
 * Build a configured MCP {@link Server} with all syntx-ai-mcp tools registered.
 *
 * `requestToken` (M2, v0.3.0) carries an HTTP request-scoped credential
 * (Authorization-header passthrough) — see {@link createMcpContext} for the
 * full token-precedence rules. Omit for stdio / single-tenant deployments.
 *
 * The returned server is NOT yet connected to a transport — call
 * `server.connect(transport)` from the transport layer.
 */
declare function createMcpServer(config: McpServerConfig, requestToken?: string): {
    server: Server;
    context: McpContext;
};

export { type MaintenanceStatus as $, type AIModel as A, type Balance as B, type Chat as C, DEFAULT_CONFIG as D, ENV_KEYS as E, type Folder as F, type GenerateAudioParams as G, type GetModelInfoParams as H, type GoogleTokenResponse as I, type InProgressItem as J, type InProgressResponse as K, type ListChatsParams as L, type McpServerConfig as M, type ListMessagesParams as N, type ListNotificationsParams as O, type ListVoiceExamplesParams as P, type LlmGenerateParams as Q, type LlmGenerateResponse as R, type SyntxToolExtra as S, type TransportKind as T, type LlmLimitWindow as U, type LlmLimits as V, type LlmListModelsParams as W, type LlmModel as X, LlmResource as Y, type LlmStreamJob as Z, type Locale as _, type McpContext as a, type Message as a0, type MessageAttachment as a1, type MessageObject as a2, type MessageObjectItem as a3, type MessagesResponse as a4, type ModelInfoV2 as a5, type Notification as a6, NotificationsResource as a7, type NotificationsResponse as a8, type OAuthProvider as a9, type UserInternal as aA, UserResource as aB, type UserSettings as aC, type VersionInfo as aD, VideoResource as aE, type VideoSettings as aF, type VoiceExample as aG, type VoiceExamplesResponse as aH, type WSSMessage as aI, type WaitForResponseOptions$1 as aJ, collectCompletedObjects as aK, createMcpServer as aL, toPublicUser as aM, type Pagination as aa, type PlanCard as ab, PlansResource as ac, type PlansResponse as ad, type PromoBanner as ae, type PublicUser as af, type RawLlmLimitWindow as ag, type ReferralInfo as ah, type SendChatMessageParams as ai, type SendMessageParams as aj, SettingsResource as ak, type StreamMode as al, type StreamResponseOptions as am, type StreamResponseResult as an, type StreamingMessage as ao, type Subscription as ap, SyntxAuth as aq, SyntxClient as ar, type SyntxClientConfig as as, type SyntxToolResult as at, SyntxWebSocket as au, type SyntxWebSocketOptions as av, type UnreadCount as aw, type UploadFileInput as ax, type UploadResult as ay, type User as az, type SyntxTool as b, type AIModelSettings as c, AIResource as d, type AIService as e, AppResource as f, type AppSettings as g, AudioResource as h, type AudioSettings as i, type AuthStart as j, type AuthTokenStatus as k, BaseClient as l, ChatsResource as m, type CompletedMedia as n, type CompletedMessage as o, type CreateChatParams as p, type CreateFolderParams as q, type CreatedFolder as r, DesignResource as s, type DesignSettings as t, type EmailOtpOptions as u, type EmailOtpSendResult as v, type EmailOtpVerifyResult as w, FoldersResource as x, type GenerateDesignParams as y, type GenerateVideoParams as z };
