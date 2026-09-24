// src/mcp/server.ts
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema
} from "@modelcontextprotocol/sdk/types.js";

// src/errors.ts
var SyntxAPIError = class extends Error {
  constructor(message, status, code, responseBody, retryAfterMs) {
    super(message);
    this.status = status;
    this.code = code;
    this.responseBody = responseBody;
    this.retryAfterMs = retryAfterMs;
    this.name = "SyntxAPIError";
  }
  status;
  code;
  responseBody;
  retryAfterMs;
};
var SyntxAuthError = class extends Error {
  constructor(message = "Authentication required") {
    super(message);
    this.name = "SyntxAuthError";
  }
};
var SyntxTimeoutError = class extends Error {
  constructor(message, chatId, elapsedMs, timeoutMs) {
    super(message);
    this.chatId = chatId;
    this.elapsedMs = elapsedMs;
    this.timeoutMs = timeoutMs;
    this.name = "SyntxTimeoutError";
  }
  chatId;
  elapsedMs;
  timeoutMs;
};
var SyntxAbortError = class extends Error {
  constructor(message = "Operation cancelled") {
    super(message);
    this.name = "SyntxAbortError";
  }
};

// src/client.ts
var RETRY_BASE_MS = 500;
var RETRY_CAP_MS = 8e3;
var RETRY_JITTER_MS = 250;
var RETRY_HINT_CAP_MS = 6e4;
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function parseRetryAfter(header, now = Date.now()) {
  if (!header) return void 0;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1e3;
  }
  const date = Date.parse(header);
  if (!Number.isNaN(date)) {
    return Math.max(0, date - now);
  }
  return void 0;
}
function isRetryable(error) {
  if (error instanceof SyntxAPIError) {
    return error.status === 408 || error.status === 429 || error.status >= 500;
  }
  return error instanceof TypeError;
}
var BaseClient = class {
  baseURL;
  token;
  timeout;
  maxRetries;
  constructor(config = {}) {
    this.baseURL = (config.baseURL ?? "https://api.syntx.ai").replace(/\/$/, "");
    this.token = config.token;
    this.timeout = config.timeout ?? 3e4;
    this.maxRetries = Math.max(1, config.maxRetries ?? 3);
  }
  setToken(token) {
    this.token = token;
  }
  getToken() {
    return this.token;
  }
  isAuthenticated() {
    return !!this.token;
  }
  async requestWithTimeout(url, options, timeoutOverride) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutOverride ?? this.timeout);
    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal
      });
      clearTimeout(id);
      return this.handleResponse(response);
    } catch (error) {
      clearTimeout(id);
      if (error instanceof Error && error.name === "AbortError") {
        throw new SyntxAPIError("Request timeout", 408);
      }
      throw error;
    }
  }
  /**
   * Execute a request, retrying transient failures with exponential
   * backoff + jitter when `retryable` is true. A `Retry-After` hint from
   * a 429 response overrides the computed delay.
   */
  async requestWithRetry(url, options, retryable, timeoutOverride) {
    const maxAttempts = retryable ? this.maxRetries : 1;
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.requestWithTimeout(url, options, timeoutOverride);
      } catch (error) {
        if (attempt >= maxAttempts || !isRetryable(error)) {
          throw error;
        }
        const rawHint = error instanceof SyntxAPIError ? error.retryAfterMs : void 0;
        const hint = rawHint !== void 0 && rawHint > 0 && rawHint <= RETRY_HINT_CAP_MS ? rawHint : void 0;
        const computed = Math.min(RETRY_BASE_MS * 2 ** (attempt - 1), RETRY_CAP_MS);
        const delay = (hint ?? computed) + Math.floor(Math.random() * RETRY_JITTER_MS);
        await sleep(delay);
      }
    }
  }
  async handleResponse(response) {
    if (response.status === 401 || response.status === 403) {
      throw new SyntxAuthError(
        `Authentication failed (${response.status})`
      );
    }
    let body;
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      body = await response.json();
    } else {
      const text = await response.text();
      body = text;
    }
    if (!response.ok) {
      const message = typeof body === "object" && body !== null && "message" in body ? String(body.message) : response.statusText;
      const status = response.status;
      const retryAfterHint = status === 429 || status >= 500 ? parseRetryAfter(response.headers.get("retry-after")) : void 0;
      throw new SyntxAPIError(
        message,
        status,
        void 0,
        body,
        retryAfterHint
      );
    }
    return body;
  }
  buildUrl(path2, params) {
    const url = new URL(this.baseURL + path2);
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== void 0) {
          url.searchParams.set(key, String(value));
        }
      });
    }
    return url.toString();
  }
  /** Shared `Accept` + bearer-token header block used by every request. */
  baseHeaders() {
    const headers = { Accept: "application/json" };
    if (this.token) {
      headers["Authorization"] = `Bearer ${this.token}`;
    }
    return headers;
  }
  jsonHeaders() {
    return { ...this.baseHeaders(), "Content-Type": "application/json" };
  }
  async get(path2, params) {
    return this.requestWithRetry(this.buildUrl(path2, params), {
      method: "GET",
      headers: this.baseHeaders()
    }, true);
  }
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
  async stream(path2, init) {
    const headers = { ...this.baseHeaders(), ...init?.headers ?? {} };
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), init?.timeoutMs ?? this.timeout);
    const onCallerAbort = () => controller.abort();
    if (init?.signal) {
      if (init.signal.aborted) controller.abort();
      else init.signal.addEventListener("abort", onCallerAbort, { once: true });
    }
    try {
      const response = await fetch(this.baseURL + path2, {
        ...init?.signal ? { signal: controller.signal } : {},
        headers
      });
      clearTimeout(timer);
      init?.signal?.removeEventListener("abort", onCallerAbort);
      return response;
    } catch (error) {
      clearTimeout(timer);
      init?.signal?.removeEventListener("abort", onCallerAbort);
      if (error instanceof Error && error.name === "AbortError") {
        throw new SyntxAPIError("Request timeout", 408);
      }
      throw error;
    }
  }
  /**
   * Shared implementation for the JSON-bodied HTTP verbs (POST / PATCH /
   * PUT / DELETE): identical pipeline, only the method differs. `body`
   * is falsy-skipped so body-less calls (e.g. toggles) send no payload.
   */
  async jsonRequest(method, path2, body, params) {
    return this.requestWithRetry(this.buildUrl(path2, params), {
      method,
      headers: this.jsonHeaders(),
      body: body ? JSON.stringify(body) : void 0
    }, false);
  }
  async post(path2, body, params) {
    return this.jsonRequest("POST", path2, body, params);
  }
  async patch(path2, body, params) {
    return this.jsonRequest("PATCH", path2, body, params);
  }
  async put(path2, body, params) {
    return this.jsonRequest("PUT", path2, body, params);
  }
  async delete(path2, body, params) {
    return this.jsonRequest("DELETE", path2, body, params);
  }
  /**
   * POST a `FormData` body through the same timeout / auth / error-mapping
   * pipeline as JSON requests.
   *
   * Unlike JSON POSTs, `Content-Type` is intentionally NOT set — fetch
   * fills in the multipart boundary. `timeoutOverride` defaults to 5 min
   * because uploads/transcriptions routinely exceed the 30 s API default.
   */
  async postForm(path2, formData, timeoutOverride = 3e5) {
    return this.requestWithRetry(this.baseURL + path2, {
      method: "POST",
      headers: this.baseHeaders(),
      body: formData
    }, false, timeoutOverride);
  }
};

// src/auth.ts
import { createHash, randomBytes } from "crypto";
function base64url(buf) {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
var SyntxAuth = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Set the API token directly (e.g. after obtaining it via OAuth).
   */
  setToken(token) {
    this.client.setToken(token);
  }
  /**
   * Get the current token.
   */
  getToken() {
    return this.client.getToken();
  }
  /**
   * Check if a token is set.
   */
  isAuthenticated() {
    return this.client.isAuthenticated();
  }
  /**
   * Clear the current token.
   */
  logout() {
    this.client.setToken(void 0);
  }
  /**
   * Placeholder: Initiate Telegram OAuth login.
   * In a browser, this typically opens a Telegram login widget popup
   * or redirects to the Telegram OAuth page.
   */
  getTelegramLoginUrl(redirectUri, botId = "syntxaibot") {
    const url = new URL("https://oauth.telegram.org/auth");
    url.searchParams.set("bot_id", botId);
    if (redirectUri) {
      url.searchParams.set("origin", redirectUri);
    }
    return url.toString();
  }
  /**
   * Generate a PKCE key pair (RFC 7636): a high-entropy `code_verifier`
   * and its `S256` `code_challenge`. Use the challenge when building the
   * authorization URL ({@link getGoogleLoginUrl}) and keep the verifier
   * secret until {@link exchangeGoogleCode}.
   */
  static generatePkcePair() {
    const codeVerifier = base64url(randomBytes(32));
    const codeChallenge = base64url(createHash("sha256").update(codeVerifier).digest());
    return { codeVerifier, codeChallenge };
  }
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
  getGoogleLoginUrl(clientId, redirectUri, options = {}) {
    const url = new URL("https://accounts.google.com/o/oauth2/v2/auth");
    url.searchParams.set("client_id", clientId);
    url.searchParams.set("redirect_uri", redirectUri);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("scope", "email profile");
    if (options.state) {
      url.searchParams.set("state", options.state);
    }
    if (options.codeChallenge) {
      url.searchParams.set("code_challenge", options.codeChallenge);
      url.searchParams.set("code_challenge_method", "S256");
    }
    return url.toString();
  }
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
  async exchangeGoogleCode(options) {
    const { clientId, redirectUri, code, codeVerifier } = options;
    if (!code.trim() || !codeVerifier.trim()) {
      throw new SyntxAuthError("exchangeGoogleCode requires non-empty code and codeVerifier.");
    }
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: clientId,
      redirect_uri: redirectUri,
      code,
      code_verifier: codeVerifier
    });
    const response = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString()
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = typeof payload.error_description === "string" ? payload.error_description : typeof payload.error === "string" ? payload.error : response.statusText;
      throw new SyntxAuthError(`Google code exchange failed (${response.status}): ${detail}`);
    }
    return payload;
  }
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
  async sendEmailOtp(email, options = {}) {
    return this.client.post("/api/v1/auth/email/send-otp", {
      email,
      ref_uuid: options.ref_uuid ?? null,
      utm: options.utm ?? ""
    });
  }
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
  async verifyEmailOtp(email, otpCode, options = {}) {
    const code = String(otpCode ?? "").trim();
    if (!code) {
      throw new SyntxAuthError("otpCode must be a non-empty string.");
    }
    const result = await this.client.post(
      "/api/v1/auth/email/verify-otp",
      {
        email,
        otp_code: code,
        ref_uuid: options.ref_uuid ?? null,
        utm: options.utm ?? ""
      }
    );
    const shouldInstall = options.install !== false;
    if (shouldInstall && result && typeof result.token === "string" && result.token.length > 0) {
      this.setToken(result.token);
    }
    return result;
  }
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
  async loginWithEmail(email, options = {}) {
    const { otpProvider, ...otpOptions } = options;
    if (typeof otpProvider !== "function") {
      throw new SyntxAuthError(
        "loginWithEmail requires options.otpProvider \u2014 a () => Promise<string> that resolves with the OTP code from the user."
      );
    }
    await this.sendEmailOtp(email, otpOptions);
    const otpCode = (await otpProvider()).trim();
    if (!otpCode) {
      throw new SyntxAuthError("OTP provider returned an empty code.");
    }
    return this.verifyEmailOtp(email, otpCode, otpOptions);
  }
  /**
   * Validate the current token by calling /api/v1/user.
   * Throws SyntxAuthError if no token is set.
   */
  async validateToken() {
    if (!this.isAuthenticated()) {
      throw new SyntxAuthError("No token set");
    }
    try {
      await this.client.get("/api/v1/user");
      return true;
    } catch {
      return false;
    }
  }
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
  async startAuth() {
    return this.client.post("/api/v1/auth/startauth");
  }
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
  async pollAuthToken(uuid) {
    return this.client.get(`/api/v1/auth/token/${encodeURIComponent(uuid)}`);
  }
  /**
   * Build a `t.me` deep-link that opens the syntx.ai Telegram bot with a
   * pre-filled `start` payload. When the user presses Start, the bot
   * receives `auth_<uuid>` and binds the session to the user's Telegram
   * identity, which unblocks the polling endpoint.
   *
   * `botUsername` defaults to `syntxaibot` (the public bot used by
   * syntx.ai). Override only if you are pointing at a custom bot.
   */
  getTelegramAuthLink(uuid, botUsername = "syntxaibot") {
    return `https://telegram.me/${botUsername}?start=auth_${uuid}`;
  }
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
  async loginWithTelegram(options = {}) {
    const {
      botUsername = "syntxaibot",
      pollIntervalMs = 3e3,
      timeoutMs = 5 * 6e4,
      // 5 min — Telegram Start button is manual
      onPoll,
      onLink
    } = options;
    const { uuid } = await this.startAuth();
    const deepLink = this.getTelegramAuthLink(uuid, botUsername);
    if (onLink) onLink(deepLink, uuid);
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const status = await this.pollAuthToken(uuid);
      const elapsedMs = Date.now() - start;
      if (onPoll) onPoll(status, elapsedMs);
      if (!status.valid) {
        throw new SyntxAuthError(
          `Auth session ${uuid} is invalid (expired or unknown). Restart the flow.`
        );
      }
      if (status.complete) {
        if (!status.token) {
          throw new SyntxAuthError(
            `Auth session ${uuid} is complete but no token was returned.`
          );
        }
        this.setToken(status.token);
        return { uuid, deepLink, token: status.token, status, elapsedMs };
      }
      await new Promise((r) => setTimeout(r, pollIntervalMs));
    }
    throw new SyntxAuthError(
      `Telegram auth timed out after ${timeoutMs}ms. User did not press Start in the bot.`
    );
  }
};

// src/resources/ai.ts
var AIResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * List all available AI services (e.g. Midjourney, Sora, Flux).
   * GET /api/v1/ai
   */
  async listServices() {
    return this.client.get("/api/v1/ai");
  }
  /**
   * List detailed AI models with upload constraints and features.
   * GET /api/v1/ai/models
   */
  async listModels() {
    return this.client.get("/api/v1/ai/models");
  }
  /**
   * Get detailed info about a specific model (v2 endpoint).
   * GET /api/v2/get_model_info
   */
  async getModelInfo(params) {
    return this.client.get("/api/v2/get_model_info", params);
  }
};

// src/resources/user.ts
var SECRET_LIKE_FIELD = /(?:^|_)(secret|token|api[_-]?key|hmac|hmac_key|client_secret)(?:$|_)/i;
function stripUnknownSecrets(obj) {
  for (const key of Object.keys(obj)) {
    if (SECRET_LIKE_FIELD.test(key)) {
      delete obj[key];
    }
  }
  return obj;
}
var UserResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Get current user profile (raw internal shape).
   * GET /api/v1/user
   *
   * The returned object includes internal identifiers (`chatwoot_hmac`,
   * `ym_client_id`) that must never be exposed via MCP tool surfaces.
   * Prefer {@link mePublic} for MCP-facing code.
   */
  async me() {
    return this.client.get("/api/v1/user");
  }
  /**
   * Public projection of the current user profile — safe to expose via MCP.
   *
   * Strips internal identifiers (`chatwoot_hmac`, `ym_client_id`) before
   * returning. The underlying network call still fetches the full payload;
   * the projection happens on the SDK boundary so callers downstream never
   * see the sensitive fields even if a future feature forgets to scrub.
   */
  async mePublic() {
    const raw = await this.me();
    return toPublicUser(raw);
  }
  /**
   * Get token balance.
   * GET /api/v1/user/balance
   */
  async getBalance() {
    return this.client.get("/api/v1/user/balance");
  }
  /**
   * Get active subscription details.
   * GET /api/v1/user/subscription
   */
  async getSubscription() {
    return this.client.get("/api/v1/user/subscription");
  }
  /**
   * Get user-specific settings.
   * GET /api/v1/user/settings
   */
  async getSettings() {
    return this.client.get("/api/v1/user/settings");
  }
};
function toPublicUser(user) {
  const scrubbed = stripUnknownSecrets({ ...user });
  return {
    id: scrubbed.id,
    user_id: scrubbed.user_id,
    name: scrubbed.name ?? null,
    username: scrubbed.username ?? null,
    email: scrubbed.email ?? null,
    avatar: scrubbed.avatar ?? null,
    auth_services: Array.isArray(scrubbed.auth_services) ? scrubbed.auth_services : []
  };
}

// src/resources/chats.ts
var MEDIA_OBJECT_TYPES = /* @__PURE__ */ new Set(["image", "video", "audio", "file"]);
function collectCompletedObjects(message) {
  const objects = message.message_object;
  if (!Array.isArray(objects) || objects.length === 0) {
    return { text: "", media: [], ready: false };
  }
  const ready = objects.every((o) => o && o.completed === true);
  const textParts = [];
  const media = [];
  for (const o of objects) {
    if (!o) continue;
    if (o.object_type === "text" || o.object_type === "filetext") {
      if (typeof o.object_text === "string" && o.object_text.length > 0) {
        textParts.push(o.object_text);
      }
    } else if (MEDIA_OBJECT_TYPES.has(o.object_type) && o.object_url) {
      media.push({
        object_type: o.object_type,
        object_url: o.object_url,
        object_text: typeof o.object_text === "string" ? o.object_text : "",
        metadata: o.metadata ?? null
      });
    }
  }
  const text = textParts.length > 1 ? textParts.join("\n\n") : textParts.join("");
  return { text, media, ready };
}
var ChatsResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * List user chats.
   * GET /api/v1/chats
   */
  async list(params) {
    return this.client.get("/api/v1/chats", params);
  }
  /**
   * Get messages for a specific chat.
   * GET /api/v1/chats/{chatId}/messages
   */
  async getMessages(chatId, params) {
    return this.client.get(`/api/v1/chats/${chatId}/messages`, params);
  }
  /**
   * Get favorite messages for a specific chat.
   * GET /api/v1/chats/favorite/{chatId}/messages
   */
  async getFavoriteMessages(chatId, params) {
    return this.client.get(`/api/v1/chats/favorite/${chatId}/messages`, params);
  }
  /**
   * Create a new chat/session.
   * POST /api/v1/chats
   *
   * Note: the API requires at least `title` to be present, otherwise
   * it returns 422 Unprocessable Entity.
   */
  async create(data) {
    return this.client.post("/api/v1/chats", data);
  }
  /**
   * Send a message (or multiple objects) to a chat.
   * POST /api/v1/chats/{chatId}/messages?ai_name={aiName}
   *
   * The real API expects `{ objects: MessageObject[] }`.
   * Each object can have object_type "text", "filetext", "image", etc.
   */
  async sendMessage(chatId, aiName, objects) {
    return this.client.post(`/api/v1/chats/${chatId}/messages`, { objects }, { ai_name: aiName });
  }
  /**
   * Check if a chat has in-progress operations.
   * GET /api/v1/chats/{chatId}/inprogress
   */
  async getInProgress(chatId) {
    return this.client.get(`/api/v1/chats/${chatId}/inprogress`);
  }
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
  async getLatestBoundary(chatId) {
    try {
      const { messages } = await this.getMessages(chatId, { page_size: 50 });
      let maxId = 0;
      for (const m of messages) {
        if (!m || !m.id) continue;
        if (m.author_id === -1) continue;
        const n = Number(m.id);
        if (!Number.isFinite(n)) continue;
        if (n > maxId) maxId = n;
      }
      return String(maxId);
    } catch {
      return "0";
    }
  }
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
  async waitForResponse(chatId, options) {
    return this.pollForResponse(chatId, options);
  }
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
  async streamResponse(prompt, options) {
    const scope = options?.scope ?? "text";
    const model = options?.model;
    const aiName = options?.aiName;
    const timeout = options?.timeout ?? 6e5;
    const pollInterval = 2e3;
    const onSession = options?.onSession;
    const onChunk = options?.onChunk;
    const start = Date.now();
    const chat = await this.create({
      scope,
      title: prompt.slice(0, 60),
      ...model ? { model } : {}
    });
    const chatUuid = chat.uuid;
    try {
      onSession?.(chatUuid);
    } catch {
    }
    await this.sendMessage(chatUuid, aiName ?? "chatgpt", [
      {
        object_type: "text",
        object_url: null,
        object_text: prompt,
        ...model ? { model_type: model } : {}
      }
    ]);
    const { text } = await this.pollForResponse(chatUuid, {
      timeout,
      pollInterval,
      signal: options?.signal,
      onProgress: options?.onProgress
    });
    if (text) {
      try {
        onChunk?.(text, text);
      } catch {
      }
    }
    return {
      text,
      // The REST `Message` shape differs from the WSS `StreamingMessage`;
      // we expose the text (the caller's primary interest) and null the
      // raw frame since we no longer use WebSocket frames.
      message: null,
      elapsedMs: Date.now() - start,
      chatUuid
    };
  }
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
  async pollForResponse(chatId, options) {
    const timeout = options?.timeout ?? 6e5;
    const maxPollInterval = options?.pollInterval ?? 5e3;
    const pageSize = options?.pageSize ?? 50;
    const preWaitTimeout = options?.preWaitTimeout ?? timeout;
    const maxConsecutiveErrors = 5;
    const signal = options?.signal;
    const onProgress = options?.onProgress;
    const dbg = process.env.SYNTX_DEBUG ? (msg) => {
      try {
        process.stderr.write(`[poll-debug chat=${chatId}] ${msg()}
`);
      } catch {
      }
    } : (_msg) => {
    };
    dbg(() => `start timeout=${timeout}ms pollInterval=${maxPollInterval}ms pageSize=${pageSize}`);
    const throwIfAborted = () => {
      if (signal?.aborted) {
        throw new SyntxAbortError(`Wait cancelled in chat ${chatId}`);
      }
    };
    const start = Date.now();
    let currentInterval = Math.max(
      Math.floor(maxPollInterval * 0.4),
      Math.min(1e3, maxPollInterval)
    );
    const nextInterval = () => {
      const interval = currentInterval;
      currentInterval = Math.min(maxPollInterval, Math.floor(currentInterval * 1.5));
      return interval;
    };
    const sleep2 = (ms) => new Promise((resolve2, reject) => {
      throwIfAborted();
      const onAbort = () => {
        clearTimeout(timer);
        reject(new SyntxAbortError(`Wait cancelled in chat ${chatId}`));
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve2();
      }, ms);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
    const heartbeat = () => {
      if (!onProgress) return;
      try {
        onProgress(Date.now() - start, timeout);
      } catch {
      }
    };
    let boundary = options?.boundary;
    if (!boundary) {
      boundary = await this.getLatestBoundary(chatId);
    }
    dbg(() => `boundary=${JSON.stringify(boundary)} hasNumericBoundary=${typeof boundary === "string" && Number.isFinite(Number(boundary)) && Number(boundary) > 0}`);
    const boundaryId = Number(boundary);
    const hasNumericBoundary = Number.isFinite(boundaryId) && boundaryId > 0;
    const progress = await this.getInProgress(chatId);
    if (Array.isArray(progress) && progress.length > 0) {
      const preWaitStart = Date.now();
      while (true) {
        heartbeat();
        await sleep2(nextInterval());
        const p = await this.getInProgress(chatId);
        if (!Array.isArray(p) || p.length === 0) break;
        const elapsedFromStart = Date.now() - start;
        if (elapsedFromStart > timeout) {
          throw new SyntxTimeoutError(
            `Timeout waiting for previous in-progress request to finish in chat ${chatId}`,
            chatId,
            elapsedFromStart,
            timeout
          );
        }
        if (Date.now() - preWaitStart > preWaitTimeout) {
          throw new SyntxTimeoutError(
            `Timeout waiting for previous in-progress request to finish in chat ${chatId}`,
            chatId,
            Date.now() - preWaitStart,
            preWaitTimeout
          );
        }
      }
    }
    let consecutiveErrors = 0;
    let iter = 0;
    while (true) {
      throwIfAborted();
      iter++;
      const elapsed = Date.now() - start;
      if (elapsed > timeout) {
        throw new SyntxTimeoutError(
          `Timeout waiting for response in chat ${chatId}`,
          chatId,
          elapsed,
          timeout
        );
      }
      heartbeat();
      await sleep2(nextInterval());
      try {
        const { messages } = await this.getMessages(chatId, { page_size: pageSize });
        if (!messages || !Array.isArray(messages)) {
          throw new Error(`getMessages returned invalid messages: ${typeof messages}`);
        }
        const newAssistantMsgs = messages.filter((m) => {
          if (!m || m.author_id !== -1) return false;
          if (hasNumericBoundary) {
            const id = Number(m.id);
            return Number.isFinite(id) && id > boundaryId;
          }
          if (boundary && m.created_at) return m.created_at > boundary;
          return true;
        });
        const assistant = newAssistantMsgs[newAssistantMsgs.length - 1];
        if (!assistant) {
          dbg(() => `iter=${iter} no assistant match: got=${messages.length} msgs, candidate authors=${messages.map((m) => `${m.id}:${m.author_id}`).slice(0, 5).join(",")}`);
          consecutiveErrors = 0;
          continue;
        }
        dbg(() => `iter=${iter} found assistant id=${assistant.id} created_at=${assistant.created_at}`);
        const projection = collectCompletedObjects(assistant);
        if (projection.ready) {
          return { text: projection.text, media: projection.media, message: assistant };
        }
        consecutiveErrors = 0;
      } catch (err) {
        if (err instanceof SyntxAbortError) throw err;
        consecutiveErrors++;
        if (consecutiveErrors >= maxConsecutiveErrors) {
          throw new Error(
            `Too many consecutive poll errors (${maxConsecutiveErrors}) in chat ${chatId}: ${err instanceof Error ? err.message : String(err)}`
          );
        }
      }
    }
  }
  /**
   * Get a specific message by ID.
   * GET /api/v1/chats/{chatId}/{messageId}
   */
  async getMessage(chatId, messageId) {
    return this.client.get(`/api/v1/chats/${chatId}/${messageId}`);
  }
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
  async uploadFiles(files, destination, checkDuplicates = true, modelType = "") {
    const formData = new FormData();
    for (const item of files) {
      if (item instanceof Blob) {
        const filename = item.name ?? "upload";
        const type = item.type || void 0;
        formData.append("files", type ? new Blob([item], { type }) : item, filename);
      } else {
        const view = new Uint8Array(item.buffer.byteLength);
        view.set(item.buffer);
        const blob = new Blob([view], item.mimeType ? { type: item.mimeType } : void 0);
        formData.append("files", blob, item.filename);
      }
    }
    if (destination) formData.append("destination", destination);
    formData.append("check_duplicates", String(checkDuplicates));
    formData.append("model_type", modelType);
    const data = await this.client.postForm(
      "/api/v1/chats/upload-files",
      formData
    );
    return "data" in data && data.data ? data.data : data;
  }
  /**
   * Delete a file.
   * DELETE /api/v1/files/delete
   *
   * The SPA accepts either `{ file_id }` or `{ url }` as the body. Pass a
   * string to delete by id (the historical SDK behaviour), or a `{ url }`
   * object to delete by the uploaded R2 URL.
   */
  async deleteFile(target) {
    const body = typeof target === "string" ? { file_id: target } : { url: target.url };
    await this.client.delete("/api/v1/files/delete", body);
  }
  /**
   * Get uploaded files.
   * GET /api/v1/files/uploaded
   */
  async getUploadedFiles(scope = "all", page = 1, pageSize = 10) {
    return this.client.get("/api/v1/files/uploaded", { scope, page, page_size: pageSize });
  }
  /**
   * Transcribe audio to text.
   * POST /api/v1/audio/transcribe
   */
  async transcribe(file) {
    const formData = new FormData();
    formData.append("file", file);
    const data = await this.client.postForm(
      "/api/v1/audio/transcribe",
      formData
    );
    return data.data ?? { text: data.text ?? "" };
  }
  /**
   * Generate a session title using AI.
   * POST /api/v1/chats/by-uuid/{chatUuid}/generate-title
   */
  async generateTitle(chatUuid) {
    await this.client.post(`/api/v1/chats/by-uuid/${chatUuid}/generate-title`);
  }
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
  async get(chatId) {
    return this.client.get(`/api/v1/chats/${chatId}`);
  }
  /**
   * Lightweight existence probe. Returns `true` iff `GET /api/v1/chats/{chatId}`
   * responds `200`. Any non-2xx response (404 in particular) returns `false`.
   * Other errors (network, 5xx) propagate so the caller can distinguish
   * "definitely missing" from "could not tell".
   */
  async exists(chatId) {
    try {
      await this.get(chatId);
      return true;
    } catch (err) {
      if (err instanceof SyntxAPIError && err.status === 404) return false;
      throw err;
    }
  }
  /**
   * Delete a chat.
   * DELETE /api/v1/chats/{chatId}
   */
  async delete(chatId) {
    await this.client.delete(`/api/v1/chats/${chatId}`);
  }
  /**
   * Pin/unpin a chat.
   * POST /api/v1/chats/{chatId}/pin
   */
  async pin(chatId) {
    await this.client.post(`/api/v1/chats/${chatId}/pin`);
  }
  /**
   * Move chat to folder.
   * POST /api/v1/chats/{chatId}/move
   */
  async moveToFolder(chatId, folderId) {
    await this.client.post(`/api/v1/chats/${chatId}/move`, { folder_id: folderId });
  }
  /**
   * Rename a chat.
   *
   * `PUT /api/v1/chats/{chatId}` with body `{title}`.
   *
   * Mirrors the syntx.ai sessions store (SPA app bundle:
   * `fe.put(\`chats/${h}\`,{title:c})`). The response body is ignored by
   * the SPA; the raw upstream response is passed through unpinned.
   */
  async rename(chatId, title) {
    return this.client.put(`/api/v1/chats/${encodeURIComponent(chatId)}`, { title });
  }
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
  async toggleFavorite(chatId) {
    return this.client.post(`/api/v1/chats/${encodeURIComponent(chatId)}/favorite`);
  }
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
  async toggleMessageFavorite(chatId, messageId) {
    return this.client.post(
      `/api/v1/chats/${encodeURIComponent(chatId)}/messages/${encodeURIComponent(messageId)}/favorite`
    );
  }
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
  async deleteMessage(messageId) {
    await this.client.delete(`/api/v1/chats/messages/${encodeURIComponent(messageId)}`);
  }
  /**
   * Cancel an in-flight message generation.
   *
   * `POST /api/v1/chats/{chatId}/messages/{messageId}/cancel`
   *
   * The endpoint is idempotent in practice: a 404 means the message has
   * already finished (race with completion) and is treated as success so
   * caller code doesn't have to special-case it.
   */
  async cancelMessage(chatId, messageId) {
    try {
      await this.client.post(
        `/api/v1/chats/${encodeURIComponent(chatId)}/messages/${encodeURIComponent(messageId)}/cancel`
      );
    } catch (err) {
      if (err instanceof SyntxAPIError && err.status === 404) return;
      throw err;
    }
  }
};

// src/resources/plans.ts
var PlansResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Get all available plans with detailed descriptions.
   * GET /api/v1/plans/card_plans
   */
  async list(lang = "en") {
    return this.client.get("/api/v1/plans/card_plans", { lang });
  }
  /**
   * Get promo banners for a specific language.
   * GET /api/v1/promo_banners
   */
  async getPromoBanners(lang) {
    return this.client.get("/api/v1/promo_banners", { lang });
  }
};

// src/resources/notifications.ts
var NotificationsResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Get global notifications with pagination.
   * GET /api/v1/notification/global
   */
  async list(params) {
    return this.client.get("/api/v1/notification/global", params);
  }
  /**
   * Get unread notifications count.
   * GET /api/v1/notification/unread/count
   */
  async getUnreadCount() {
    return this.client.get("/api/v1/notification/unread/count");
  }
  /**
   * Mark a single notification as read.
   * PATCH /api/v1/notification/mark/global/{id}
   *
   * The previous SDK implementation targeted `PATCH /api/v1/notifications/{id}/read`
   * but that endpoint returned 404 against api.syntx.ai. The SPA-observed
   * path (`notification/mark/global/{id}`) returns 403 (auth-gated) — same
   * pattern as the other working endpoints — and is therefore authoritative.
   */
  async markAsRead(id) {
    await this.client.patch(`/api/v1/notification/mark/global/${encodeURIComponent(id)}`);
  }
  /**
   * Mark every notification as read.
   * PATCH /api/v1/notification/mark/all
   *
   * The SPA fires this from `notification.js:markAllRead` whenever the
   * "mark all" UI action is invoked. No body is required.
   */
  async markAll() {
    await this.client.patch("/api/v1/notification/mark/all");
  }
};

// src/resources/folders-settings.ts
var FoldersResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * List text folders.
   * GET /api/v1/folders/text/list
   */
  async listTextFolders() {
    return this.client.get("/api/v1/folders/text/list");
  }
  /**
   * List image folders.
   * GET /api/v1/folders/image/list
   */
  async listImageFolders() {
    return this.client.get("/api/v1/folders/image/list");
  }
  /**
   * List video folders.
   * GET /api/v1/folders/video/list
   */
  async listVideoFolders() {
    return this.client.get("/api/v1/folders/video/list");
  }
  /**
   * List audio folders.
   * GET /api/v1/folders/audio/list
   */
  async listAudioFolders() {
    return this.client.get("/api/v1/folders/audio/list");
  }
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
  async create(data) {
    const body = {
      title: data.title,
      scope: data.scope ?? "text",
      color: data.color ?? "#9C9C9C",
      chat_uuids: data.chat_uuids ?? []
    };
    return this.client.post("/api/v1/folders/create", body);
  }
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
  async addChats(folderUuid, chatUuids) {
    return this.client.post(`/api/v1/folders/${encodeURIComponent(folderUuid)}/add`, chatUuids);
  }
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
  async removeChats(folderUuid, chatUuids) {
    return this.client.post(`/api/v1/folders/${encodeURIComponent(folderUuid)}/remove`, chatUuids);
  }
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
  async update(folderUuid, data) {
    const body = {};
    if (data.title !== void 0) body.title = data.title;
    if (data.color !== void 0) body.color = data.color;
    return this.client.patch(`/api/v1/folders/${encodeURIComponent(folderUuid)}/change`, body);
  }
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
  async move(folderUuid, afterUuid) {
    return this.client.patch(
      `/api/v1/folders/${encodeURIComponent(folderUuid)}/move`,
      { after_uuid: afterUuid }
    );
  }
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
  async delete(folderUuid) {
    return this.client.delete(`/api/v1/folders/${encodeURIComponent(folderUuid)}/delete`);
  }
};
var SettingsResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Get application-wide settings (OAuth providers, available AI list, IP, country).
   * GET /api/v1/settings
   */
  async get() {
    return this.client.get("/api/v1/settings");
  }
  /**
   * Get available UI locales.
   * GET /api/v1/i18n/locales
   */
  async getLocales(lang, namespace) {
    return this.client.get("/api/v1/i18n/locales", { lang, namespace });
  }
};

// src/resources/provider-rules.ts
function drop(s, k) {
  delete s[k];
}
function dropAll(s, keys) {
  for (const k of keys) drop(s, k);
}
function isLandscape(ratio) {
  const parts = ratio.split(":");
  if (parts.length !== 2) return false;
  const w = Number(parts[0]);
  const h = Number(parts[1]);
  if (!Number.isFinite(w) || !Number.isFinite(h) || h <= 0) return false;
  return w / h > 1.05;
}
var grokVideoRule = {
  aiName: "grok_video",
  afterMerge(s, ctx) {
    if (ctx.modelType === "grok_i2v") {
      drop(s, "aspect_ratio");
    }
    if (ctx.modelType === "grok_v2v") {
      dropAll(s, ["aspect_ratio", "video_duration", "resolution"]);
    }
  }
};
var klingVideoRule = {
  aiName: "kling",
  afterMerge(s, ctx) {
    if (/^kling_o1_/.test(ctx.modelType)) {
      drop(s, "mode");
    }
  }
};
var runwayVideoRule = {
  aiName: "runway",
  afterMerge(s, ctx) {
    if (ctx.modelType === "acttwo") {
      drop(s, "video_duration");
    }
  }
};
var grokImageRule = {
  aiName: "grok_image",
  afterMerge(s, ctx) {
    if (ctx.modelType === "grok_i2i_pro") {
      drop(s, "aspect_ratio");
      return;
    }
    if (ctx.modelType === "grok_i2i" && (ctx.fileCount ?? 0) < 2) {
      drop(s, "aspect_ratio");
    }
  }
};
var ideogramImageRule = {
  aiName: "ideogram",
  afterMerge(s) {
    const mode = s.mode;
    if (mode === "upscale") {
      drop(s, "aspect_ratio");
      return;
    }
    if (mode === "describe") {
      dropAll(s, [
        "aspect_ratio",
        "quality",
        "details_quality",
        "seed",
        "style",
        "version",
        "negative_prompt",
        "enhance",
        "rendering_speed"
      ]);
    }
  }
};
var lumaImageRule = {
  aiName: "luma_image",
  afterMerge(s, ctx) {
    if ((ctx.fileCount ?? 0) > 0 && s.mode !== void 0) {
      s.mode = "auto";
    }
    const mode = s.mode;
    const ratio = s.aspect_ratio;
    if (mode === "manga" && typeof ratio === "string" && /^\d+:\d+$/.test(ratio) && isLandscape(ratio)) {
      s.aspect_ratio = "2:3";
    }
  }
};
var midjourneyImageRule = {
  aiName: "midjourney",
  afterMerge(s) {
    const v = s.version;
    if (v === "8.1" || v === "niji 7") {
      drop(s, "quality");
    }
  }
};
var runwayFramesImageRule = {
  aiName: "runway-frames",
  afterMerge(s) {
    drop(s, "style");
  }
};
var seedreamImageRule = {
  aiName: "seedream",
  afterMerge(s, ctx) {
    if ((ctx.modelType === "seedream-4.5" || ctx.modelType === "seedream-5") && s.resolution === "1K") {
      s.resolution = "2K";
    }
    if (ctx.modelType === "seedream-5.0-pro" && s.resolution === "4K") {
      s.resolution = "2K";
    }
  }
};
var soraImagesRule = {
  aiName: "sora-images",
  afterMerge(s, ctx) {
    if (!/^gpt-image-2/.test(ctx.modelType)) {
      drop(s, "quality");
      drop(s, "details_quality");
    }
  }
};
var wanImageRule = {
  aiName: "wan_image",
  afterMerge(s, ctx) {
    if (ctx.modelType === "wan-2.7-pro" && (ctx.fileCount ?? 0) > 0 && s.resolution === "4K") {
      s.resolution = "2K";
    }
  }
};
var sunoAudioRule = {
  aiName: "suno",
  afterMerge(s) {
    const mode = s.mode ?? "generate";
    if (mode === "generate") {
      dropAll(s, ["audio_url", "continue_at", "source_clip_id", "source_task_id"]);
    }
  }
};
var RULES = [
  // video
  grokVideoRule,
  klingVideoRule,
  runwayVideoRule,
  // image
  grokImageRule,
  ideogramImageRule,
  lumaImageRule,
  midjourneyImageRule,
  runwayFramesImageRule,
  seedreamImageRule,
  soraImagesRule,
  wanImageRule,
  // audio
  sunoAudioRule
];
function applyProviderRules(aiName, settings, ctx, phase) {
  for (const rule of RULES) {
    if (rule.aiName !== aiName) continue;
    const fn = phase === "before" ? rule.beforeMerge : rule.afterMerge;
    if (fn) fn(settings, ctx);
  }
}

// src/resources/design.ts
var DesignResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Generate an image/design.
   * POST /api/v1/design/generate?ai_name={aiName}
   *
   * Normalization: provider-specific rules from `./provider-rules` mutate
   * `settings` in place after the optional `model_settings` merge, so the
   * wire shape matches the SPA's `aiSettingsOnInput` payload (e.g. drop
   * `aspect_ratio` for `grok_i2i_pro`, coerce `seedream` resolutions).
   */
  async generate(aiName, params) {
    const settings = {
      ...params.settings,
      ...params.model_settings ?? {}
    };
    applyProviderRules(
      aiName,
      settings,
      {
        modelType: typeof settings.model_type === "string" ? settings.model_type : ""
      },
      "after"
    );
    return this.client.post(
      "/api/v1/design/generate",
      { chat_uuid: params.chat_uuid, prompt: params.prompt, settings },
      { ai_name: aiName }
    );
  }
};

// src/resources/audio.ts
var AudioResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * List voice examples for ElevenLabs.
   * GET /api/v1/audio/elevenlabs/voice_examples
   */
  async listVoiceExamples(params) {
    return this.client.get("/api/v1/audio/elevenlabs/voice_examples", params);
  }
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
  async generate(aiName, params) {
    const settings = {
      ...params.settings,
      ...params.model_settings ?? {}
    };
    applyProviderRules(
      aiName,
      settings,
      {
        modelType: typeof settings.model_type === "string" ? settings.model_type : "",
        fileCount: params.file_urls?.length ?? 0
      },
      "after"
    );
    const { model_settings: _ignored, ...rest } = params;
    void _ignored;
    return this.client.post(
      "/api/v1/audio/generate",
      { ...rest, settings },
      { ai_name: aiName }
    );
  }
};

// src/resources/video.ts
var VideoResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
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
  async generate(aiName, params) {
    const settings = { ...params.settings };
    applyProviderRules(
      aiName,
      settings,
      {
        modelType: typeof settings.model_type === "string" ? settings.model_type : "",
        fileCount: params.file_urls?.length ?? 0
      },
      "after"
    );
    return this.client.post(
      "/api/v1/video/generate",
      { ...params, settings },
      { ai_name: aiName }
    );
  }
};

// src/resources/app.ts
var AppResource = class {
  baseURL = "https://syntx.ai";
  /**
   * Get the current deployed app version.
   * GET https://syntx.ai/version.json
   */
  async getVersion() {
    const response = await fetch(`${this.baseURL}/version.json`);
    if (!response.ok) {
      throw new Error(`Failed to fetch version: ${response.status}`);
    }
    return response.json();
  }
  /**
   * Check if the platform is under maintenance.
   * GET https://syntx.ai/maintenance-status.json
   */
  async getMaintenanceStatus() {
    const response = await fetch(`${this.baseURL}/maintenance-status.json`);
    if (!response.ok) {
      throw new Error(`Failed to fetch maintenance status: ${response.status}`);
    }
    return response.json();
  }
};

// src/transport/sse.ts
function openSse(opts) {
  const controller = new AbortController();
  if (opts.signal) {
    if (opts.signal.aborted) controller.abort();
    else opts.signal.addEventListener("abort", () => controller.abort(), { once: true });
  }
  let resolveDone = () => {
  };
  let rejectDone = () => {
  };
  const donePromise = new Promise((resolve2, reject) => {
    resolveDone = resolve2;
    rejectDone = reject;
  });
  const handle = {
    closed: false,
    done: donePromise,
    close
  };
  (async () => {
    try {
      let response;
      try {
        response = await fetch(opts.url, {
          method: "GET",
          headers: opts.headers,
          signal: controller.signal
        });
      } catch (err) {
        handle.closed = true;
        rejectDone(err);
        return;
      }
      if (!response.ok || !response.body) {
        handle.closed = true;
        rejectDone(new Error(`SSE connection failed: ${response.status} ${response.statusText}`));
        return;
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buffer = "";
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          buffer = dispatchFrames(buffer, opts.onEvent);
        }
        buffer += decoder.decode();
        if (buffer.trim().length > 0) {
          buffer = dispatchFrames(buffer + "\n\n", opts.onEvent);
        }
      } finally {
        try {
          reader.releaseLock();
        } catch {
        }
        handle.closed = true;
      }
      resolveDone();
    } catch (err) {
      handle.closed = true;
      rejectDone(err);
    }
  })();
  return handle;
  function close() {
    if (handle.closed) return;
    handle.closed = true;
    try {
      controller.abort();
    } catch {
    }
    resolveDone();
  }
}
function dispatchFrames(buffer, onEvent) {
  let start = 0;
  while (true) {
    const idx = buffer.indexOf("\n\n", start);
    if (idx === -1) return buffer.slice(start);
    const raw = buffer.slice(start, idx);
    start = idx + 2;
    const parsed = parseFrame(raw);
    if (!parsed) continue;
    if (parsed.event === "ping") continue;
    onEvent(parsed);
  }
}
function parseFrame(raw) {
  const lines = raw.split("\n");
  let event = "message";
  const dataLines = [];
  for (const line of lines) {
    if (line.startsWith(":")) continue;
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      let d = line.slice("data:".length);
      if (d.startsWith(" ")) d = d.slice(1);
      dataLines.push(d);
    }
  }
  if (dataLines.length === 0 && event === "message") return null;
  return { event, data: dataLines.join("\n") };
}

// src/resources/llm.ts
var LlmResource = class {
  constructor(client) {
    this.client = client;
  }
  client;
  /**
   * Current LLM usage limits (6h and 7d windows). GET /api/v1/llm/limits.
   *
   * Windows with `expires_at === null` or `expires_at` already in the past
   * are normalised to `{ percent_left: 100, started_at: null, expires_at: null }`
   * so callers never have to re-implement that rule.
   */
  async getLimits() {
    const res = await this.client.get("/api/v1/llm/limits");
    return {
      window_6h: normalizeWindow(res.window_6h),
      window_7d: normalizeWindow(res.window_7d)
    };
  }
  async generate(params) {
    const model = params.modelType ?? "gpt-5.6-luna";
    const body = { text: params.prompt, model };
    if (params.chatUuid) body.chat_uuid = params.chatUuid;
    if (params.thinking !== void 0) body.thinking = params.thinking;
    if (params.plan !== void 0) body.plan = params.plan;
    if (params.deepResearch !== void 0) body.deep_research = params.deepResearch;
    if (params.tools) body.tools = params.tools;
    return this.client.post(
      "/api/v1/llm/generate",
      body,
      { ai_name: params.aiName }
    );
  }
  async listModels(params) {
    const res = await this.client.get(
      "/api/v1/llm/models",
      {
        enabled_only: params?.enabled_only,
        lang: params?.lang
      }
    );
    if (Array.isArray(res)) return res;
    if (res && Array.isArray(res.models)) {
      return res.models;
    }
    return [];
  }
  async getChatStream(chatId) {
    const res = await this.client.get(
      `/api/v1/llm/chats/${encodeURIComponent(chatId)}/stream`
    );
    if (res && Array.isArray(res.jobs)) {
      return { jobs: res.jobs };
    }
    return { jobs: [] };
  }
  async waitForResponse(chatId, opts) {
    const timeout = opts?.timeout ?? 6e5;
    const sseTimeoutMs = opts?.sseTimeoutMs ?? Math.floor(timeout * 0.6);
    const llmSseBaseUrl = (opts?.llmSseBaseUrl ?? "https://sse.syntx.ai").replace(/\/$/, "");
    const start = Date.now();
    const remaining = () => Math.max(0, timeout - (Date.now() - start));
    const jobs = await this.getChatStream(chatId);
    const job = jobs.jobs[0];
    if (!job) {
      return pollFallback(chatId, opts, remaining());
    }
    const sseUrl = resolveSseUrl(llmSseBaseUrl, job.stream_url);
    const text = await consumeSse(sseUrl, sseTimeoutMs, opts?.signal, opts?.onProgress, timeout, start);
    if (process.env.SYNTX_DEBUG) {
      try {
        process.stderr.write(`[sse-debug chat=${chatId}] outcome=${text.kind} after ${Date.now() - start}ms (budget ${timeout}ms)
`);
      } catch {
      }
    }
    if (text.kind === "ok" || text.kind === "cancelled") {
      return buildCompletedMessage(chatId, text.text, job.message_id);
    }
    if (text.kind === "aborted") {
      throw new SyntxAbortError(`Wait cancelled in chat ${chatId}`);
    }
    const left = remaining();
    if (left <= 0) {
      throw new SyntxTimeoutError(
        `Timeout waiting for response in chat ${chatId}`,
        chatId,
        Date.now() - start,
        timeout
      );
    }
    return pollFallback(chatId, opts, left);
  }
};
async function consumeSse(url, sseTimeoutMs, signal, onProgress, timeout, start) {
  return new Promise((resolve2) => {
    const accumulated = [];
    let settled = false;
    const settle = (o) => {
      if (settled) return;
      settled = true;
      handle.close();
      resolve2(o);
    };
    const joined = () => accumulated.join("");
    const handle = openSse({
      url,
      headers: { Accept: "text/event-stream" },
      signal,
      onEvent: (event) => {
        try {
          onProgress?.(Date.now() - start, timeout);
        } catch {
        }
        if (event.event === "message") {
          if (event.data === "[DONE]") {
            settle({ kind: "ok", text: joined() });
            return;
          }
          try {
            const parsed = JSON.parse(event.data);
            if (parsed && typeof parsed === "object") {
              if (parsed.type === "content" && typeof parsed.content === "string") {
                accumulated.push(parsed.content);
              }
              return;
            }
          } catch {
          }
          accumulated.push(event.data);
        } else if (event.event === "complete") {
          settle({ kind: "ok", text: joined() });
        } else if (event.event === "cancelled") {
          settle({ kind: "cancelled", text: joined() });
        } else if (event.event === "error") {
          settle({ kind: "error" });
        }
      }
    });
    handle.done.then(
      () => {
        if (!settled) settle({ kind: "ok", text: joined() });
      },
      () => {
        if (!settled) settle({ kind: "error" });
      }
    );
    if (signal) {
      signal.addEventListener("abort", () => settle({ kind: "aborted" }), { once: true });
    }
    setTimeout(() => settle({ kind: "timeout" }), sseTimeoutMs);
  });
}
function resolveSseUrl(baseUrl, streamUrl) {
  if (/^https?:\/\//i.test(streamUrl)) return streamUrl;
  const trimmed = streamUrl.replace(/^\/+/, "");
  return `${baseUrl}/${trimmed}`;
}
function buildCompletedMessage(chatId, text, messageId) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const message = {
    id: messageId,
    chat_id: chatId,
    author_id: -1,
    created_at: now,
    updated_at: now,
    is_favorite: false,
    message_object: [
      {
        id: 0,
        message_id: 0,
        object_type: "text",
        object_url: null,
        object_text: text,
        completed: true,
        created_at: now,
        updated_at: now,
        model_type: null,
        metadata: null
      }
    ]
  };
  return { text, media: [], message };
}
async function pollFallback(chatId, opts, timeoutMs) {
  if (opts?.fallbackPoll) {
    return opts.fallbackPoll(chatId, { timeout: timeoutMs, signal: opts.signal });
  }
  return buildCompletedMessage(chatId, "", "");
}
function normalizeWindow(raw) {
  if (raw === null || raw === void 0) return null;
  const expiresAt = raw.expires_at ?? null;
  const expired = expiresAt === null || expiresAt === void 0 || typeof expiresAt === "string" && !Number.isNaN(Date.parse(expiresAt)) && Date.parse(expiresAt) <= Date.now();
  if (expired) {
    return { percent_left: 100, started_at: null, expires_at: null };
  }
  return {
    percent_left: typeof raw.percent_left === "number" ? raw.percent_left : 0,
    started_at: raw.started_at ?? null,
    expires_at: expiresAt
  };
}

// src/syntx-client.ts
var SyntxClient = class {
  client;
  auth;
  ai;
  user;
  chats;
  plans;
  notifications;
  folders;
  settings;
  design;
  audio;
  video;
  app;
  llm;
  constructor(config) {
    this.client = new BaseClient(config);
    this.auth = new SyntxAuth(this.client);
    this.ai = new AIResource(this.client);
    this.user = new UserResource(this.client);
    this.chats = new ChatsResource(this.client);
    this.plans = new PlansResource(this.client);
    this.notifications = new NotificationsResource(this.client);
    this.folders = new FoldersResource(this.client);
    this.settings = new SettingsResource(this.client);
    this.design = new DesignResource(this.client);
    this.audio = new AudioResource(this.client);
    this.video = new VideoResource(this.client);
    this.app = new AppResource();
    this.llm = new LlmResource(this.client);
  }
  /**
   * Direct access to the underlying HTTP client for advanced use cases.
   */
  get http() {
    return this.client;
  }
};

// src/mcp/context.ts
function createMcpContext(config, requestToken) {
  const syntx = new SyntxClient({
    token: requestToken ?? config.token,
    baseURL: config.baseURL,
    timeout: config.timeout
  });
  return {
    syntx,
    config,
    setToken(token) {
      if (requestToken !== void 0) {
        throw new SyntxAuthError(
          "This context carries a request-scoped credential (HTTP Authorization passthrough) and cannot be mutated. Retry with a different Authorization header instead."
        );
      }
      syntx.auth.setToken(token ?? "");
    }
    // sendProgress / sendLog are wired per-request by `createMcpServer` via
    // the request extra — see `mcp/server.ts`. They are declared as optional
    // so direct programmatic use of the context still works.
  };
}
function withRequestContext(base, extra) {
  if (!extra) return base;
  const progressToken = extra._meta?.progressToken;
  const sendProgress = async (progress, total, message) => {
    if (progressToken === void 0 || !extra.sendNotification) return;
    try {
      await extra.sendNotification({
        method: "notifications/progress",
        params: {
          progressToken,
          progress,
          ...total !== void 0 ? { total } : {},
          ...message !== void 0 ? { message } : {}
        }
      });
    } catch {
    }
  };
  const sendLog = async (level, data, logger) => {
    if (!extra.sendNotification) return;
    try {
      await extra.sendNotification({
        method: "notifications/message",
        params: { level, data, logger }
      });
    } catch {
    }
  };
  return { ...base, sendProgress, sendLog };
}

// src/mcp/errors.ts
function toMcpError(error, context) {
  const prefix = context ? `${context}: ` : "";
  if (error instanceof SyntxAuthError) {
    return toolError(
      `${prefix}Authentication required or invalid. Use the "set-token" tool to provide a valid syntx.ai token. (${error.message})`
    );
  }
  if (error instanceof SyntxTimeoutError) {
    const recovery = error.chatId ? ` The chat persists on the server \u2014 recover the reply with get-messages(chat_id="${error.chatId}") or resume waiting with wait-for-response(chat_id="${error.chatId}"). Do NOT re-send the prompt.` : "";
    return toolError(
      `${prefix}${error.message} (elapsed ${error.elapsedMs} ms of ${error.timeoutMs} ms budget).${recovery}`
    );
  }
  if (error instanceof SyntxAbortError) {
    return toolError(`${prefix}Cancelled: ${error.message}`);
  }
  if (error instanceof SyntxAPIError) {
    const detail = error.responseBody !== void 0 ? ` ${typeof error.responseBody === "string" ? error.responseBody : JSON.stringify(error.responseBody)}` : "";
    return toolError(
      `${prefix}syntx.ai API error ${error.status}${error.code ? ` [${error.code}]` : ""}: ${error.message}${detail}`
    );
  }
  return toolError(`${prefix}${error instanceof Error ? error.message : String(error)}`);
}
function toolError(text) {
  return { isError: true, content: [{ type: "text", text }] };
}
function textResult(text) {
  return { content: [{ type: "text", text }] };
}

// src/mcp/security-log.ts
var ALLOWED_META_KEYS = /* @__PURE__ */ new Set([
  "tool",
  "header",
  "method",
  "limitBytes",
  "observedBytes",
  "limit",
  "mime",
  "source"
]);
function sanitiseEvent(event) {
  const safe = {
    kind: event.kind,
    transport: event.transport
  };
  if (typeof event.clientAddr === "string" && event.clientAddr.length > 0) {
    safe.clientAddr = event.clientAddr;
  }
  if (typeof event.reason === "string" && event.reason.length > 0) {
    safe.reason = event.reason;
  }
  if (event.meta) {
    const meta = {};
    for (const [k, v] of Object.entries(event.meta)) {
      if (ALLOWED_META_KEYS.has(k)) meta[k] = v;
    }
    if (Object.keys(meta).length > 0) safe.meta = meta;
  }
  return safe;
}
function logSecurityEvent(event) {
  try {
    const safe = sanitiseEvent(event);
    const line = JSON.stringify({
      ts: (/* @__PURE__ */ new Date()).toISOString(),
      component: "syntx-mcp",
      ...safe
    });
    process.stderr.write(line + "\n");
  } catch {
  }
}

// src/mcp/tools/auth.ts
function assertLocalAuthMutationAllowed(ctx, op) {
  if (ctx.config.transport !== "stdio") {
    logSecurityEvent({
      kind: "auth-mutation.rejected",
      transport: ctx.config.transport,
      reason: op,
      meta: { tool: op }
    });
    return toolError(
      `${op}: not permitted over the ${ctx.config.transport} transport. Authentication state is process-global; configure the bearer via the SYNTX_TOKEN environment variable before starting the server.`
    );
  }
  return null;
}
var authTools = [
  {
    name: "whoami",
    description: 'Return an identity check for the current syntx.ai user: { authenticated, user } where `user` is a sanitised public profile (id, user_id, name, username, email, avatar, auth_services). Internal identifiers such as `chatwoot_hmac` / `ym_client_id` are intentionally stripped. This tool NEVER errors on missing/invalid tokens \u2014 it returns { authenticated: false } instead. Use it to verify authentication status. Real failures (network/API errors) still raise an MCP error so you can tell "not logged in" from "API unreachable".',
    inputSchema: {
      type: "object",
      properties: {},
      additionalProperties: false
    },
    async handler(_args, ctx) {
      if (!ctx.syntx.auth.isAuthenticated()) {
        return textResult(JSON.stringify({ authenticated: false, user: null }, null, 2));
      }
      try {
        const user = await ctx.syntx.user.mePublic();
        return textResult(JSON.stringify({ authenticated: true, user }, null, 2));
      } catch (err) {
        if (err instanceof SyntxAuthError) {
          return textResult(JSON.stringify({ authenticated: false, user: null }, null, 2));
        }
        return toMcpError(err, "whoami");
      }
    }
  },
  {
    name: "set-token",
    description: "Set or replace the syntx.ai bearer token used by the server at runtime. Call this before any authenticated operation if SYNTX_TOKEN was not configured. The token is held in memory only \u2014 it is not persisted to disk and is lost when the process restarts. **stdio only**: this tool is rejected over the HTTP transport to prevent a remote client from hijacking the process-shared bearer (H4). Configure the token via the SYNTX_TOKEN env variable when running with --transport http.",
    inputSchema: {
      type: "object",
      properties: {
        token: {
          type: "string",
          description: "A syntx.ai bearer token."
        }
      },
      required: ["token"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const blocked = assertLocalAuthMutationAllowed(ctx, "set-token");
      if (blocked) return blocked;
      const token = String(args.token ?? "").trim();
      if (!token) return toMcpError(new Error("token must be a non-empty string"), "set-token");
      ctx.setToken(token);
      return textResult('Token updated. Use "whoami" or "validate-token" to confirm it works.');
    }
  }
];

// src/mcp/tools/_helpers.ts
function wrapSdk(name, fn) {
  return async (args, ctx) => {
    try {
      const result = await fn(args, ctx);
      return textResult(JSON.stringify(result, null, 2));
    } catch (err) {
      return toMcpError(err, `tool:${name}`);
    }
  };
}
function jsonOrAck(response, ack) {
  return response === void 0 || response === null ? textResult(ack) : textResult(JSON.stringify(response, null, 2));
}

// src/mcp/tools/user.ts
var userTools = [
  {
    name: "get-profile",
    description: "Return the current user profile (sanitised public fields: id, user_id, name, username, email, avatar, auth_services). Internal identifiers such as `chatwoot_hmac` / `ym_client_id` are stripped before returning. Requires authentication \u2014 raises a clear error when no token is set. For a non-erroring identity check, use `whoami` which returns { authenticated, user }.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    async handler(_args, ctx) {
      if (!ctx.syntx.auth.isAuthenticated()) {
        return toolError(
          'get-profile: Unauthorized. Set your syntx.ai token first via the "set-token" tool (or the SYNTX_TOKEN env variable).'
        );
      }
      try {
        return textResult(JSON.stringify(await ctx.syntx.user.mePublic(), null, 2));
      } catch (err) {
        return toMcpError(err, "get-profile");
      }
    }
  },
  {
    name: "get-balance",
    description: "Return the current token balance for the authenticated user.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    handler: wrapSdk("get-balance", async (_args, ctx) => ctx.syntx.user.getBalance())
  }
];

// src/mcp/tools/ai.ts
var KNOWN_PROVIDERS = Object.freeze({
  text: /* @__PURE__ */ new Set([
    "chatgpt",
    "claude",
    "deepseek",
    "gemini",
    "qwen",
    "grok",
    "perplexity"
  ]),
  image: /* @__PURE__ */ new Set([
    "midjourney",
    "flux",
    "sora-images",
    "banana",
    "ideogram",
    "stable-diffusion",
    "recraft",
    "runway-frames",
    "seedream",
    "higgsfield-soul",
    "higgsfield",
    "kling-kolors",
    "kling"
  ]),
  video: /* @__PURE__ */ new Set([
    "topaz_astra",
    "seedance",
    "beeble"
  ]),
  audio: /* @__PURE__ */ new Set([
    "suno",
    "elevenlabs"
  ]),
  upscale: /* @__PURE__ */ new Set([
    "magnific",
    "topaz_ai"
  ])
});
function inferScope(aiName) {
  if (!aiName) return null;
  for (const scope of Object.keys(KNOWN_PROVIDERS)) {
    if (KNOWN_PROVIDERS[scope].has(aiName)) return scope;
  }
  return null;
}
function filterModels(models, params = {}) {
  const { scope, ai_name, active_only = true, search } = params;
  const needle = search?.trim().toLowerCase();
  return models.filter((m) => {
    if (active_only && m.active === false) return false;
    if (ai_name !== void 0 && ai_name !== "" && m.ai_name !== ai_name) return false;
    if (scope !== void 0 && inferScope(m.ai_name) !== scope) return false;
    if (needle) {
      const hay = `${m.value} ${m.label}`.toLowerCase();
      if (!hay.includes(needle)) return false;
    }
    return true;
  });
}
var aiTools = [
  {
    name: "list-ai-services",
    description: "List all syntx.ai AI services (e.g. ChatGPT, Midjourney, Sora) with their scope and status.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    handler: wrapSdk("list-ai-services", async (_args, ctx) => ctx.syntx.ai.listServices())
  },
  {
    name: "list-models",
    description: 'List AI models with upload constraints, supported media types, and features. Filters (all optional, combined with AND): `scope` (text|image|video|audio|upscale), `ai_name` (exact match, e.g. "chatgpt"), `active_only` (default true), `search` (case-insensitive substring against `value`/`label`).',
    inputSchema: {
      type: "object",
      properties: {
        scope: {
          type: "string",
          enum: ["text", "image", "video", "audio", "upscale"],
          description: "Capability bucket inferred from the syntx.ai provider. Omit to receive models from every bucket (including providers that don't match any known bucket)."
        },
        ai_name: {
          type: "string",
          description: 'Exact syntx.ai provider name, e.g. "chatgpt", "claude", "midjourney".'
        },
        active_only: {
          type: "boolean",
          default: true,
          description: "When true (default), drop inactive models. Set false to include them."
        },
        search: {
          type: "string",
          description: "Case-insensitive substring matched against the model `value` and `label`."
        }
      },
      additionalProperties: false
    },
    handler: wrapSdk(
      "list-models",
      async (args, ctx) => filterModels(await ctx.syntx.ai.listModels(), {
        scope: args.scope,
        ai_name: args.ai_name,
        active_only: args.active_only === void 0 ? true : args.active_only,
        search: args.search
      })
    )
  },
  {
    name: "get-model-info",
    description: "Return detailed information about a specific AI model (pricing/cost params, limits). Providers whose catalog declares `get_cost_params` reject the request with 400 until the listed params are supplied \u2014 every `get_cost_params` field is accepted here as a flat top-level argument (see per-field descriptions for provider-specific enums).",
    inputSchema: {
      type: "object",
      properties: {
        ai_name: { type: "string", description: 'AI service name, e.g. "chatgpt".' },
        model_type: { type: "string", description: 'Model identifier, e.g. "gpt-5-mini".' },
        batch_size: { type: "number" },
        quality: {
          type: "string",
          description: 'Quality/size tier. For sora-images gpt-image-2 the API validates it as "1K" | "2K" | "4K" (NOT low/medium/high).'
        },
        video_duration: { type: "number" },
        chars_count: { type: "number" },
        mode: { type: "string" },
        image_size: {
          type: "string",
          description: 'Image size tier, e.g. "1K", "2K", "4K". Required (in the query string) by providers with `get_cost_params: ["image_size"]` \u2014 banana / banana2 / banana3, seedream. Exposed flat because some MCP clients cannot pass nested objects reliably.'
        },
        details_quality: {
          type: "string",
          description: 'Details level (sora-images gpt-image-2: "high" confirmed; grok_imagine_2: "low" | "medium"). Required by /api/v2/get_model_info together with quality for that provider.'
        },
        resolution: {
          type: "string",
          description: 'Output resolution. Enum is per-provider, e.g. wan_video "720P"|"1080P", grok_image "1k"|"2k", hailuo-minimax "512p"|"768p"|"1080p"|"2K" (per model), topaz_astra/beeble "1920x1080"|"3840x2160". An invalid value yields a 400 listing the valid options.'
        },
        ref_count: {
          type: "number",
          description: "Reference image count (sora-images gpt-image-2.5-*, grok_imagine_2, hailuo-3.0)."
        },
        size: {
          type: "string",
          description: 'Size tier. wan_image models use "1K"; seedance uses aspect ratios ("21:9", "16:9", "9:16", "1:1", "4:3", "3:4", "adaptive").'
        },
        duration: {
          type: "number",
          description: "Duration in seconds (seedance-2.x, wan_2x r2v/videoedit, heygen, elevenlabs sts)."
        },
        frame_rate: {
          type: "number",
          description: "Frame rate (topaz_astra, beeble switchx)."
        },
        version: {
          type: "string",
          description: 'Model version (kling: "1.5"\u2026"3.0"; kling_motion_control also "standart"|"hd"; suno: "V6").'
        },
        native_audio: {
          type: "boolean",
          description: "Native audio flag (kling, wan_26 i2v/r2v flash)."
        },
        generate_audio: {
          type: "boolean",
          description: "Generate audio track (seedance-1.5-pro)."
        },
        draft: {
          type: "boolean",
          description: "Draft mode (flux3_video)."
        },
        upscale: {
          type: "number",
          description: "Upscale flag as INTEGER 0|1 (veo3 family; the API rejects non-integers)."
        },
        gen_type: {
          type: "string",
          description: 'Generation type for kling_motion_control ("mcv" | "mci").'
        },
        width: { type: "number", description: "Target width (magnific)." },
        height: { type: "number", description: "Target height (magnific)." },
        scale_factor: {
          type: "string",
          description: 'Upscale factor (magnific): "2x" | "4x" | "8x" | "16x".'
        },
        rendering_speed: {
          type: "string",
          description: 'Rendering speed (ideogram), e.g. "TURBO".'
        }
      },
      required: ["ai_name", "model_type"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "get-model-info",
      async (args, ctx) => ctx.syntx.ai.getModelInfo({
        ai_name: args.ai_name,
        model_type: args.model_type,
        batch_size: args.batch_size,
        quality: args.quality,
        video_duration: args.video_duration,
        chars_count: args.chars_count,
        mode: args.mode,
        image_size: args.image_size,
        details_quality: args.details_quality,
        resolution: args.resolution,
        ref_count: args.ref_count,
        size: args.size,
        duration: args.duration,
        frame_rate: args.frame_rate,
        version: args.version,
        native_audio: args.native_audio,
        generate_audio: args.generate_audio,
        draft: args.draft,
        upscale: args.upscale,
        gen_type: args.gen_type,
        width: args.width,
        height: args.height,
        scale_factor: args.scale_factor,
        rendering_speed: args.rendering_speed
      })
    )
  }
];

// src/mcp/tools/chats.ts
var chatsTools = [
  {
    name: "list-chats",
    description: "List the user chats, optionally filtered by scope or a search query.",
    inputSchema: {
      type: "object",
      properties: {
        scope: { type: "string", description: "Chat scope: text, image, audio, or video." },
        search: { type: "string", description: "Substring to filter chat titles." },
        direction: { type: "string", enum: ["older", "newer"] },
        page_size: { type: "number", minimum: 1, maximum: 100 }
      },
      additionalProperties: false
    },
    handler: wrapSdk(
      "list-chats",
      async (args, ctx) => ctx.syntx.chats.list({
        scope: args.scope,
        search: args.search,
        direction: args.direction,
        page_size: args.page_size
      })
    )
  },
  {
    name: "create-chat",
    description: "Create a new syntx.ai chat session and return its UUID. A title is required by the API.",
    inputSchema: {
      type: "object",
      properties: {
        title: { type: "string", description: "Chat title (required)." },
        scope: { type: "string", description: 'Chat scope. Defaults to "text".', default: "text" },
        model: { type: "string", description: "Initial model for the chat." }
      },
      required: ["title"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "create-chat",
      async (args, ctx) => ctx.syntx.chats.create({
        title: args.title,
        scope: args.scope ?? "text",
        model: args.model
      })
    )
  },
  {
    name: "get-messages",
    description: "Return the message history of a chat (by UUID or numeric id).",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or id." },
        page_size: { type: "number", minimum: 1, maximum: 100 },
        direction: { type: "string", enum: ["older", "newer"] }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "get-messages",
      async (args, ctx) => ctx.syntx.chats.getMessages(args.chat_id, {
        page_size: args.page_size,
        direction: args.direction
      })
    )
  },
  {
    name: "chat-exists",
    description: "Pre-flight check: returns whether a chat (by id or uuid) currently exists on the server. Use this before `send-message` if you may be holding a stale reference. Backed by `GET /api/v1/chats/{chatId}` \u2014 404 maps to `false`, 200 maps to `true`.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat numeric id or uuid." }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "chat-exists",
      async (args, ctx) => {
        const chatId = String(args.chat_id);
        const exists = await ctx.syntx.chats.exists(chatId);
        return { exists, chat_id: chatId };
      }
    )
  },
  {
    name: "send-message",
    description: "Send a message (prompt) with optional uploaded-file attachments to an existing chat and return immediately. The assistant response is generated asynchronously \u2014 poll with `wait-for-response` or use `ask` / `stream-message` for a single blocking call.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or id." },
        prompt: { type: "string", description: "The prompt text to send." },
        ai_name: { type: "string", description: "AI service name. Defaults to the server default." },
        model_type: { type: "string", description: "Model identifier for this message." },
        attachments: {
          type: "array",
          maxItems: 10,
          description: "Files returned by `upload-files` to attach to this message.",
          items: {
            type: "object",
            properties: {
              url: { type: "string", minLength: 1, description: "Uploaded file URL." },
              filename: { type: "string", minLength: 1, description: "File name shown in the chat." },
              mime_type: { type: "string", minLength: 1, description: "Uploaded file MIME type." },
              size: { type: "number", minimum: 0, description: "Uploaded file size in bytes." },
              type: {
                type: "string",
                enum: ["image", "video", "audio"],
                description: 'Optional category hint for media files (image/video/audio). When set, the attachment is sent with the corresponding object_type. For text documents and other non-media files, omit this field \u2014 the type is inferred from mime_type and sent as "filetext". NOTE: "file" is NOT a valid input object_type on the syntx.ai API; use "filetext" instead.'
              }
            },
            required: ["url", "filename"],
            anyOf: [{ required: ["mime_type"] }, { required: ["type"] }],
            additionalProperties: false
          }
        }
      },
      required: ["chat_id", "prompt"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const aiName = args.ai_name ?? ctx.config.defaultAI;
        const modelType = args.model_type ?? ctx.config.defaultModel;
        const attachments = args.attachments ?? [];
        const chatId = String(args.chat_id);
        if (!await ctx.syntx.chats.exists(chatId)) {
          throw new SyntxAPIError(
            `Chat ${chatId} not found. Use chat-exists to validate before sending, or list-chats to find a valid id.`,
            404,
            "chat_not_found"
          );
        }
        const flow = routeTextFlow(ctx, { scope: "text" });
        if (flow && attachments.length === 0) {
          await ctx.syntx.llm.generate({
            prompt: String(args.prompt),
            aiName,
            modelType,
            chatUuid: chatId
          });
          return textResult(
            `Message sent to chat ${chatId}. Use "wait-for-response" or "get-messages" to read the reply.`
          );
        }
        const objects = [
          {
            object_type: "text",
            object_url: null,
            object_text: String(args.prompt),
            model_type: modelType
          },
          ...attachments.map((attachment) => {
            const mimeCategory = attachment.mime_type?.split("/", 1)[0]?.toLowerCase();
            const category = attachment.type ?? mimeCategory;
            const objectType = category === "image" || category === "video" || category === "audio" ? category : "filetext";
            return {
              object_type: objectType,
              object_url: attachment.url,
              object_text: attachment.filename,
              model_type: modelType
            };
          })
        ];
        await ctx.syntx.chats.sendMessage(chatId, aiName, objects);
        return textResult(
          `Message sent to chat ${chatId}. Use "wait-for-response" or "get-messages" to read the reply.`
        );
      } catch (err) {
        return toMcpError(err, "send-message");
      }
    }
  },
  {
    name: "wait-for-response",
    description: "Block until the latest assistant message in a chat finishes generating, then return its text and media URLs. Resolves when every message_object[i].completed === true \u2014 including image / video / audio / file-only replies. Text-scope chats open an SSE connection on sse.syntx.ai and fall back to REST polling on transport failure. Use after `send-message`.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string" },
        timeout: { type: "number", description: "Override max wait time in milliseconds." },
        poll_interval: { type: "number", description: "Override poll interval in milliseconds." }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    async handler(args, ctx, extra) {
      try {
        const chatId = String(args.chat_id);
        if (!await ctx.syntx.chats.exists(chatId)) {
          throw new SyntxAPIError(
            `Chat ${chatId} not found. Use chat-exists to validate, or list-chats to find a valid id.`,
            404,
            "chat_not_found"
          );
        }
        const flow = routeTextFlow(ctx, { scope: "text" });
        if (flow) {
          const completed = await flow.waitForResponse(chatId, {
            timeout: args.timeout ?? ctx.config.pollTimeout,
            signal: extra?.signal,
            onProgress: (elapsed, total) => {
              void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
            }
          });
          const textBlock2 = completed.text || (completed.media.length === 0 ? "(no assistant reply yet)" : "(media-only reply, see media below)");
          return textResult(
            `Assistant reply:

${textBlock2}

--- media ---
${JSON.stringify(completed.media, null, 2)}

--- metadata ---
${JSON.stringify(completed.message, null, 2)}`
          );
        }
        const { text, media, message } = await ctx.syntx.chats.waitForResponse(
          chatId,
          {
            timeout: args.timeout ?? ctx.config.pollTimeout,
            pollInterval: args.poll_interval ?? ctx.config.pollInterval,
            signal: extra?.signal,
            onProgress: (elapsed, total) => {
              void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
            }
          }
        );
        const textBlock = text || (media.length === 0 ? "(no assistant reply yet)" : "(media-only reply, see media below)");
        return textResult(
          `Assistant reply:

${textBlock}

--- media ---
${JSON.stringify(media, null, 2)}

--- metadata ---
${JSON.stringify(message, null, 2)}`
        );
      } catch (err) {
        return toMcpError(err, "wait-for-response");
      }
    }
  },
  {
    name: "ask",
    description: 'One-shot helper: create a chat, send a prompt, wait for the completed assistant reply, and return it. Ideal for stateless Q&A. The created chat UUID is included in the response for follow-ups. Set `mode: "stream"` to opt into real-time token delivery (default behaviour is controlled by SYNTX_STREAM_MODE).',
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "The prompt text to send." },
        title: { type: "string", description: "Chat title. Defaults to a truncated prompt." },
        ai_name: { type: "string" },
        model_type: { type: "string" },
        scope: { type: "string", default: "text" },
        timeout: { type: "number" },
        poll_interval: { type: "number" },
        mode: {
          type: "string",
          enum: ["auto", "stream", "poll", "off"],
          description: 'Override the streaming strategy. "stream" uses WSS; "poll" uses REST polling; "auto" tries WSS then falls back to polling; "off" disables waiting (the tool returns after sending).'
        }
      },
      required: ["prompt"],
      additionalProperties: false
    },
    async handler(args, ctx, extra) {
      try {
        const prompt = String(args.prompt);
        const mode = args.mode ?? ctx.config.streamMode;
        const scope = args.scope ?? "text";
        const aiName = args.ai_name ?? ctx.config.defaultAI;
        const modelType = args.model_type ?? ctx.config.defaultModel;
        const timeout = args.timeout ?? ctx.config.pollTimeout;
        const flow = routeTextFlow(ctx, { scope });
        if (mode === "off") {
          const { uuid } = await ctx.syntx.chats.create({
            title: args.title ?? prompt.slice(0, 60),
            scope
          });
          if (flow) {
            await ctx.syntx.llm.generate({ prompt, aiName, modelType, chatUuid: uuid });
          } else {
            await ctx.syntx.chats.sendMessage(uuid, aiName, [
              {
                object_type: "text",
                object_url: null,
                object_text: prompt,
                ...modelType ? { model_type: modelType } : {}
              }
            ]);
          }
          return textResult(
            `chat_uuid: ${uuid}

Message sent. Use "wait-for-response" or "stream-message" to read the reply.`
          );
        }
        if (flow) {
          const { uuid } = await ctx.syntx.chats.create({
            title: args.title ?? prompt.slice(0, 60),
            scope
          });
          await ctx.syntx.llm.generate({ prompt, aiName, modelType, chatUuid: uuid });
          const completed = await flow.waitForResponse(uuid, {
            timeout,
            signal: extra?.signal,
            onProgress: (elapsed, total) => {
              void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
            }
          });
          return textResult(
            `chat_uuid: ${uuid}

` + (completed.text || (completed.media.length === 0 ? "(no assistant reply yet)" : "(media-only reply)"))
          );
        }
        if (mode === "poll") {
          const { uuid, text } = await pollAsk(prompt, args, ctx, extra);
          return textResult(`chat_uuid: ${uuid}

${text}`);
        }
        if (mode === "stream" || mode === "auto") {
          return await streamAsk(prompt, args, ctx, extra);
        }
        throw new Error(`Unknown stream mode: ${String(mode)}`);
      } catch (err) {
        return toMcpError(err, "ask");
      }
    }
  },
  {
    name: "stream-message",
    description: "One-shot streaming chat for text scope: opens an SSE connection on sse.syntx.ai, sends the prompt, and streams the assistant reply. Falls back to REST polling on transport failure. Intermediate progress is reported via `notifications/progress` (when the client supplies a progressToken); the final tool result contains the complete text. Non-text scopes use the legacy polling path.",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "The prompt text to send." },
        scope: { type: "string", default: "text" },
        model: { type: "string", description: "Initial model for the chat." },
        ai_name: { type: "string" },
        model_type: { type: "string" },
        timeout: { type: "number", description: "Max wait time in milliseconds." },
        mode: {
          type: "string",
          enum: ["auto", "stream", "poll"],
          description: 'Override the streaming strategy. Default "auto" (WSS with polling fallback).'
        }
      },
      required: ["prompt"],
      additionalProperties: false
    },
    async handler(args, ctx, extra) {
      try {
        const prompt = String(args.prompt);
        const scope = args.scope ?? "text";
        const aiName = args.ai_name ?? ctx.config.defaultAI;
        const modelType = args.model_type ?? ctx.config.defaultModel;
        const timeout = args.timeout ?? ctx.config.pollTimeout;
        const flow = routeTextFlow(ctx, { scope });
        if (flow) {
          const { uuid } = await ctx.syntx.chats.create({
            title: prompt.slice(0, 60),
            scope,
            ...modelType ? { model: modelType } : {}
          });
          await ctx.syntx.llm.generate({ prompt, aiName, modelType, chatUuid: uuid });
          let chunkCount = 0;
          const completed = await flow.waitForResponse(uuid, {
            timeout,
            signal: extra?.signal,
            onProgress: (elapsed, total) => {
              void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
            }
          });
          if (completed.text) {
            chunkCount = 1;
            await ctx.sendProgress?.(completed.text.length, void 0, completed.text);
            await ctx.sendLog?.("info", { chunk: chunkCount, length: completed.text.length }, "stream-message");
          }
          return textResult(
            `chat_uuid: ${uuid}
elapsed_ms: ${Date.now()}
chunks: ${chunkCount}

${completed.text}`
          );
        }
        return await streamAsk(prompt, args, ctx, extra);
      } catch (err) {
        return toMcpError(err, "stream-message");
      }
    }
  },
  {
    name: "delete-chat",
    description: "Permanently delete a chat. Mirrors `syntx.chats.delete`. Issues `DELETE /api/v1/chats/{chat_id}`. This action is destructive and cannot be undone.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const chatId = String(args.chat_id ?? "").trim();
      if (!chatId) {
        return toMcpError(new Error('"chat_id" must be a non-empty string'), "delete-chat");
      }
      try {
        await ctx.syntx.chats.delete(chatId);
        return textResult(`Deleted chat ${chatId}.`);
      } catch (err) {
        return toMcpError(err, "delete-chat");
      }
    }
  },
  {
    name: "get-inprogress",
    description: "Return the in-progress generations for a chat. Mirrors `syntx.chats.getInProgress`. Hits `GET /api/v1/chats/{chat_id}/inprogress`. An empty array means nothing is currently generating; otherwise each entry describes an active assistant object (model, object_type, created_at, task_id). Used internally by `wait-for-response` to gate on prior requests.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "get-inprogress",
      async (args, ctx) => ctx.syntx.chats.getInProgress(args.chat_id)
    )
  },
  {
    name: "get-favorite-messages",
    description: "Return the favorite (bookmarked) messages for a chat. Mirrors `syntx.chats.getFavoriteMessages`. Hits `GET /api/v1/chats/favorite/{chat_id}/messages`. This is the only way to read starred messages through MCP \u2014 `get-messages` does not include them.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." },
        page_size: { type: "number", minimum: 1, maximum: 100 },
        direction: { type: "string", enum: ["older", "newer"] }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    handler: wrapSdk(
      "get-favorite-messages",
      async (args, ctx) => ctx.syntx.chats.getFavoriteMessages(args.chat_id, {
        page_size: args.page_size,
        direction: args.direction
      })
    )
  },
  {
    name: "cancel-message",
    description: "Cancel an in-flight assistant message generation. Mirrors `syntx.chats.cancelMessage`. Issues `POST /api/v1/chats/{chat_id}/messages/{message_id}/cancel`. A 404 response is treated as success (the message already finished).",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." },
        message_id: { type: "string", description: "Message id to cancel (required)." }
      },
      required: ["chat_id", "message_id"],
      additionalProperties: false
    },
    handler: async (args, ctx) => {
      try {
        const chatId = String(args.chat_id);
        const messageId = String(args.message_id);
        await ctx.syntx.chats.cancelMessage(chatId, messageId);
        return textResult(`Cancelled message ${messageId} in chat ${chatId}.`);
      } catch (err) {
        return toMcpError(err, "cancel-message");
      }
    }
  },
  {
    name: "rename-chat",
    description: "Rename a chat. Mirrors `syntx.chats.rename`. Issues `PUT /api/v1/chats/{chat_id}` with body `{title}`.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." },
        title: { type: "string", description: "New chat title (required)." }
      },
      required: ["chat_id", "title"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const chatId = String(args.chat_id ?? "").trim();
      if (!chatId) {
        return toMcpError(new Error('"chat_id" must be a non-empty string'), "rename-chat");
      }
      const title = String(args.title ?? "").trim();
      if (!title) {
        return toMcpError(new Error('"title" must be a non-empty string'), "rename-chat");
      }
      try {
        const response = await ctx.syntx.chats.rename(chatId, title);
        return jsonOrAck(response, `Renamed chat ${chatId} to "${title}".`);
      } catch (err) {
        return toMcpError(err, "rename-chat");
      }
    }
  },
  {
    name: "delete-message",
    description: "Permanently delete a single message. Mirrors `syntx.chats.deleteMessage`. Issues `DELETE /api/v1/chats/messages/{message_id}` (no chat id in the path). This action is destructive and cannot be undone.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "string", description: "Message id to delete (required)." }
      },
      required: ["message_id"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const messageId = String(args.message_id ?? "").trim();
      if (!messageId) {
        return toMcpError(new Error('"message_id" must be a non-empty string'), "delete-message");
      }
      try {
        await ctx.syntx.chats.deleteMessage(messageId);
        return textResult(`Deleted message ${messageId}.`);
      } catch (err) {
        return toMcpError(err, "delete-message");
      }
    }
  },
  {
    name: "toggle-chat-favorite",
    description: "Toggle the favorite (bookmark) flag of a chat. Mirrors `syntx.chats.toggleFavorite`. Issues `POST /api/v1/chats/{chat_id}/favorite` (no body). Each call flips the current state \u2014 favorited becomes unfavorited and vice versa; verify the result via `list-chats`.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." }
      },
      required: ["chat_id"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const chatId = String(args.chat_id ?? "").trim();
      if (!chatId) {
        return toMcpError(new Error('"chat_id" must be a non-empty string'), "toggle-chat-favorite");
      }
      try {
        const response = await ctx.syntx.chats.toggleFavorite(chatId);
        return jsonOrAck(response, `Toggled favorite flag of chat ${chatId}.`);
      } catch (err) {
        return toMcpError(err, "toggle-chat-favorite");
      }
    }
  },
  {
    name: "toggle-message-favorite",
    description: "Toggle the favorite (bookmark) flag of a single message. Mirrors `syntx.chats.toggleMessageFavorite`. Issues `POST /api/v1/chats/{chat_id}/messages/{message_id}/favorite` (no body). Each call flips the current state; favorites are readable via `get-favorite-messages`.",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "Chat UUID or numeric id (required)." },
        message_id: { type: "string", description: "Message id (required)." }
      },
      required: ["chat_id", "message_id"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const chatId = String(args.chat_id ?? "").trim();
      if (!chatId) {
        return toMcpError(new Error('"chat_id" must be a non-empty string'), "toggle-message-favorite");
      }
      const messageId = String(args.message_id ?? "").trim();
      if (!messageId) {
        return toMcpError(new Error('"message_id" must be a non-empty string'), "toggle-message-favorite");
      }
      try {
        const response = await ctx.syntx.chats.toggleMessageFavorite(chatId, messageId);
        return jsonOrAck(
          response,
          `Toggled favorite flag of message ${messageId} in chat ${chatId}.`
        );
      } catch (err) {
        return toMcpError(err, "toggle-message-favorite");
      }
    }
  }
];
async function pollAsk(prompt, args, ctx, extra) {
  const { uuid } = await ctx.syntx.chats.create({
    title: args.title ?? prompt.slice(0, 60),
    scope: args.scope ?? "text"
  });
  const aiName = args.ai_name ?? ctx.config.defaultAI;
  await ctx.syntx.chats.sendMessage(uuid, aiName, [
    {
      object_type: "text",
      object_url: null,
      object_text: prompt,
      model_type: args.model_type ?? ctx.config.defaultModel
    }
  ]);
  const { text } = await ctx.syntx.chats.waitForResponse(uuid, {
    timeout: args.timeout ?? ctx.config.pollTimeout,
    pollInterval: args.poll_interval ?? ctx.config.pollInterval,
    signal: extra?.signal,
    onProgress: (elapsed, total) => {
      void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
    }
  });
  return { uuid, text };
}
async function streamAsk(prompt, args, ctx, _extra) {
  const timeout = args.timeout ?? ctx.config.pollTimeout;
  const aiName = args.ai_name ?? ctx.config.defaultAI;
  const modelType = args.model_type ?? ctx.config.defaultModel;
  const scope = args.scope ?? "text";
  let chunkCount = 0;
  let chatUuid;
  const result = await ctx.syntx.chats.streamResponse(prompt, {
    timeout,
    scope,
    model: modelType,
    aiName,
    signal: _extra?.signal,
    onProgress: (elapsed, total) => {
      void ctx.sendProgress?.(elapsed, total, "Waiting for assistant reply\u2026");
    },
    onSession: (uuid2) => {
      chatUuid = uuid2;
    },
    onChunk: async (_chunk, accumulated) => {
      chunkCount++;
      await ctx.sendProgress?.(accumulated.length, void 0, accumulated);
      await ctx.sendLog?.("info", { chunk: chunkCount, length: accumulated.length }, "stream-message");
    }
  });
  const uuid = result.chatUuid ?? chatUuid;
  return textResult(
    `chat_uuid: ${uuid ?? "(no session)"}
elapsed_ms: ${result.elapsedMs}
chunks: ${chunkCount}

${result.text}`
  );
}
function routeTextFlow(ctx, opts) {
  if (ctx.config.legacyTextTransport) return null;
  if ((opts.scope ?? "text") !== "text") return null;
  return {
    generate: ctx.syntx.llm.generate.bind(ctx.syntx.llm),
    waitForResponse: async (chatId, waitOpts) => waitForTextResponse(chatId, waitOpts, ctx)
  };
}
async function waitForTextResponse(chatId, waitOpts, ctx) {
  return ctx.syntx.llm.waitForResponse(chatId, {
    timeout: waitOpts.timeout ?? ctx.config.pollTimeout,
    signal: waitOpts.signal,
    onProgress: waitOpts.onProgress,
    llmSseBaseUrl: ctx.config.llmSseBaseUrl,
    fallbackPoll: async (cid, opts2) => ctx.syntx.chats.pollForResponse(cid, {
      timeout: opts2.timeout ?? ctx.config.pollTimeout,
      signal: opts2.signal,
      pollInterval: ctx.config.pollInterval
    })
  });
}

// src/mcp/tools/design.ts
var designTools = [
  {
    name: "generate-image",
    description: "Generate one or more images on syntx.ai using a design service (e.g. sora-images, flux). Requires a target chat UUID; the result includes generation metadata returned by the API.",
    inputSchema: {
      type: "object",
      properties: {
        ai_name: {
          type: "string",
          description: 'Design service name, e.g. "sora-images".',
          default: "sora-images"
        },
        chat_uuid: { type: "string", description: "Target chat UUID (create one with create-chat)." },
        prompt: { type: "string", description: "Text prompt describing the image(s)." },
        n: { type: "number", minimum: 1, description: "Number of images to generate.", default: 1 },
        model_type: { type: "string", description: 'Model identifier, e.g. "gpt-image-2".' },
        resolution: { type: "string", description: 'Image resolution, e.g. "720x1280".' },
        quality: { type: "string", description: 'Quality level, e.g. "medium" or "high".' },
        image_size: {
          type: "string",
          description: 'Image size tier, e.g. "1K", "2K", "4K". Required by banana / banana3 / seedream (catalog `get_cost_params: ["image_size"]`). Exposed as a flat top-level field in addition to `model_settings` because some MCP clients cannot pass nested object arguments reliably.'
        },
        aspect_ratio: {
          type: "string",
          description: 'Aspect ratio, e.g. "3:4", "16:9", "1:1". Flat top-level alias for the same-named `settings` key the SPA sends.'
        },
        details_quality: {
          type: "string",
          description: 'Details level for sora-images gpt-image-2 (its `quality` is the size tier "1K"|"2K"|"4K"; "high" confirmed) and grok_imagine_2 ("low"|"medium"). Flat top-level alias for the same-named `settings` key.'
        },
        batch_size: {
          type: "number",
          description: "Batch size (grok_image, wan_image, seedream-5.0-pro, higgsfield-soul). Flat top-level alias for the same-named `settings` key."
        },
        ref_count: {
          type: "number",
          description: "Reference image count (sora-images gpt-image-2.5-*, grok_imagine_2). Flat top-level alias for the same-named `settings` key."
        },
        size: {
          type: "string",
          description: 'Size tier for wan_image models ("1K"). Flat top-level alias for the same-named `settings` key.'
        },
        version: {
          type: "string",
          description: "Model version (higgsfield-soul). Flat top-level alias for the same-named `settings` key."
        },
        rendering_speed: {
          type: "string",
          description: 'Rendering speed (ideogram), e.g. "TURBO". Flat top-level alias for the same-named `settings` key.'
        },
        image_url: {
          type: "array",
          items: { type: "string" },
          description: "Optional reference image URLs."
        },
        model_settings: {
          type: "object",
          additionalProperties: true,
          description: "Provider-specific settings merged into `body.settings` after the top-level fields above. Use for keys the top-level surface does not expose (e.g. ideogram wants `mode`, `style_type`, `rendering_speed`; seedream wants `stream`, `aspect_ratio` coercion; midjourney wants `version`, `style`, `seed`). Merged AFTER the top-level fields, so values here override them. Only plain JSON values are allowed; arrays and nested objects are passed through verbatim."
        }
      },
      required: ["chat_uuid", "prompt"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const aiName = args.ai_name ?? "sora-images";
        const modelSettings = args.model_settings;
        if (modelSettings !== void 0 && modelSettings !== null) {
          if (typeof modelSettings !== "object" || Array.isArray(modelSettings)) {
            throw new Error("model_settings must be a JSON object");
          }
        }
        const bodyParams = {
          chat_uuid: String(args.chat_uuid),
          prompt: String(args.prompt),
          settings: {
            n: args.n,
            model_type: args.model_type,
            resolution: args.resolution,
            quality: args.quality,
            image_size: args.image_size,
            aspect_ratio: args.aspect_ratio,
            details_quality: args.details_quality,
            batch_size: args.batch_size,
            ref_count: args.ref_count,
            size: args.size,
            version: args.version,
            rendering_speed: args.rendering_speed,
            image_url: args.image_url
          }
        };
        if (modelSettings !== void 0 && modelSettings !== null) {
          bodyParams.model_settings = modelSettings;
        }
        const result = await ctx.syntx.design.generate(aiName, bodyParams);
        return textResult(JSON.stringify(result, null, 2));
      } catch (err) {
        return toMcpError(err, "generate-image");
      }
    }
  }
];

// src/mcp/tools/file-input.ts
import * as fs from "fs";
import * as fsp from "fs/promises";
import * as path from "path";
import { Buffer } from "buffer";
var MAX_FILE_SIZE = 100 * 1024 * 1024;
var MAX_FILES_PER_CALL = 10;
var EXT_TO_MIME = {
  ".txt": "text/plain",
  ".md": "text/markdown",
  ".json": "application/json",
  ".csv": "text/csv",
  ".html": "text/html",
  ".pdf": "application/pdf",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".mp3": "audio/mpeg",
  ".mpeg": "audio/mpeg",
  ".wav": "audio/wav",
  ".mp4": "video/mp4",
  ".webm": "video/webm"
};
function guessMimeFromExt(filename) {
  return EXT_TO_MIME[path.extname(filename).toLowerCase()];
}
function decodeBase64(input) {
  const comma = input.indexOf(",");
  const body = input.startsWith("data:") && comma !== -1 ? input.slice(comma + 1) : input;
  const cleaned = body.replace(/\s+/g, "");
  return Buffer.from(cleaned, "base64");
}
function resolveAllowedRoots(env = process.env) {
  const raw = env.MCP_FILE_ROOTS;
  if (raw && raw.trim().length > 0) {
    const roots = raw.split(",").map((p) => p.trim()).filter((p) => p.length > 0).map((p) => path.isAbsolute(p) ? p : path.resolve(process.cwd(), p)).map((p) => {
      try {
        return fs.realpathSync(p);
      } catch {
        return p;
      }
    });
    if (roots.length > 0) return { roots, source: "env" };
  }
  let cwd;
  try {
    cwd = fs.realpathSync(process.cwd());
  } catch {
    cwd = path.resolve(process.cwd());
  }
  return { roots: [cwd], source: "default" };
}
function assertPathSourceAllowed(source, transport) {
  if (source === "path" && transport !== "stdio") {
    throw new Error(
      "`path` is not permitted over the HTTP transport (server-side file read). Send the payload inline via `content_base64` instead."
    );
  }
}
function resolveSafePath(inputPath, allowedRoots, maxBytes = MAX_FILE_SIZE) {
  if (!inputPath || typeof inputPath !== "string") {
    throw new Error("`path` must be a non-empty string.");
  }
  const abs = path.resolve(inputPath);
  let real;
  try {
    real = fs.realpathSync(abs);
  } catch {
    throw new Error(`File not found or not readable: ${inputPath}`);
  }
  const normalizedReal = process.platform === "win32" ? real.toLowerCase() : real;
  const ok = allowedRoots.some((root) => {
    const normalizedRoot = process.platform === "win32" ? root.toLowerCase() : root;
    const withSep = normalizedRoot.endsWith(path.sep) ? normalizedRoot : normalizedRoot + path.sep;
    return normalizedReal === normalizedRoot || normalizedReal.startsWith(withSep);
  });
  if (!ok) {
    throw new Error(
      `Path is outside of allowed roots: ${inputPath}. Set MCP_FILE_ROOTS to expand the allow-list (default: process.cwd()).`
    );
  }
  let stat2;
  try {
    stat2 = fs.statSync(real);
  } catch {
    throw new Error(`File not found or not readable: ${inputPath}`);
  }
  if (!stat2.isFile()) {
    const kind = stat2.isDirectory() ? "directory" : stat2.isSymbolicLink() ? "symlink" : stat2.isFIFO() ? "FIFO/pipe" : stat2.isSocket() ? "socket" : stat2.isBlockDevice() || stat2.isCharacterDevice() ? "device" : "special file";
    throw new Error(`Path is a ${kind}, not a regular file: ${inputPath}`);
  }
  if (stat2.size > maxBytes) {
    throw new Error(
      `File too large: ${stat2.size} bytes (limit ${maxBytes}). Use a smaller file.`
    );
  }
  return real;
}
async function resolveFileInput(item) {
  const hasPath = typeof item.path === "string" && item.path.length > 0;
  const hasBase64 = typeof item.content_base64 === "string" && item.content_base64.length > 0;
  if (hasPath && hasBase64) {
    throw new Error("Provide either `path` or `content_base64`, not both.");
  }
  if (!hasPath && !hasBase64) {
    throw new Error("Each file entry must include `path` or `content_base64`.");
  }
  if (hasPath) {
    const abs = path.resolve(item.path);
    const stat2 = await fsp.stat(abs).catch(() => {
      throw new Error(`File not found or not readable: ${item.path}`);
    });
    if (stat2.isDirectory()) {
      throw new Error(`Path is a directory, not a file: ${item.path}`);
    }
    if (stat2.size > MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${stat2.size} bytes (limit ${MAX_FILE_SIZE}). Use a smaller file.`
      );
    }
    const buffer2 = await fsp.readFile(abs);
    const filename = item.filename ?? path.basename(abs);
    const mimeType = item.mime_type ?? guessMimeFromExt(filename);
    return { buffer: buffer2, filename, mimeType, source: "path" };
  }
  if (!item.filename) {
    throw new Error("`filename` is required when uploading via `content_base64`.");
  }
  const buffer = decodeBase64(item.content_base64);
  if (buffer.byteLength > MAX_FILE_SIZE) {
    throw new Error(
      `Decoded payload too large: ${buffer.byteLength} bytes (limit ${MAX_FILE_SIZE}).`
    );
  }
  return {
    buffer,
    filename: item.filename,
    mimeType: item.mime_type ?? guessMimeFromExt(item.filename),
    source: "base64"
  };
}
async function resolveFileInputs(items) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error("`files` must be a non-empty array.");
  }
  if (items.length > MAX_FILES_PER_CALL) {
    throw new Error(
      `Too many files in one call: ${items.length} (limit ${MAX_FILES_PER_CALL}).`
    );
  }
  return Promise.all(items.map(resolveFileInput));
}

// src/mcp/tools/files.ts
var CODE_EXTENSIONS_TO_TXT = /* @__PURE__ */ new Set([
  ".js",
  ".css",
  ".json",
  ".py"
]);
function rewriteCodeExtension(filename) {
  if (typeof filename !== "string" || filename.length === 0) return filename;
  const dot = filename.lastIndexOf(".");
  if (dot <= 0 || dot === filename.length - 1) return filename;
  const ext = filename.slice(dot).toLowerCase();
  if (!CODE_EXTENSIONS_TO_TXT.has(ext)) return filename;
  return filename.slice(0, dot) + ".txt";
}
var filesTools = [
  {
    name: "list-uploaded-files",
    description: "List files previously uploaded to the syntx.ai account.",
    inputSchema: {
      type: "object",
      properties: {
        scope: {
          type: "string",
          description: "Filter by scope: all, text, image, audio, or video.",
          default: "all"
        },
        page: { type: "number", minimum: 1, default: 1 },
        page_size: { type: "number", minimum: 1, maximum: 100, default: 10 }
      },
      additionalProperties: false
    },
    handler: wrapSdk(
      "list-uploaded-files",
      async (args, ctx) => ctx.syntx.chats.getUploadedFiles(
        args.scope ?? "all",
        args.page ?? 1,
        args.page_size ?? 10
      )
    )
  },
  {
    name: "upload-files",
    capability: { localFileRead: true },
    description: `Upload one or more files to the syntx.ai account. Each file entry accepts either \`path\` (server-side file path, e.g. "C:\\photo.jpg") OR \`content_base64\` (inline base64, with optional \`data:<mime>;base64,\` prefix). For base64 entries, \`filename\` is required; \`mime_type\` is auto-guessed from extension if omitted. Max ${MAX_FILES_PER_CALL} files per call, ${Math.round(MAX_FILE_SIZE / (1024 * 1024))} MB each. Returns \`{ files: [{ url, filename, size, mime_type }] }\`.`,
    inputSchema: {
      type: "object",
      properties: {
        files: {
          type: "array",
          minItems: 1,
          maxItems: MAX_FILES_PER_CALL,
          description: "Files to upload.",
          items: {
            type: "object",
            properties: {
              path: {
                type: "string",
                description: "Filesystem path readable by the MCP server."
              },
              content_base64: {
                type: "string",
                description: "Inline base64 payload. May include a `data:<mime>;base64,` prefix. Mutually exclusive with `path`."
              },
              filename: {
                type: "string",
                description: "Override or supply the filename. Required for `content_base64` entries."
              },
              mime_type: {
                type: "string",
                description: "MIME type override. Auto-detected from the filename extension if omitted."
              }
            },
            additionalProperties: false,
            anyOf: [{ required: ["path"] }, { required: ["content_base64"] }]
          }
        },
        check_duplicates: {
          type: "boolean",
          default: true,
          description: "Ask the server to detect duplicates and skip them."
        },
        model_type: {
          type: "string",
          description: "Model identifier to scope the upload to (mirrors the SPA's `settings.model_type` field). Defaults to the server default model, or empty string when none is configured."
        }
      },
      required: ["files"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const items = args.files ?? [];
        const transport = ctx.config.transport;
        for (const item of items) {
          const hasPath = typeof item.path === "string" && item.path.length > 0;
          if (hasPath) {
            assertPathSourceAllowed("path", transport);
          }
        }
        const allowedRoots = resolveAllowedRoots().roots;
        for (const item of items) {
          if (typeof item.path === "string" && item.path.length > 0) {
            try {
              resolveSafePath(item.path, allowedRoots);
            } catch (err) {
              logSecurityEvent({
                kind: "upload-files.path.rejected",
                transport,
                reason: err instanceof Error ? err.message : String(err)
              });
              throw err;
            }
          }
        }
        const resolved = await resolveFileInputs(items);
        const checkDuplicates = args.check_duplicates === void 0 ? true : Boolean(args.check_duplicates);
        const modelTypeRaw = args.model_type;
        const modelType = typeof modelTypeRaw === "string" && modelTypeRaw.length > 0 ? modelTypeRaw : ctx.config.defaultModel ?? "";
        const result = await ctx.syntx.chats.uploadFiles(
          resolved.map((r) => ({
            buffer: r.buffer,
            filename: rewriteCodeExtension(r.filename),
            mimeType: r.mimeType
          })),
          "hidden",
          checkDuplicates,
          modelType
        );
        return textResult(JSON.stringify(result, null, 2));
      } catch (err) {
        return toMcpError(err, "upload-files");
      }
    }
  },
  {
    name: "delete-file",
    description: "Permanently delete an uploaded file. Accepts either `file_id` (the historical behaviour) or `url` (the uploaded R2 URL, mirroring the SPA's `file-storage.remove`). Exactly one of the two must be provided.",
    inputSchema: {
      type: "object",
      properties: {
        file_id: { type: "string", description: "Uploaded file id." },
        url: { type: "string", description: "Uploaded file URL." }
      },
      additionalProperties: false,
      anyOf: [{ required: ["file_id"] }, { required: ["url"] }]
    },
    async handler(args, ctx) {
      const fileId = typeof args.file_id === "string" ? args.file_id.trim() : "";
      const url = typeof args.url === "string" ? args.url.trim() : "";
      if (!fileId && !url) {
        return toMcpError(
          new Error('Exactly one of "file_id" or "url" must be provided'),
          "delete-file"
        );
      }
      try {
        const target = fileId ? fileId : { url };
        await ctx.syntx.chats.deleteFile(target);
        const label = fileId ? `file_id ${fileId}` : `url ${url}`;
        return textResult(`Deleted ${label}.`);
      } catch (err) {
        return toMcpError(err, "delete-file");
      }
    }
  }
];

// src/mcp/tools/audio.ts
var TRANSCRIBE_MAX_SIZE = 52428800;
var TRANSCRIBE_ACCEPTED_MIMES = ["audio/mpeg", "audio/wav", "audio/mp3"];
var audioTools = [
  {
    name: "transcribe",
    capability: { localFileRead: true },
    description: `Transcribe an audio file to text via syntx.ai (POST /api/v1/audio/transcribe). Provide a single file either as \`path\` (server filesystem; stdio transport only) or as \`content_base64\` with \`filename\`. IMPORTANT: when the server runs over the HTTP transport, \`path\` is rejected (arbitrary server-side file reads by remote clients) \u2014 use \`content_base64\` instead. Limit ${Math.round(TRANSCRIBE_MAX_SIZE / (1024 * 1024))} MB; accepted formats: mp3, wav, mpeg. Returns { text }.`,
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Path to an audio file on the MCP server filesystem. stdio transport only \u2014 rejected over HTTP. Use content_base64 over HTTP."
        },
        content_base64: {
          type: "string",
          description: "Inline base64 audio payload (optionally with a `data:<mime>;base64,` prefix). Mutually exclusive with `path`. Preferred for the HTTP transport."
        },
        filename: {
          type: "string",
          description: "Filename. Required when using `content_base64` (used for MIME inference and the upload name)."
        },
        mime_type: {
          type: "string",
          description: "Optional MIME type override (auto-detected from extension if omitted)."
        }
      },
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const spec = {
          path: typeof args.path === "string" ? args.path : void 0,
          content_base64: typeof args.content_base64 === "string" ? args.content_base64 : void 0,
          filename: typeof args.filename === "string" ? args.filename : void 0,
          mime_type: typeof args.mime_type === "string" ? args.mime_type : void 0
        };
        if (spec.path) {
          assertPathSourceAllowed("path", ctx.config.transport);
          try {
            resolveSafePath(spec.path, resolveAllowedRoots().roots, TRANSCRIBE_MAX_SIZE);
          } catch (err) {
            logSecurityEvent({
              kind: "upload-files.path.rejected",
              transport: ctx.config.transport,
              reason: err instanceof Error ? err.message : String(err)
            });
            throw err;
          }
        }
        const resolved = await resolveFileInput(spec);
        const resolvedMime = (resolved.mimeType ?? "").toLowerCase().split(";")[0].trim();
        if (!TRANSCRIBE_ACCEPTED_MIMES.includes(resolvedMime)) {
          logSecurityEvent({
            kind: "transcribe.mime.rejected",
            transport: ctx.config.transport,
            reason: resolvedMime || "unknown",
            meta: { mime: resolvedMime || "unknown" }
          });
          return toMcpError(
            new Error(
              `Unsupported audio type: ${resolved.mimeType ?? "unknown"} (accepted: ${TRANSCRIBE_ACCEPTED_MIMES.join(", ")}).`
            ),
            "transcribe"
          );
        }
        if (resolved.buffer.byteLength > TRANSCRIBE_MAX_SIZE) {
          return toMcpError(
            new Error(
              `Audio too large: ${resolved.buffer.byteLength} bytes (transcribe limit ${TRANSCRIBE_MAX_SIZE} bytes \u2248 ${Math.round(
                TRANSCRIBE_MAX_SIZE / (1024 * 1024)
              )} MB).`
            ),
            "transcribe"
          );
        }
        const view = new Uint8Array(resolved.buffer.byteLength);
        view.set(resolved.buffer);
        const fileParts = [view];
        const file = new File(fileParts, resolved.filename, {
          type: resolved.mimeType || void 0
        });
        const result = await ctx.syntx.chats.transcribe(file);
        return textResult(JSON.stringify(result, null, 2));
      } catch (err) {
        return toMcpError(err, "transcribe");
      }
    }
  },
  {
    name: "generate-audio",
    description: "Generate audio (TTS, voice change, music) via syntx.ai. Mirrors `syntx.audio.generate` and the SPA `ai-audio.sendMessage` flow. Posts to `POST /api/v1/audio/generate?ai_name={ai_name}`. Requires a target chat UUID (use `create-chat` first). The result includes generation metadata returned by the API; follow up with `wait-for-response` or `get-messages` to read the completed audio URL once the model finishes.",
    inputSchema: {
      type: "object",
      properties: {
        ai_name: {
          type: "string",
          description: 'Audio provider name (e.g. "elevenlabs", "suno-music"). Use `list-models` with scope=audio to discover valid values.',
          default: "elevenlabs"
        },
        chat_uuid: { type: "string", description: "Target chat UUID (create one with create-chat)." },
        prompt: { type: "string", description: "Text prompt describing the audio to produce." },
        voice_id: { type: "string", description: "Voice identifier for TTS models (e.g. ElevenLabs voice_id)." },
        model_type: { type: "string", description: "Model identifier within the provider." },
        duration: { type: "number", minimum: 0, description: "Target duration in seconds (music/clip models)." },
        chars_count: {
          type: "number",
          description: "Character count for TTS cost (elevenlabs; required in get_model_info for text_to_speech/text_to_dialogue modes). Flat top-level alias for the same-named `settings` key."
        },
        mode: {
          type: "string",
          description: 'Generation mode. ElevenLabs: "text_to_speech" | "text_to_dialogue" | "speech_to_speech". Flat top-level alias for the same-named `settings` key.'
        },
        version: {
          type: "string",
          description: 'Model version (suno: "V6"). Flat top-level alias for the same-named `settings` key.'
        },
        sample_rate: {
          type: "number",
          description: "Sample rate override in Hz (e.g. 22050, 44100)."
        },
        style_prompt: {
          type: "string",
          description: 'Provider-specific style/mood hint (e.g. "pop, sad, rainy night").'
        },
        file_urls: {
          type: "array",
          items: { type: "string" },
          description: "Optional input file URLs (e.g. source audio for voice-change). Mirrors the SPA `attachments` argument translated to `file_urls`."
        },
        model_settings: {
          type: "object",
          additionalProperties: true,
          description: "Provider-specific settings merged into `body.settings` after the top-level fields above. Use for keys the top-level surface does not expose (e.g. suno wants `mode`, `is_instrumental`, `styles`, `title`, `negative_tags`, `source_clip_id`, `source_task_id`, `continue_at`). Merged AFTER the top-level fields, so values here override them. Only plain JSON values are allowed; arrays and nested objects are passed through verbatim."
        }
      },
      required: ["chat_uuid", "prompt"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const aiName = args.ai_name ?? "elevenlabs";
        const settings = {};
        if (args.voice_id !== void 0) settings.voice_id = String(args.voice_id);
        if (args.model_type !== void 0) settings.model_type = String(args.model_type);
        if (args.duration !== void 0) settings.duration = Number(args.duration);
        if (args.sample_rate !== void 0) settings.sample_rate = Number(args.sample_rate);
        if (args.style_prompt !== void 0) settings.prompt = String(args.style_prompt);
        if (args.chars_count !== void 0) settings.chars_count = Number(args.chars_count);
        if (args.mode !== void 0) settings.mode = String(args.mode);
        if (args.version !== void 0) settings.version = String(args.version);
        const modelSettings = args.model_settings;
        if (modelSettings !== void 0 && modelSettings !== null) {
          if (typeof modelSettings !== "object" || Array.isArray(modelSettings)) {
            throw new Error("model_settings must be a JSON object");
          }
        }
        const bodyParams = {
          chat_uuid: String(args.chat_uuid),
          prompt: String(args.prompt),
          settings,
          file_urls: args.file_urls ?? void 0
        };
        if (modelSettings !== void 0 && modelSettings !== null) {
          bodyParams.model_settings = modelSettings;
        }
        const result = await ctx.syntx.audio.generate(aiName, bodyParams);
        return textResult(JSON.stringify(result, null, 2));
      } catch (err) {
        return toMcpError(err, "generate-audio");
      }
    }
  }
];

// src/mcp/tools/folders.ts
var foldersTools = [
  {
    name: "list-projects",
    description: "List projects (a.k.a. folders) for a given scope. Mirrors `syntx.folders.listTextFolders` / `listImageFolders` / `listVideoFolders` / `listAudioFolders`. Hits `GET /api/v1/folders/{scope}/list`. Returns an array of `Folder` items.",
    inputSchema: {
      type: "object",
      properties: {
        scope: {
          type: "string",
          enum: ["text", "image", "video", "audio"],
          default: "text",
          description: 'Project scope. Defaults to "text" (matches the web client).'
        }
      },
      additionalProperties: false
    },
    handler: wrapSdk(
      "list-projects",
      async (args, ctx) => {
        switch (args.scope ?? "text") {
          case "image":
            return ctx.syntx.folders.listImageFolders();
          case "video":
            return ctx.syntx.folders.listVideoFolders();
          case "audio":
            return ctx.syntx.folders.listAudioFolders();
          case "text":
          default:
            return ctx.syntx.folders.listTextFolders();
        }
      }
    )
  },
  {
    name: "create-project",
    description: "Create a syntx.ai project (a.k.a. folder) and optionally seed it with existing chats. Returns the created project as JSON (uuid, title, scope, color, chats). Mirrors `syntx.folders.create`.",
    inputSchema: {
      type: "object",
      properties: {
        title: { type: "string", description: "Project title (required)." },
        scope: {
          type: "string",
          enum: ["text", "image", "video", "audio"],
          default: "text",
          description: 'Project scope. Defaults to "text" (matches the web client).'
        },
        color: {
          type: "string",
          default: "#9C9C9C",
          description: 'CSS hex color for the project chip. Defaults to "#9C9C9C".'
        },
        chat_uuids: {
          type: "array",
          items: { type: "string" },
          uniqueItems: true,
          description: "Optional list of existing chat UUIDs to add on creation."
        }
      },
      required: ["title"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const title = String(args.title ?? "").trim();
      if (!title) {
        return toMcpError(new Error('"title" must be a non-empty string'), "create-project");
      }
      const rawChatUuids = args.chat_uuids;
      let chatUuids;
      if (rawChatUuids !== void 0) {
        if (!Array.isArray(rawChatUuids) || !rawChatUuids.every((c) => typeof c === "string")) {
          return toMcpError(
            new Error('"chat_uuids" must be an array of strings when provided'),
            "create-project"
          );
        }
        chatUuids = rawChatUuids.map((c) => c.trim()).filter((c) => c.length > 0);
      }
      try {
        const folder = await ctx.syntx.folders.create({
          title,
          scope: args.scope,
          color: args.color,
          chat_uuids: chatUuids
        });
        return textResult(JSON.stringify(folder, null, 2));
      } catch (err) {
        return toMcpError(err, "create-project");
      }
    }
  },
  {
    name: "add-chats-to-project",
    description: "Add one or more existing chats to an existing project. Mirrors `syntx.folders.addChats`. Sends a bare JSON array of chat UUIDs to `POST /api/v1/folders/{folder_uuid}/add`.",
    inputSchema: {
      type: "object",
      properties: {
        folder_uuid: { type: "string", description: "Project UUID (required)." },
        chat_uuids: {
          type: "array",
          items: { type: "string" },
          minItems: 1,
          uniqueItems: true,
          description: "Chat UUIDs to add. Must contain at least one entry."
        }
      },
      required: ["folder_uuid", "chat_uuids"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const folderUuid = String(args.folder_uuid ?? "").trim();
      if (!folderUuid) {
        return toMcpError(new Error('"folder_uuid" must be a non-empty string'), "add-chats-to-project");
      }
      const rawChatUuids = args.chat_uuids;
      if (!Array.isArray(rawChatUuids) || rawChatUuids.length === 0) {
        return toolError(
          'add-chats-to-project: "chat_uuids" must be a non-empty array of chat UUIDs.'
        );
      }
      if (!rawChatUuids.every((c) => typeof c === "string")) {
        return toMcpError(
          new Error('"chat_uuids" must be an array of strings'),
          "add-chats-to-project"
        );
      }
      const chatUuids = rawChatUuids.map((c) => c.trim()).filter((c) => c.length > 0);
      if (chatUuids.length === 0) {
        return toolError('add-chats-to-project: "chat_uuids" must contain at least one non-empty UUID.');
      }
      try {
        const response = await ctx.syntx.folders.addChats(folderUuid, chatUuids);
        if (response === void 0 || response === null) {
          return textResult(
            `Added ${chatUuids.length} chat(s) to project ${folderUuid}.`
          );
        }
        return textResult(JSON.stringify(response, null, 2));
      } catch (err) {
        return toMcpError(err, "add-chats-to-project");
      }
    }
  },
  {
    name: "delete-project",
    description: "Permanently delete a syntx.ai project (a.k.a. folder). Mirrors `syntx.folders.delete`. Issues `DELETE /api/v1/folders/{folder_uuid}/delete`. This action is destructive and cannot be undone.",
    inputSchema: {
      type: "object",
      properties: {
        folder_uuid: { type: "string", description: "Project UUID to delete (required)." }
      },
      required: ["folder_uuid"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const folderUuid = String(args.folder_uuid ?? "").trim();
      if (!folderUuid) {
        return toMcpError(new Error('"folder_uuid" must be a non-empty string'), "delete-project");
      }
      try {
        const response = await ctx.syntx.folders.delete(folderUuid);
        if (response === void 0 || response === null) {
          return textResult(`Deleted project ${folderUuid}.`);
        }
        return textResult(JSON.stringify(response, null, 2));
      } catch (err) {
        return toMcpError(err, "delete-project");
      }
    }
  },
  {
    name: "remove-chats-from-project",
    description: "Remove one or more chats from a project. Mirrors `syntx.folders.removeChats`. Inverse of `add-chats-to-project`: sends a bare JSON array of chat UUIDs to `POST /api/v1/folders/{folder_uuid}/remove`.",
    inputSchema: {
      type: "object",
      properties: {
        folder_uuid: { type: "string", description: "Project UUID (required)." },
        chat_uuids: {
          type: "array",
          items: { type: "string" },
          minItems: 1,
          uniqueItems: true,
          description: "Chat UUIDs to remove. Must contain at least one entry."
        }
      },
      required: ["folder_uuid", "chat_uuids"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const folderUuid = String(args.folder_uuid ?? "").trim();
      if (!folderUuid) {
        return toMcpError(new Error('"folder_uuid" must be a non-empty string'), "remove-chats-from-project");
      }
      const rawChatUuids = args.chat_uuids;
      if (!Array.isArray(rawChatUuids) || rawChatUuids.length === 0) {
        return toolError(
          'remove-chats-from-project: "chat_uuids" must be a non-empty array of chat UUIDs.'
        );
      }
      if (!rawChatUuids.every((c) => typeof c === "string")) {
        return toMcpError(
          new Error('"chat_uuids" must be an array of strings'),
          "remove-chats-from-project"
        );
      }
      const chatUuids = rawChatUuids.map((c) => c.trim()).filter((c) => c.length > 0);
      if (chatUuids.length === 0) {
        return toolError('remove-chats-from-project: "chat_uuids" must contain at least one non-empty UUID.');
      }
      try {
        const response = await ctx.syntx.folders.removeChats(folderUuid, chatUuids);
        return jsonOrAck(
          response,
          `Removed ${chatUuids.length} chat(s) from project ${folderUuid}.`
        );
      } catch (err) {
        return toMcpError(err, "remove-chats-from-project");
      }
    }
  },
  {
    name: "update-project",
    description: "Update a project's title and/or color. Mirrors `syntx.folders.update`. Issues `PATCH /api/v1/folders/{folder_uuid}/change`; only the provided fields are sent. At least one of `title` / `color` is required.",
    inputSchema: {
      type: "object",
      properties: {
        folder_uuid: { type: "string", description: "Project UUID (required)." },
        title: { type: "string", description: "New project title." },
        color: { type: "string", description: 'New project color (e.g. a CSS hex value like "#9C9C9C").' }
      },
      required: ["folder_uuid"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const folderUuid = String(args.folder_uuid ?? "").trim();
      if (!folderUuid) {
        return toMcpError(new Error('"folder_uuid" must be a non-empty string'), "update-project");
      }
      const data = {};
      if (args.title !== void 0) {
        const title = String(args.title).trim();
        if (!title) {
          return toMcpError(new Error('"title" must be a non-empty string when provided'), "update-project");
        }
        data.title = title;
      }
      if (args.color !== void 0) {
        const color = String(args.color).trim();
        if (!color) {
          return toMcpError(new Error('"color" must be a non-empty string when provided'), "update-project");
        }
        data.color = color;
      }
      if (data.title === void 0 && data.color === void 0) {
        return toolError('update-project: provide at least one of "title" or "color".');
      }
      try {
        const response = await ctx.syntx.folders.update(folderUuid, data);
        return jsonOrAck(response, `Updated project ${folderUuid}.`);
      } catch (err) {
        return toMcpError(err, "update-project");
      }
    }
  },
  {
    name: "reorder-project",
    description: "Reorder a project within its scope. Mirrors `syntx.folders.move`. Issues `PATCH /api/v1/folders/{folder_uuid}/move` with `{after_uuid}`. Pass the UUID of the project to place it after; omit `after_uuid` (or pass null) to move to the top.",
    inputSchema: {
      type: "object",
      properties: {
        folder_uuid: { type: "string", description: "Project UUID (required)." },
        after_uuid: {
          type: ["string", "null"],
          minLength: 1,
          description: "UUID of the project to place this one after. Omit or pass null to move to the top."
        }
      },
      required: ["folder_uuid"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      const folderUuid = String(args.folder_uuid ?? "").trim();
      if (!folderUuid) {
        return toMcpError(new Error('"folder_uuid" must be a non-empty string'), "reorder-project");
      }
      let afterUuid = null;
      if (args.after_uuid !== void 0 && args.after_uuid !== null) {
        afterUuid = String(args.after_uuid).trim();
        if (!afterUuid) {
          return toMcpError(new Error('"after_uuid" must be a non-empty string or null'), "reorder-project");
        }
      }
      try {
        const response = await ctx.syntx.folders.move(folderUuid, afterUuid);
        return jsonOrAck(
          response,
          afterUuid === null ? `Moved project ${folderUuid} to the top.` : `Moved project ${folderUuid} after ${afterUuid}.`
        );
      } catch (err) {
        return toMcpError(err, "reorder-project");
      }
    }
  }
];

// src/mcp/tools/video.ts
var videoTools = [
  {
    name: "generate-video",
    description: "Generate a video via syntx.ai. Mirrors `syntx.video.generate` and the SPA `ai-video.sendMessage` flow. Posts to `POST /api/v1/video/generate?ai_name={ai_name}`. Requires a target chat UUID (use `create-chat` first). Generation is long-running \u2014 poll the resulting chat with `wait-for-response` or `get-messages` to read the completed video URL once the model finishes.",
    inputSchema: {
      type: "object",
      properties: {
        ai_name: {
          type: "string",
          description: 'Video provider name (e.g. "wan_video", "runway", "kling"). Use `list-models` with scope=video to discover valid values.',
          default: "wan_video"
        },
        chat_id: { type: "string", description: "Target chat UUID (create one with create-chat)." },
        prompt: { type: "string", description: "Text prompt describing the video to produce." },
        model_type: { type: "string", description: "Model identifier within the provider." },
        duration: { type: "number", minimum: 0, description: "Target duration in seconds." },
        resolution: {
          type: "string",
          description: 'Output resolution, e.g. "1280x720" or "720x1280".'
        },
        aspect_ratio: {
          type: "string",
          description: 'Aspect ratio, e.g. "16:9", "9:16", "1:1".'
        },
        fps: { type: "number", description: "Frame rate override." },
        frame_rate: {
          type: "number",
          description: "Frame rate (topaz_astra, beeble switchx). Distinct from `fps`: some providers spell the setting `frame_rate`."
        },
        video_duration: {
          type: ["number", "string"],
          description: 'Duration in seconds for providers that spell it `video_duration` (kling: "5"|"10"|"15"; grok_video: "6"|"10"; sora: "4"\u2026"25"; hailuo: "6"|"10"; veo_omni: "4"|"6"|"8"|"10"). String or number both accepted.'
        },
        mode: {
          type: "string",
          description: 'Generation mode (kling: "standart"|"hd"|"4K"; seedance: "std"; veo_omni: "frames"|"omni-references"|"edit"|"extend").'
        },
        size: {
          type: "string",
          description: 'Size/aspect (seedance: "21:9"|"16:9"|"9:16"|"1:1"|"4:3"|"3:4"|"adaptive").'
        },
        version: {
          type: "string",
          description: 'Model version (kling: "1.5"\u2026"3.0"; kling_motion_control also "standart"|"hd").'
        },
        native_audio: {
          type: "boolean",
          description: "Native audio flag (kling, wan_26 i2v/r2v flash)."
        },
        generate_audio: {
          type: "boolean",
          description: "Generate audio track (seedance-1.5-pro)."
        },
        draft: {
          type: "boolean",
          description: "Draft mode (flux3_video)."
        },
        upscale: {
          type: "number",
          description: "Upscale flag as INTEGER 0|1 (veo3 family; the API rejects non-integers)."
        },
        gen_type: {
          type: "string",
          description: 'Generation type for kling_motion_control ("mcv" | "mci").'
        },
        ref_count: {
          type: "number",
          description: "Reference image count (hailuo-3.0)."
        },
        quality: { type: "string", description: 'Quality preset (e.g. sora: "480"|"720"|"1080").' },
        seed: { type: "number", description: "Seed for deterministic sampling, when supported." },
        file_urls: {
          type: "array",
          items: { type: "string" },
          description: "Optional input file URLs (e.g. source image for image-to-video). `wan_video` reads `settings.file_urls` for the same purpose."
        },
        audio_url: {
          type: "string",
          description: "Optional audio track URL to mix into the generated video. Distinct from `file_urls` (SPA `audio_url` field)."
        },
        model_settings: {
          type: "object",
          additionalProperties: true,
          description: "Provider-specific settings merged into `body.settings` after the top-level fields above. Use for keys the top-level surface does not expose (e.g. grok_video wants `video_duration` not `duration`, and accepts resolution enum `480p`|`720p`; kling wants `version`, `mode`, `native_audio`). Merged AFTER the top-level fields, so values here override them. Only plain JSON values are allowed; arrays and nested objects are passed through verbatim."
        }
      },
      required: ["chat_id", "prompt"],
      additionalProperties: false
    },
    async handler(args, ctx) {
      try {
        const aiName = args.ai_name ?? "wan_video";
        const settings = {};
        if (args.model_type !== void 0) settings.model_type = String(args.model_type);
        if (args.duration !== void 0) settings.duration = Number(args.duration);
        if (args.resolution !== void 0) settings.resolution = String(args.resolution);
        if (args.aspect_ratio !== void 0) settings.aspect_ratio = String(args.aspect_ratio);
        if (args.fps !== void 0) settings.fps = Number(args.fps);
        if (args.frame_rate !== void 0) settings.frame_rate = Number(args.frame_rate);
        if (args.video_duration !== void 0) settings.video_duration = args.video_duration;
        if (args.mode !== void 0) settings.mode = String(args.mode);
        if (args.size !== void 0) settings.size = String(args.size);
        if (args.version !== void 0) settings.version = String(args.version);
        if (args.native_audio !== void 0) settings.native_audio = Boolean(args.native_audio);
        if (args.generate_audio !== void 0) settings.generate_audio = Boolean(args.generate_audio);
        if (args.draft !== void 0) settings.draft = Boolean(args.draft);
        if (args.upscale !== void 0) settings.upscale = Number(args.upscale);
        if (args.gen_type !== void 0) settings.gen_type = String(args.gen_type);
        if (args.ref_count !== void 0) settings.ref_count = Number(args.ref_count);
        if (args.quality !== void 0) settings.quality = String(args.quality);
        if (args.seed !== void 0) settings.seed = Number(args.seed);
        const modelSettings = args.model_settings;
        if (modelSettings !== void 0 && modelSettings !== null) {
          if (typeof modelSettings !== "object" || Array.isArray(modelSettings)) {
            throw new Error("model_settings must be a JSON object");
          }
          for (const [k, v] of Object.entries(modelSettings)) {
            settings[k] = v;
          }
        }
        const body = {
          chat_id: String(args.chat_id),
          prompt: String(args.prompt),
          settings
        };
        const fileUrls = args.file_urls;
        if (fileUrls !== void 0) body.file_urls = fileUrls;
        const audioUrl = typeof args.audio_url === "string" && args.audio_url.length > 0 ? args.audio_url : void 0;
        if (audioUrl !== void 0) body.audio_url = audioUrl;
        const result = await ctx.syntx.video.generate(aiName, body);
        return textResult(JSON.stringify(result, null, 2));
      } catch (err) {
        return toMcpError(err, "generate-video");
      }
    }
  }
];

// src/mcp/tools/llm.ts
var llmTools = [
  {
    name: "get-llm-limits",
    description: "Return the current LLM usage limits (6h and 7d windows). Each window reports percent_left (0..100), started_at, expires_at. A window with expired or null expires_at is normalized to {percent_left: 100, started_at: null, expires_at: null}.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    handler: wrapSdk(
      "get-llm-limits",
      async (_args, ctx) => ctx.syntx.llm.getLimits()
    )
  }
];

// src/mcp/tools/index.ts
var allTools = [
  ...authTools,
  ...userTools,
  ...aiTools,
  ...chatsTools,
  ...designTools,
  ...filesTools,
  ...audioTools,
  ...videoTools,
  ...foldersTools,
  ...llmTools
];

// src/mcp/server.ts
var SERVER_NAME = "syntx-ai-mcp";
var SERVER_VERSION = "0.3.0";
function createMcpServer(config, requestToken) {
  const context = createMcpContext(config, requestToken);
  const server = new Server(
    { name: SERVER_NAME, version: SERVER_VERSION },
    {
      capabilities: {
        tools: {}
      }
    }
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: allTools.map((t) => ({
      name: t.name,
      description: t.description,
      inputSchema: t.inputSchema
    }))
  }));
  server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
    const { name, arguments: args } = request.params;
    const tool = allTools.find((t) => t.name === name);
    if (!tool) {
      return toMcpError(new Error(`Unknown tool: ${name}`), "call-tool");
    }
    if (context.config.transport !== "stdio" && tool.capability?.localFileRead && typeof args?.path === "string") {
      logSecurityEvent({
        kind: "upload-files.path.rejected",
        transport: context.config.transport,
        reason: "capability-localFileRead",
        meta: { tool: name }
      });
      return toMcpError(
        new Error(
          `${name}: \`path\` is not permitted over the ${context.config.transport} transport. Send the payload inline (e.g. \`content_base64\`) instead.`
        ),
        `tool:${name}`
      );
    }
    try {
      const reqCtx = withRequestContext(context, extra);
      return await tool.handler(
        args ?? {},
        reqCtx,
        extra
      );
    } catch (err) {
      return toMcpError(err, `tool:${name}`);
    }
  });
  return { server, context };
}

export {
  SyntxAPIError,
  SyntxAuthError,
  SyntxTimeoutError,
  SyntxAbortError,
  BaseClient,
  SyntxAuth,
  AIResource,
  UserResource,
  toPublicUser,
  collectCompletedObjects,
  ChatsResource,
  PlansResource,
  NotificationsResource,
  FoldersResource,
  SettingsResource,
  DesignResource,
  AudioResource,
  VideoResource,
  AppResource,
  LlmResource,
  SyntxClient,
  createMcpContext,
  withRequestContext,
  logSecurityEvent,
  allTools,
  createMcpServer
};
