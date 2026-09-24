import {
  DEFAULT_CONFIG,
  ENV_KEYS,
  loadConfig,
  runTransport,
  startHttp,
  startStdio
} from "./chunk-77K6Q6R3.mjs";
import {
  AIResource,
  AppResource,
  AudioResource,
  BaseClient,
  ChatsResource,
  DesignResource,
  FoldersResource,
  LlmResource,
  NotificationsResource,
  PlansResource,
  SettingsResource,
  SyntxAPIError,
  SyntxAbortError,
  SyntxAuth,
  SyntxAuthError,
  SyntxClient,
  SyntxTimeoutError,
  UserResource,
  VideoResource,
  allTools,
  collectCompletedObjects,
  createMcpContext,
  createMcpServer,
  toPublicUser,
  withRequestContext
} from "./chunk-B2QIVNSM.mjs";

// src/websocket.ts
var SyntxWebSocket = class {
  ws = null;
  baseURL;
  token;
  lang;
  endpoint = null;
  handlers = [];
  connectHandlers = [];
  disconnectHandlers = [];
  errorHandlers = [];
  pingTimer = null;
  pingIntervalMs;
  autoReconnect;
  closedByUser = false;
  constructor(tokenOrOptions = {}, lang = "en") {
    if (typeof tokenOrOptions === "string") {
      this.token = tokenOrOptions;
      this.lang = lang;
      this.baseURL = "wss://api.syntx.ai/api/v1";
      this.pingIntervalMs = 3e4;
      this.autoReconnect = false;
    } else {
      this.token = tokenOrOptions.token ?? "";
      this.lang = tokenOrOptions.lang ?? "en";
      this.baseURL = (tokenOrOptions.baseURL ?? "wss://api.syntx.ai/api/v1").replace(/\/$/, "");
      this.pingIntervalMs = tokenOrOptions.pingIntervalMs ?? 3e4;
      this.autoReconnect = tokenOrOptions.autoReconnect ?? false;
    }
  }
  /**
   * Build the WSS URL for an endpoint. The bearer token is intentionally
   * NOT placed in the query string (H3) — only the public `lang` parameter
   * remains. The token travels in the `Authorization: Bearer …` header set
   * by `connect()`.
   */
  buildUrl(endpoint) {
    const url = new URL(`${this.baseURL}/${endpoint.replace(/^\//, "")}`);
    if (this.lang) url.searchParams.set("lang", this.lang);
    return url.toString();
  }
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
  connect(endpoint) {
    if (this.ws && this.ws.readyState !== WebSocket.CLOSED) {
      throw new Error("WebSocket already connected; call close() first");
    }
    this.endpoint = endpoint;
    this.closedByUser = false;
    const url = this.buildUrl(endpoint);
    const headers = {};
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    const ws = new WebSocket(url, { headers });
    this.ws = ws;
    ws.onopen = () => {
      this.connectHandlers.forEach((h) => h());
      this.startPing();
    };
    ws.onmessage = (event) => {
      let parsed;
      try {
        parsed = JSON.parse(event.data);
      } catch {
        return;
      }
      this.dispatch(parsed);
    };
    ws.onerror = () => {
      const error = new Error("WebSocket error");
      this.errorHandlers.forEach((h) => h(error));
    };
    ws.onclose = () => {
      this.stopPing();
      this.disconnectHandlers.forEach((h) => h());
      if (this.autoReconnect && !this.closedByUser && this.endpoint) {
        try {
          this.connect(this.endpoint);
        } catch {
        }
      }
    };
  }
  /**
   * Send a JSON frame over the open socket. No-op if the socket is not yet open.
   */
  send(data) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
      return true;
    }
    return false;
  }
  /**
   * Create a new chat session via WSS and resolve with the chat UUID.
   *
   * Sends `{ action: 'create', scope, model? }` and waits for the
   * `session` message carrying `uuid`.
   */
  createSession(scope = "text", model, timeoutMs = 15e3) {
    return new Promise((resolve, reject) => {
      if (!this.endpoint) {
        this.connect("chats/stream");
      }
      const t = setTimeout(() => {
        reject(new Error("Timeout waiting for session response"));
      }, timeoutMs);
      this.once("session", (msg) => {
        clearTimeout(t);
        if (msg.uuid) resolve(msg.uuid);
        else reject(new Error("No uuid in session response"));
      });
      this.once("error", (msg) => {
        clearTimeout(t);
        reject(new Error(msg.error || "Session creation failed"));
      });
      this.send({
        action: "create",
        scope,
        ...model ? { model } : {}
      });
    });
  }
  /**
   * Send a prompt to an existing chat session. The server will respond with
   * a stream of `message` frames and a final `done` frame.
   */
  sendPrompt(chatUuid, prompt, settings) {
    return this.send({
      action: "prompt",
      chat_uuid: chatUuid,
      prompt,
      settings: settings ?? {}
    });
  }
  /** Close the connection and release all handlers. Idempotent. */
  close() {
    this.closedByUser = true;
    this.stopPing();
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
      }
      this.ws = null;
    }
    this.handlers = [];
  }
  /** Register a handler for every incoming message. */
  onMessage(handler) {
    this.handlers.push({ type: null, once: false, fn: handler });
  }
  /** Register a one-shot handler for messages of a given `type`. */
  once(type, handler) {
    this.handlers.push({ type, once: true, fn: handler });
  }
  /** Listen for connection-open events. */
  onConnect(handler) {
    this.connectHandlers.push(handler);
  }
  /** Listen for connection-close events. */
  onDisconnect(handler) {
    this.disconnectHandlers.push(handler);
  }
  /** Listen for transport-level errors. */
  onError(handler) {
    this.errorHandlers.push(handler);
  }
  get readyState() {
    return this.ws?.readyState ?? WebSocket.CLOSED;
  }
  get isConnected() {
    return this.ws?.readyState === WebSocket.OPEN;
  }
  /** The endpoint this socket is (or was last) connected to. */
  get currentEndpoint() {
    return this.endpoint;
  }
  // ── Internal ──────────────────────────────────────────────────────────
  dispatch(msg) {
    const handlers = this.handlers;
    const keep = [];
    for (const h of handlers) {
      if (h.type === null || h.type === msg.type) {
        try {
          h.fn(msg);
        } catch {
        }
        if (!h.once) keep.push(h);
      } else {
        keep.push(h);
      }
    }
    this.handlers = keep;
  }
  startPing() {
    this.stopPing();
    if (this.pingIntervalMs <= 0) return;
    this.pingTimer = setInterval(() => {
      this.send({ action: "ping" });
    }, this.pingIntervalMs);
    if (typeof this.pingTimer === "object" && this.pingTimer && "unref" in this.pingTimer) {
      this.pingTimer.unref?.();
    }
  }
  stopPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
};
export {
  AIResource,
  AppResource,
  AudioResource,
  BaseClient,
  ChatsResource,
  DEFAULT_CONFIG,
  DesignResource,
  ENV_KEYS,
  FoldersResource,
  LlmResource,
  NotificationsResource,
  PlansResource,
  SettingsResource,
  SyntxAPIError,
  SyntxAbortError,
  SyntxAuth,
  SyntxAuthError,
  SyntxClient,
  SyntxTimeoutError,
  SyntxWebSocket,
  UserResource,
  VideoResource,
  allTools,
  collectCompletedObjects,
  createMcpContext,
  createMcpServer,
  loadConfig,
  runTransport,
  startHttp,
  startStdio,
  toPublicUser,
  withRequestContext
};
