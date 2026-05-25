const DEBUGGER_VERSION = "1.3";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;

const DEFAULT_VAR_ORDER_SCRIPT = String.raw`
return (async () => {
  const MAX_QUOTE_USD = 25;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const textOf = (el) =>
    (el.innerText || el.textContent || el.value || el.getAttribute("aria-label") || "")
      .replace(/\s+/g, " ")
      .trim();
  const visible = (el) => {
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0 && r.bottom > 0 && r.right > 0;
  };
  const click = (el) => {
    el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window }));
  };
  const setInputValue = (input, value) => {
    const setter =
      Object.getOwnPropertyDescriptor(Object.getPrototypeOf(input), "value")?.set ||
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    setter.call(input, String(value));
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const side = String(payload.side || "").toUpperCase();
  const market = String(payload.market || "BTC").toUpperCase();
  const amountMode = String(payload.amountMode || "quote").toLowerCase();
  const amount = String(payload.amount || payload.notionalUsd || "").trim();

  if (!["BUY", "SELL"].includes(side)) {
    throw new Error("Unsupported side: " + payload.side);
  }
  if (!amount || Number(amount) <= 0) {
    throw new Error("Invalid amount: " + amount);
  }
  if (amountMode === "quote" && Number(amount) > MAX_QUOTE_USD) {
    throw new Error("Quote amount " + amount + " exceeds script safety cap " + MAX_QUOTE_USD);
  }

  const sideWord = side === "BUY" ? "Buy" : "Sell";
  const panels = [...document.querySelectorAll("div")]
    .filter(visible)
    .filter((el) => {
      const t = textOf(el);
      const r = el.getBoundingClientRect();
      return r.x > window.innerWidth * 0.45 &&
        t.includes("Available to Trade") &&
        t.includes("Current Position") &&
        t.includes("Size");
    });
  const panel = panels.sort((a, b) => b.getBoundingClientRect().width - a.getBoundingClientRect().width)[0];
  if (!panel) {
    throw new Error("Order panel not found.");
  }

  const buttons = [...panel.querySelectorAll("button")].filter(visible);
  const marketButton = buttons.find((btn) => textOf(btn) === "Market");
  if (marketButton && !marketButton.disabled) {
    click(marketButton);
    await sleep(80);
  }

  const sideButton = buttons.find((btn) => {
    const t = textOf(btn);
    return t.startsWith(sideWord) && t.includes("$") && t !== sideWord + " " + market;
  });
  if (sideButton && !sideButton.disabled && sideButton.getAttribute("aria-disabled") !== "true") {
    click(sideButton);
    await sleep(150);
  }

  const unitButton = [...panel.querySelectorAll("button")]
    .filter(visible)
    .find((btn) => ["$", market].includes(textOf(btn)));
  if (unitButton) {
    const currentUnit = textOf(unitButton);
    const wantedUnit = amountMode === "base" ? market : "$";
    if (currentUnit !== wantedUnit) {
      click(unitButton);
      await sleep(100);
    }
  }

  const amountInput = [...panel.querySelectorAll('input[type="text"]')]
    .filter(visible)
    .sort((a, b) => b.getBoundingClientRect().width - a.getBoundingClientRect().width)[0];
  if (!amountInput) {
    throw new Error("Size input not found.");
  }

  amountInput.focus();
  setInputValue(amountInput, amount);
  await sleep(250);

  const submitButton = [...panel.querySelectorAll("button")]
    .filter(visible)
    .find((btn) => textOf(btn) === sideWord + " " + market);
  if (!submitButton) {
    throw new Error(sideWord + " " + market + " submit button not found.");
  }

  const result = {
    ok: true,
    side,
    market,
    amount,
    amountMode,
    inputValue: amountInput.value,
    submitText: textOf(submitButton),
    submitDisabled: Boolean(submitButton.disabled) || submitButton.getAttribute("aria-disabled") === "true",
    seenAt: new Date().toISOString()
  };
  if (result.submitDisabled) {
    throw new Error("Submit button disabled: " + JSON.stringify(result));
  }

  click(submitButton);
  return { ...result, clickedAt: new Date().toISOString() };
})();
`.trim();

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  varOrderScript: DEFAULT_VAR_ORDER_SCRIPT,
  restAllowlist: [
    "https://omni.variational.io/api/quotes/indicative"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastError: null,
  lastCommandAt: null,
  lastCommandResult: null,
  lastAutoReloadAt: 0
};

class ForwardSocket {
  constructor(label, configKey) {
    this.label = label;
    this.configKey = configKey;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.flush();
        if (this.configKey === "wsEndpoint") {
          autoReloadAttachedTab("forward receiver connected");
        }
        notifyStatus();
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      this.ws.send(this.queue.shift());
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

class CommandSocket extends ForwardSocket {
  constructor() {
    super("command", "commandEndpoint");
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        socket.send(JSON.stringify({ type: "REGISTER", role: "extension", timestamp: nowIso() }));
        this.flush();
        notifyStatus();
      };

      socket.onmessage = (event) => {
        if (this.ws !== socket) {
          return;
        }
        handleCommandMessage(event.data).catch((error) => {
          state.lastError = `Command handling failed: ${error.message}`;
          notifyStatus();
        });
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");
const commandSocket = new CommandSocket();

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `Auto reload failed (${reason}): ${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  state.configLoaded = true;
}

function sanitizeConfig(incoming = {}) {
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    commandEndpoint: asStringOrDefault(incoming.commandEndpoint, DEFAULT_CONFIG.commandEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    varOrderScript: asStringOrDefault(incoming.varOrderScript, DEFAULT_CONFIG.varOrderScript),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  const strict = cleaned.filter((item) => item === DEFAULT_CONFIG.restAllowlist[0]);
  if (!strict.length) {
    return [...DEFAULT_CONFIG.restAllowlist];
  }
  return strict;
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function getActiveTabId() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs.length || tabs[0].id == null) {
    throw new Error("No active tab found.");
  }
  return tabs[0].id;
}

async function startForwarding(tabId = null) {
  await ensureConfigLoaded();

  if (state.active) {
    return getStatus();
  }

  const targetTabId = tabId ?? (await getActiveTabId());
  await debuggerAttach(targetTabId);

  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
    await sendDebuggerCommand(targetTabId, "Runtime.enable");
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.lastError = null;
  wsForwarder.connect();
  restForwarder.connect();
  commandSocket.connect();
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `Debugger detach failed: ${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  wsForwarder.close();
  restForwarder.close();
  commandSocket.close();
}

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandSocket.status
    },
    lastError: state.lastError,
    lastCommandAt: state.lastCommandAt,
    lastCommandResult: state.lastCommandResult
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function sendCommandResult(requestId, payload) {
  commandSocket.send({
    type: "ORDER_RESULT",
    requestId,
    timestamp: nowIso(),
    ...payload
  });
}

async function handleCommandMessage(rawMessage) {
  let payload;
  try {
    payload = JSON.parse(rawMessage);
  } catch {
    return;
  }
  if (!payload || typeof payload !== "object") {
    return;
  }

  const msgType = String(payload.type || "").toUpperCase();
  if (msgType === "REGISTER_ACK" || msgType === "ORDER_DISPATCHED" || msgType === "PONG") {
    return;
  }
  if (msgType === "PING") {
    commandSocket.send({ type: "PONG", timestamp: nowIso() });
    return;
  }
  if (msgType !== "PLACE_ORDER") {
    return;
  }

  const requestId = String(payload.requestId || "");
  state.lastCommandAt = nowIso();
  try {
    const result = await executeVariationalOrderCommand(payload);
    state.lastCommandResult = {
      requestId,
      ok: Boolean(result.ok),
      dryRun: Boolean(result.dryRun),
      completedAt: nowIso()
    };
    sendCommandResult(requestId, result);
  } catch (error) {
    const result = {
      ok: false,
      error: error.message,
      timings: { totalMs: 0 }
    };
    state.lastCommandResult = {
      requestId,
      ok: false,
      completedAt: nowIso(),
      error: error.message
    };
    sendCommandResult(requestId, result);
  } finally {
    notifyStatus();
  }
}

async function executeVariationalOrderCommand(command) {
  const startedAt = performance.now();
  const cdpStartedAt = nowIso();
  if (!state.active || state.attachedTabId == null) {
    throw new Error("Forwarder is not attached to a Variational tab.");
  }

  const orderPayload = {
    requestId: command.requestId,
    signalId: command.signalId || null,
    side: command.side,
    amount: command.amount,
    amountMode: command.amountMode || "quote",
    market: command.market || null,
    account: command.account || null,
    dryRun: command.dryRun !== false,
    referencePrice: command.referencePrice || null,
    baseQty: command.baseQty || null,
    notionalUsd: command.notionalUsd || null,
    timeoutMs: command.timeoutMs || null,
    receivedAt: nowIso()
  };

  if (orderPayload.dryRun) {
    return {
      ok: true,
      dryRun: true,
      requestId: command.requestId,
      signalId: command.signalId || null,
      order: orderPayload,
      timings: {
        extensionReceivedAt: cdpStartedAt,
        totalMs: performance.now() - startedAt
      }
    };
  }

  if (!state.config.varOrderScript.trim()) {
    throw new Error("Live Var order requested, but varOrderScript is empty in the extension config.");
  }

  const expression = `
    (async () => {
      const payload = ${JSON.stringify(orderPayload)};
      const userCode = ${JSON.stringify(state.config.varOrderScript)};
      const fn = new Function("payload", userCode);
      return await fn(payload);
    })()
  `;
  const evaluateStarted = performance.now();
  const result = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
    expression,
    awaitPromise: true,
    returnByValue: true,
    timeout: Number(command.timeoutMs || 5000)
  });

  if (result.exceptionDetails) {
    const details = result.exceptionDetails;
    const message = details.text || details.exception?.description || "Runtime.evaluate failed.";
    throw new Error(message);
  }

  const value = result.result?.value ?? null;
  const totalMs = performance.now() - startedAt;
  return {
    ok: true,
    dryRun: false,
    requestId: command.requestId,
    signalId: command.signalId || null,
    order: orderPayload,
    pageResult: value,
    timings: {
      extensionReceivedAt: cdpStartedAt,
      evaluateMs: performance.now() - evaluateStarted,
      totalMs
    }
  };
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "Unknown WebSocket frame error"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `Debugger detached: ${reason}`;
  cleanupForwardingState();
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
        commandSocket.restart();
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    return { ok: false, error: `Unknown action: ${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});
