const $ = (id) => document.getElementById(id);

const inputs = {
  domainFilter: $("domainFilter"),
  wsEndpoint: $("wsEndpoint"),
  restEndpoint: $("restEndpoint"),
  commandEndpoint: $("commandEndpoint"),
  restAllowlist: $("restAllowlist"),
  varOrderScript: $("varOrderScript")
};

const statusEl = $("status");

const DEFAULTS = {
  domainFilter: "variational",
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  restAllowlist: ["https://omni.variational.io/api/quotes/indicative"],
  varOrderScript: ""
};

function configValue(status, key) {
  return status?.config?.[key] || DEFAULTS[key] || "";
}

function toStatusText(status) {
  const sockets = status.sockets || {};
  return [
    `Forwarding: ${status.active ? "ON" : "OFF"}`,
    `Attached tab: ${status.attachedTabId ?? "-"}`,
    `Domain filter: ${configValue(status, "domainFilter")}`,
    `WS socket (${configValue(status, "wsEndpoint")}): ${sockets.websocket || "disconnected"}`,
    `REST socket (${configValue(status, "restEndpoint")}): ${sockets.rest || "disconnected"}`,
    `Command socket (${configValue(status, "commandEndpoint")}): ${sockets.command || "disconnected"}`,
    `REST allowlist entries: ${(status.config?.restAllowlist || DEFAULTS.restAllowlist).length}`,
    `Last command: ${status.lastCommandAt || "-"}`,
    `Last command result: ${status.lastCommandResult ? JSON.stringify(status.lastCommandResult) : "-"}`,
    `Last error: ${status.lastError || "-"}`
  ].join("\n");
}

function updateFormFromStatus(status) {
  inputs.domainFilter.value = configValue(status, "domainFilter");
  inputs.wsEndpoint.value = configValue(status, "wsEndpoint");
  inputs.restEndpoint.value = configValue(status, "restEndpoint");
  inputs.commandEndpoint.value = configValue(status, "commandEndpoint");
  inputs.restAllowlist.value = (status.config?.restAllowlist || DEFAULTS.restAllowlist).join("\n");
  inputs.varOrderScript.value = configValue(status, "varOrderScript");
}

function updateStatus(status) {
  statusEl.textContent = toStatusText(status);
}

async function send(action, payload = {}) {
  const response = await chrome.runtime.sendMessage({
    action,
    ...payload
  });
  if (!response?.ok) {
    throw new Error(response?.error || "Unknown extension error");
  }
  return response.status;
}

function readConfig() {
  const restAllowlist = inputs.restAllowlist.value
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  return {
    domainFilter: inputs.domainFilter.value.trim() || DEFAULTS.domainFilter,
    wsEndpoint: inputs.wsEndpoint.value.trim() || DEFAULTS.wsEndpoint,
    restEndpoint: inputs.restEndpoint.value.trim() || DEFAULTS.restEndpoint,
    commandEndpoint: inputs.commandEndpoint.value.trim() || DEFAULTS.commandEndpoint,
    restAllowlist,
    varOrderScript: inputs.varOrderScript.value
  };
}

async function refreshStatus() {
  const status = await send("getStatus");
  updateFormFromStatus(status);
  updateStatus(status);
}

$("saveConfig").addEventListener("click", async () => {
  try {
    const status = await send("updateConfig", { config: readConfig() });
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Save failed: ${error.message}`;
  }
});

$("start").addEventListener("click", async () => {
  try {
    await send("updateConfig", { config: readConfig() });
    const status = await send("start");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Start failed: ${error.message}`;
  }
});

$("stop").addEventListener("click", async () => {
  try {
    const status = await send("stop");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Stop failed: ${error.message}`;
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message.event === "status" && message.status) {
    updateStatus(message.status);
  }
});

refreshStatus().catch((error) => {
  statusEl.textContent = `Failed to load status: ${error.message}`;
});
