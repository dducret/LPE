const feedback = document.getElementById("feedback");
const loginFeedback = document.getElementById("login-feedback");
const loginShell = document.getElementById("login-shell");
const consoleShell = document.getElementById("console-shell");
const configDrawer = document.getElementById("config-drawer");
const drawerTitle = document.getElementById("drawer-title");
const drawerSummary = document.getElementById("drawer-summary");
const drawerPanels = Array.from(document.querySelectorAll(".drawer-panel"));
const panelTriggers = Array.from(document.querySelectorAll("[data-open-panel]"));
const AUTH_TOKEN_KEY = "lpeCtAdminToken";
const LAST_ADMIN_EMAIL_KEY = "lpeCtAdminLastEmail";

const loginEmailField = document.querySelector("#login-form input[name='email']");
if (loginEmailField) {
  loginEmailField.value = window.localStorage.getItem(LAST_ADMIN_EMAIL_KEY) ?? "";
}

function authHeaders() {
  const token = window.localStorage.getItem(AUTH_TOKEN_KEY);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function fetchDashboard() {
  const response = await fetch("/api/dashboard", { headers: authHeaders() });
  if (!response.ok) {
    throw new Error(`dashboard request failed: ${response.status}`);
  }
  return response.json();
}

async function submitForm(path, payload, successMessage) {
  const response = await fetch(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }

  const dashboard = await response.json();
  render(dashboard);
  closeDrawer();
  showFeedback(successMessage, false);
}

function openDrawer(panelId, title, summary) {
  drawerPanels.forEach((panel) => {
    panel.classList.toggle("hidden", panel.id !== panelId);
  });
  panelTriggers.forEach((trigger) => {
    trigger.classList.toggle("is-active", trigger.dataset.openPanel === panelId);
  });
  drawerTitle.textContent = title;
  drawerSummary.textContent = summary;
  configDrawer.classList.remove("hidden");
}

function closeDrawer() {
  panelTriggers.forEach((trigger) => trigger.classList.remove("is-active"));
  configDrawer.classList.add("hidden");
}

function showFeedback(message, isError) {
  feedback.textContent = message;
  feedback.className = isError ? "feedback error" : "feedback";
}

function showLoginFeedback(message, isError) {
  loginFeedback.textContent = message;
  loginFeedback.className = isError ? "feedback error" : "feedback";
}

function csvLines(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function assignValues(form, values) {
  Object.entries(values).forEach(([key, value]) => {
    const field = form.elements.namedItem(key);
    if (!field) {
      return;
    }

    if (field.type === "checkbox") {
      field.checked = Boolean(value);
      return;
    }

    field.value = Array.isArray(value) ? value.join("\n") : String(value);
  });
}

function renderAudit(audit) {
  const container = document.getElementById("audit-log");
  container.innerHTML = "";

  audit.forEach((entry) => {
    const row = document.createElement("article");
    row.className = "audit-entry";
    row.innerHTML = `<strong>${entry.action}</strong><span>${entry.actor}</span><span>${entry.timestamp}</span><p>${entry.details}</p>`;
    container.appendChild(row);
  });
}

function render(dashboard) {
  document.getElementById("node-name").textContent = dashboard.site.node_name;
  document.getElementById("hero-summary").textContent =
    `${dashboard.site.dmz_zone} · MX ${dashboard.site.published_mx} · relais primaire ${dashboard.relay.primary_upstream}`;
  document.getElementById("status-badge").textContent = dashboard.policies.drain_mode ? "Drain mode" : "Production";
  document.getElementById("status-badge").className = dashboard.policies.drain_mode ? "badge warn" : "badge ok";
  document.getElementById("upstream-badge").textContent = dashboard.queues.upstream_reachable ? "LAN relay reachable" : "LAN relay unreachable";
  document.getElementById("upstream-badge").className = dashboard.queues.upstream_reachable ? "badge ok" : "badge danger";

  document.getElementById("metric-inbound").textContent = dashboard.queues.inbound_messages;
  document.getElementById("metric-deferred").textContent = dashboard.queues.deferred_messages;
  document.getElementById("metric-quarantine").textContent = dashboard.queues.quarantined_messages;
  document.getElementById("metric-attempts").textContent = dashboard.queues.delivery_attempts_last_hour;

  assignValues(document.getElementById("site-form"), dashboard.site);
  assignValues(document.getElementById("relay-form"), dashboard.relay);
  assignValues(document.getElementById("network-form"), dashboard.network);
  assignValues(document.getElementById("policies-form"), dashboard.policies);
  assignValues(document.getElementById("updates-form"), dashboard.updates);
  renderAudit(dashboard.audit);
}

function formPayloads() {
  return {
    site: () => {
      const form = document.getElementById("site-form");
      return Object.fromEntries(new FormData(form).entries());
    },
    relay: () => {
      const form = document.getElementById("relay-form");
      return {
        primary_upstream: form.elements.namedItem("primary_upstream").value,
        secondary_upstream: form.elements.namedItem("secondary_upstream").value,
        sync_interval_seconds: Number(form.elements.namedItem("sync_interval_seconds").value),
        lan_dependency_note: form.elements.namedItem("lan_dependency_note").value,
        mutual_tls_required: form.elements.namedItem("mutual_tls_required").checked,
        fallback_to_hold_queue: form.elements.namedItem("fallback_to_hold_queue").checked,
      };
    },
    network: () => {
      const form = document.getElementById("network-form");
      return {
        allowed_management_cidrs: csvLines(form.elements.namedItem("allowed_management_cidrs").value),
        allowed_upstream_cidrs: csvLines(form.elements.namedItem("allowed_upstream_cidrs").value),
        outbound_smart_hosts: csvLines(form.elements.namedItem("outbound_smart_hosts").value),
        public_listener_enabled: form.elements.namedItem("public_listener_enabled").checked,
        submission_listener_enabled: form.elements.namedItem("submission_listener_enabled").checked,
        proxy_protocol_enabled: form.elements.namedItem("proxy_protocol_enabled").checked,
        max_concurrent_sessions: Number(form.elements.namedItem("max_concurrent_sessions").value),
      };
    },
    policies: () => {
      const form = document.getElementById("policies-form");
      return {
        drain_mode: form.elements.namedItem("drain_mode").checked,
        quarantine_enabled: form.elements.namedItem("quarantine_enabled").checked,
        greylisting_enabled: form.elements.namedItem("greylisting_enabled").checked,
        require_spf: form.elements.namedItem("require_spf").checked,
        require_dkim_alignment: form.elements.namedItem("require_dkim_alignment").checked,
        require_dmarc_enforcement: form.elements.namedItem("require_dmarc_enforcement").checked,
        attachment_text_scan_enabled: form.elements.namedItem("attachment_text_scan_enabled").checked,
        max_message_size_mb: Number(form.elements.namedItem("max_message_size_mb").value),
      };
    },
    updates: () => {
      const form = document.getElementById("updates-form");
      return {
        channel: form.elements.namedItem("channel").value,
        auto_download: form.elements.namedItem("auto_download").checked,
        maintenance_window: form.elements.namedItem("maintenance_window").value,
        last_applied_release: form.elements.namedItem("last_applied_release").value,
        update_source: form.elements.namedItem("update_source").value,
      };
    },
  };
}

async function load() {
  try {
    const dashboard = await fetchDashboard();
    render(dashboard);
    loginShell.classList.add("hidden");
    consoleShell.classList.remove("hidden");
    feedback.className = "feedback hidden";
  } catch (error) {
    if (error instanceof Error && error.message.includes("401")) {
      window.localStorage.removeItem(AUTH_TOKEN_KEY);
      consoleShell.classList.add("hidden");
      loginShell.classList.remove("hidden");
      showLoginFeedback("Management authentication required.", true);
      return;
    }
    showFeedback(error instanceof Error ? error.message : "unknown error", true);
  }
}

async function loginAdmin() {
  const form = document.getElementById("login-form");
  const payload = Object.fromEntries(new FormData(form).entries());
  const response = await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`login request failed: ${response.status}`);
  }
  const body = await response.json();
  window.localStorage.setItem(AUTH_TOKEN_KEY, body.token);
  if (typeof payload.email === "string" && payload.email.trim()) {
    window.localStorage.setItem(LAST_ADMIN_EMAIL_KEY, payload.email.trim());
  }
  showLoginFeedback("Authenticated.", false);
  await load();
}

document.getElementById("refresh").addEventListener("click", () => {
  void load();
});

const refreshToolbar = document.getElementById("refresh-toolbar");
if (refreshToolbar) {
  refreshToolbar.addEventListener("click", () => {
    void load();
  });
}

panelTriggers.forEach((button) => {
  button.addEventListener("click", () => {
    openDrawer(button.dataset.openPanel, button.dataset.title, button.dataset.summary);
  });
});

document.getElementById("drawer-close").addEventListener("click", closeDrawer);

configDrawer.addEventListener("click", (event) => {
  if (event.target === configDrawer) {
    closeDrawer();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !configDrawer.classList.contains("hidden")) {
    closeDrawer();
  }
});

const payloads = formPayloads();

document.getElementById("site-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/site", payloads.site(), "Profil DMZ enregistre.");
});

document.getElementById("relay-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/relay", payloads.relay(), "Relais LAN enregistre.");
});

document.getElementById("network-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/network", payloads.network(), "Surface reseau enregistree.");
});

document.getElementById("policies-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/policies", payloads.policies(), "Politiques de tri enregistrees.");
});

document.getElementById("updates-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/updates", payloads.updates(), "Politique de mise a jour enregistree.");
});

document.getElementById("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void loginAdmin().catch((error) => {
    showLoginFeedback(error instanceof Error ? error.message : "unknown error", true);
  });
});

void load();
