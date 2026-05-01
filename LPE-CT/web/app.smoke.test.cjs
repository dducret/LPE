const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const vm = require("node:vm");

class MockClassList {
  constructor() {
    this.values = new Set();
  }

  add(...names) {
    names.forEach((name) => this.values.add(name));
  }

  remove(...names) {
    names.forEach((name) => this.values.delete(name));
  }

  toggle(name, force) {
    if (force === undefined) {
      if (this.values.has(name)) {
        this.values.delete(name);
        return false;
      }
      this.values.add(name);
      return true;
    }
    if (force) {
      this.values.add(name);
      return true;
    }
    this.values.delete(name);
    return false;
  }

  contains(name) {
    return this.values.has(name);
  }
}

class MockElement {
  constructor(id = "", tagName = "div") {
    this.id = id;
    this.tagName = tagName.toUpperCase();
    this.dataset = {};
    this.classList = new MockClassList();
    this.listeners = {};
    this.children = [];
    this.attributes = {};
    this.textContent = "";
    this.innerHTML = "";
    this.value = "";
    this.disabled = false;
    this.checked = false;
    this.elements = { namedItem: () => null };
  }

  addEventListener(type, handler) {
    this.listeners[type] = handler;
  }

  appendChild(child) {
    this.children.push(child);
    return child;
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  getAttribute(name) {
    return this.attributes[name];
  }

  querySelector(selector) {
    if (selector === 'button[type="submit"]') {
      return this.submitButton ?? null;
    }
    return null;
  }

  querySelectorAll() {
    return [];
  }

  closest() {
    return null;
  }

  focus() {}
}

class MockFormData {
  constructor(form) {
    this.entriesList = form && typeof form.__formData === "function" ? form.__formData() : [];
  }

  entries() {
    return this.entriesList[Symbol.iterator]();
  }

  get(name) {
    const entry = this.entriesList.find(([key]) => key === name);
    return entry ? entry[1] : null;
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

function createForm(id, fields) {
  const form = new MockElement(id, "form");
  const fieldMap = {};
  for (const [name, config] of Object.entries(fields)) {
    fieldMap[name] = { name, value: config.value ?? "", checked: Boolean(config.checked) };
  }
  form.elements = {
    namedItem(name) {
      return fieldMap[name] ?? null;
    },
  };
  form.__formData = () =>
    Object.entries(fieldMap).map(([name, field]) => [name, field.checked && field.value === "" ? "on" : field.value]);
  form.submitButton = new MockElement(`${id}-submit`, "button");
  form.submitButton.textContent = "Submit";
  return form;
}

function createFetchStub() {
  const dashboard = {
    site: {
      node_name: "ct-node-1",
      role: "sorting-center",
      region: "eu-central",
      dmz_zone: "dmz-a",
      published_mx: "mx1.example.test",
      management_fqdn: "ct.example.test",
      public_smtp_bind: "0.0.0.0:25",
      management_bind: "127.0.0.1:8080",
    },
    relay: {
      ha_enabled: true,
      primary_upstream: "lpe-core-a",
      secondary_upstream: "lpe-core-b",
      core_delivery_base_url: "https://lpe-core.internal",
      mutual_tls_required: false,
      fallback_to_hold_queue: true,
      sync_interval_seconds: 30,
      lan_dependency_note: "relay note",
    },
    network: {
      allowed_management_cidrs: ["10.0.0.0/24"],
      allowed_upstream_cidrs: ["10.1.0.0/24"],
      outbound_smart_hosts: ["relay.example.test"],
      public_listener_enabled: true,
      submission_listener_enabled: true,
      proxy_protocol_enabled: false,
      max_concurrent_sessions: 120,
    },
    updates: {
      channel: "stable",
      auto_download: false,
      maintenance_window: "Sun 02:00",
      last_applied_release: "1.0.0",
      update_source: "git",
    },
    reporting: {
      digest_enabled: true,
      digest_interval_minutes: 30,
      digest_max_items: 25,
      history_retention_days: 14,
      last_digest_run_at: "2026-05-01T10:00:00Z",
      next_digest_run_at: "2026-05-01T10:30:00Z",
      domain_defaults: [{ domain: "example.test", recipients: ["ops@example.test"] }],
      user_overrides: [{ mailbox: "security@example.test", recipient: "security@example.test", enabled: true }],
    },
    policies: {
      drain_mode: false,
      require_spf: true,
      require_dkim_alignment: false,
      require_dmarc_enforcement: true,
      defer_on_auth_tempfail: true,
      bayespam_enabled: true,
      reputation_enabled: true,
      spam_quarantine_threshold: 5.0,
      spam_reject_threshold: 9.0,
      reputation_quarantine_threshold: -4,
      reputation_reject_threshold: -8,
      address_policy: {
        allow_senders: ["trusted@example.test"],
        block_senders: [],
        allow_recipients: [],
        block_recipients: ["blocked@example.test"],
      },
      attachment_policy: {
        allow_extensions: ["pdf"],
        block_extensions: ["exe"],
        allow_mime_types: [],
        block_mime_types: ["application/x-msdownload"],
        allow_detected_types: [],
        block_detected_types: ["exe"],
      },
      recipient_verification: {
        enabled: true,
        fail_closed: true,
        cache_ttl_seconds: 300,
      },
      dkim: {
        enabled: true,
        over_sign: false,
        headers: ["from", "subject"],
        expiration_seconds: 3600,
        domains: [{ domain: "example.test", selector: "mail", private_key_path: "/keys/mail.pem", enabled: true }],
      },
    },
    queues: {
      upstream_reachable: true,
      inbound_messages: 4,
      incoming_messages: 3,
      active_messages: 1,
      deferred_messages: 2,
      quarantined_messages: 1,
      corrupt_messages: 0,
      delivery_attempts_last_hour: 12,
    },
    system: {
      host_time: "2026-05-01T10:06:00Z",
      hostname: "ct-node-1",
      uptime_seconds: 172800,
      cpu_utilization_percent: 18.5,
      processor_type: "x86_64",
      processor_speed_mhz: 2600,
      os_name: "Debian",
      architecture: "x86_64",
      memory_used_percent: 45,
      memory_total_bytes: 17179869184,
      disk_used_percent: 62,
      disk_total_bytes: 274877906944,
      network_interfaces: [
        {
          name: "eth0",
          address: "10.0.0.10",
          netmask: "255.255.255.0",
          default_gateway: "10.0.0.1",
        },
      ],
    },
    audit: [{ action: "policy.updated", actor: "admin@example.test", timestamp: "2026-05-01T10:05:00Z", details: "Updated policy." }],
  };

  const routes = { relay_targets: ["lpe-core-a"] };
  const reporting = { settings: dashboard.reporting, recent_reports: [] };
  const policyStatus = {
    recipient_verification: {
      enabled: true,
      fail_closed: true,
      cache_ttl_seconds: 300,
      cache_backend: "private-postgres",
      operational_state: "active",
    },
    dkim: {
      enabled: true,
      over_sign: false,
      headers: ["from", "subject"],
      expiration_seconds: 3600,
      domains: [{ domain: "example.test", selector: "mail", private_key_path: "/keys/mail.pem", enabled: true, key_status: "present" }],
    },
  };
  const quarantine = [
    {
      trace_id: "trace-1",
      subject: "Suspicious inbound",
      received_at: "2026-05-01T10:03:00Z",
      mail_from: "sender@example.test",
      rcpt_to: ["user@example.test"],
      status: "quarantined",
      direction: "inbound",
      reason: "spam threshold",
      internet_message_id: "<msg1@example.test>",
      spam_score: 6.4,
      security_score: 1.1,
    },
  ];
  const history = {
    items: [
      {
        trace_id: "lpe-ct-in-177764830abcdef",
        subject: "Outbound delivery",
        latest_event_at: "2026-05-01T09:50:00Z",
        mail_from: "<ops@example.test> SIZE=2048",
        rcpt_to: ["dest@example.org"],
        peer: "203.0.113.44:587",
        queue: "sent",
        status: "relayed",
        direction: "outbound",
        reason: "250 ok",
        route_target: "relay.example.test",
        event_count: 3,
        spam_score: 2.1,
        message_size_bytes: 2048,
        policy_tags: ["dkim", "relay"],
      },
      {
        trace_id: "trace-3",
        subject: "Spam inbound",
        latest_event_at: "2026-04-30T09:50:00Z",
        mail_from: "spam@example.test",
        rcpt_to: ["dest@example.org"],
        queue: "quarantine",
        status: "quarantined",
        direction: "inbound",
        reason: "spam threshold",
        route_target: "relay.example.test",
        event_count: 2,
        spam_score: 7.2,
        message_size_bytes: 4096,
        security_score: 0,
        dnsbl_hits: [],
        policy_tags: ["spam"],
      },
    ],
  };

  return async function fetchStub(url) {
    const ok = (body) => ({
      ok: true,
      status: 200,
      headers: { get: () => "application/json" },
      async json() {
        return body;
      },
      async text() {
        return JSON.stringify(body);
      },
    });

    if (url === "/api/dashboard") return ok(dashboard);
    if (String(url).startsWith("/api/quarantine")) return ok(quarantine);
    if (String(url).startsWith("/api/history?")) return ok(history);
    if (url === "/api/routes/diagnostics") return ok(routes);
    if (url === "/api/reporting") return ok(reporting);
    if (url === "/api/reporting/digests") return ok([]);
    if (url === "/api/policies/status") return ok(policyStatus);

    throw new Error(`Unexpected fetch url: ${url}`);
  };
}

function createContext() {
  const ids = [
    "feedback",
    "login-feedback",
    "login-shell",
    "console-shell",
    "main-workspace",
    "sidebar",
    "sidebar-backdrop",
    "sidebar-toggle",
    "mobile-sidebar-toggle",
    "drawer-backdrop",
    "drawer",
    "drawer-title",
    "drawer-summary",
    "drawer-content",
    "drawer-close",
    "refresh",
    "refresh-toolbar",
    "run-digests",
    "create-address-rule",
    "create-attachment-rule",
    "edit-filtering-policy",
    "edit-recipient-verification",
    "edit-dkim-settings",
    "create-dkim-domain",
    "edit-digest-settings",
    "create-digest-default",
    "create-digest-override",
    "operator-email",
    "operator-role",
    "system-overview-list",
    "queue-status-list",
    "scanner-status-list",
    "relay-health-list",
    "top-spam-relays-list",
    "top-virus-relays-list",
    "top-viruses-list",
    "scan-summary-list",
    "traffic-chart",
    "traffic-table",
    "quarantine-list",
    "history-list",
    "filtering-policy-status",
    "address-rules-list",
    "attachment-rules-list",
    "recipient-verification-status",
    "dkim-domain-list",
    "digest-settings-list",
    "digest-defaults-list",
    "digest-overrides-list",
    "digest-report-list",
    "platform-list",
    "mail-log",
    "audit-log",
    "message-log",
    "email-alert-log",
  ];

  const elements = Object.fromEntries(ids.map((id) => [id, new MockElement(id)]));
  elements["login-form"] = createForm("login-form", {
    email: { value: "" },
    password: { value: "" },
  });
  elements["quarantine-search-form"] = createForm("quarantine-search-form", {
    q: { value: "" },
    direction: { value: "" },
    domain: { value: "" },
  });
  elements["history-search-form"] = createForm("history-search-form", {
    q: { value: "" },
    direction: { value: "" },
    queue: { value: "" },
    disposition: { value: "" },
    domain: { value: "" },
  });

  const localePickers = [new MockElement("", "select"), new MockElement("", "select"), new MockElement("", "select")];
  const navButtons = ["dashboard", "system-setup", "filtering", "anti-spam", "quarantine", "reporting", "logs"].map((target) => {
    const button = new MockElement("", "button");
    button.dataset.pageTarget = target;
    return button;
  });
  const pageViews = ["dashboard", "anti-spam", "quarantine", "reporting", "filtering", "filtering", "filtering", "filtering", "reporting", "system-setup", "logs"].map((page) => {
    const view = new MockElement("", "section");
    view.dataset.pageView = page;
    return view;
  });

  const document = {
    title: "",
    body: new MockElement("body", "body"),
    documentElement: new MockElement("html", "html"),
    getElementById(id) {
      return elements[id] ?? null;
    },
    querySelectorAll(selector) {
      if (selector === "[data-locale-picker]") return localePickers;
      if (selector === "[data-nav-button]") return navButtons;
      if (selector === "[data-page-view]") return pageViews;
      return [];
    },
    createElement(tagName) {
      return new MockElement("", tagName);
    },
    addEventListener() {},
  };

  const localStorage = {
    store: new Map(),
    getItem(key) {
      return this.store.has(key) ? this.store.get(key) : null;
    },
    setItem(key, value) {
      this.store.set(key, String(value));
    },
    removeItem(key) {
      this.store.delete(key);
    },
  };

  class MockIntersectionObserver {
    constructor() {}
    observe() {}
    disconnect() {}
  }

  const window = {
    document,
    localStorage,
    navigator: { languages: ["en-US"], language: "en-US" },
    location: { hash: "" },
    history: { replaceState() {} },
    fetch: createFetchStub(),
    __intervals: [],
    requestAnimationFrame(callback) {
      callback();
    },
    setInterval(callback, delay) {
      this.__intervals.push(delay);
      const handle = setInterval(callback, delay);
      handle.unref?.();
      return handle;
    },
    clearInterval,
    IntersectionObserver: MockIntersectionObserver,
    addEventListener() {},
    console,
  };

  const context = {
    window,
    document,
    localStorage,
    navigator: window.navigator,
    fetch: window.fetch,
    requestAnimationFrame: window.requestAnimationFrame,
    IntersectionObserver: MockIntersectionObserver,
    URLSearchParams,
    FormData: MockFormData,
    console,
    setTimeout,
    clearTimeout,
  };

  window.window = window;
  return { context, elements, document, navButtons, pageViews };
}

async function main() {
  const { context, elements, document, navButtons, pageViews } = createContext();
  const i18nSource = fs.readFileSync(path.join(__dirname, "i18n.js"), "utf8");
  globalThis.window = context.window;
  globalThis.document = context.document;
  globalThis.localStorage = context.localStorage;
  Object.defineProperty(globalThis, "navigator", {
    value: context.navigator,
    configurable: true,
    writable: true,
  });
  globalThis.fetch = context.fetch;
  globalThis.requestAnimationFrame = context.requestAnimationFrame;
  globalThis.IntersectionObserver = context.IntersectionObserver;
  globalThis.URLSearchParams = context.URLSearchParams;
  globalThis.FormData = context.FormData;
  globalThis.setTimeout = context.setTimeout;
  globalThis.clearTimeout = context.clearTimeout;
  globalThis.setInterval = setInterval;
  globalThis.clearInterval = clearInterval;
  vm.runInThisContext(i18nSource, { filename: "i18n.js" });
  await import(pathToFileURL(path.join(__dirname, "app.js")).href);

  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(document.title, "LPE-CT Management Console");
  assert.match(elements["quarantine-list"].innerHTML, /trace-1/);
  assert.match(elements["quarantine-list"].innerHTML, /quarantine-bulk-actions/);
  assert.match(elements["quarantine-list"].innerHTML, /data-bulk-action="release"/);
  assert.match(elements["quarantine-list"].innerHTML, /data-bulk-action="allow"/);
  assert.match(elements["quarantine-list"].innerHTML, /data-bulk-action="block"/);
  assert.match(elements["quarantine-list"].innerHTML, /data-bulk-action="delete"/);
  assert.match(elements["quarantine-list"].innerHTML, /data-quarantine-select/);
  assert.match(elements["quarantine-list"].innerHTML, /data-sort-key="from"/);
  assert.match(elements["quarantine-list"].innerHTML, /sender@example\.test/);
  assert.match(elements["quarantine-list"].innerHTML, /user@example\.test/);
  assert.match(elements["quarantine-list"].innerHTML, /Suspicious inbound/);
  assert.match(elements["quarantine-list"].innerHTML, /6\.4/);
  assert.doesNotMatch(elements["quarantine-list"].innerHTML, /trace-open/);
  assert.match(elements["history-list"].innerHTML, /177764830abcdef/);
  assert.match(elements["history-list"].innerHTML, /Client Address/);
  assert.match(elements["history-list"].innerHTML, /data-sort-key="date"/);
  assert.match(elements["history-list"].innerHTML, /data-history-resizer/);
  assert.match(elements["history-list"].innerHTML, /2026-05-01 \d{2}:50:00/);
  assert.match(elements["history-list"].innerHTML, /203\.0\.113\.44/);
  assert.doesNotMatch(elements["history-list"].innerHTML, /203\.0\.113\.44:587/);
  assert.match(elements["history-list"].innerHTML, /ops@example\.test/);
  assert.doesNotMatch(elements["history-list"].innerHTML, /SIZE=2048/);
  assert.match(elements["history-list"].innerHTML, /Clean \(2\.10\)/);
  assert.match(elements["history-list"].innerHTML, /2 KB/);
  assert.match(elements["mail-log"].innerHTML, /177764830abcdef/);
  assert.match(elements["mail-log"].innerHTML, /data-action="log-sort"/);
  assert.match(elements["mail-log"].innerHTML, /data-log-resizer/);
  assert.doesNotMatch(elements["mail-log"].innerHTML, /trace-open/);
  assert.match(elements["audit-log"].innerHTML, /policy\.updated/);
  assert.match(elements["audit-log"].innerHTML, /admin@example\.test/);
  assert.match(elements["audit-log"].innerHTML, /data-log-table="interface"/);
  assert.match(elements["message-log"].innerHTML, /System message logs are not exposed/);
  assert.match(elements["email-alert-log"].innerHTML, /Email alert logs are not exposed/);
  assert.match(elements["platform-list"].innerHTML, /Network/);
  assert.match(elements["platform-list"].innerHTML, /IP/);
  assert.match(elements["platform-list"].innerHTML, /eth0/);
  assert.match(elements["system-overview-list"].innerHTML, /CPU utilization/);
  assert.match(elements["queue-status-list"].innerHTML, /Corrupt queue/);
  assert.match(elements["scan-summary-list"].innerHTML, /Spam messages/);
  assert.match(elements["traffic-table"].innerHTML, /Invalid rcpts/);
  assert.match(elements["filtering-policy-status"].innerHTML, /SPF enforcement/);
  assert.match(elements["filtering-policy-status"].innerHTML, /Spam reject threshold/);
  assert.ok(context.window.__intervals.includes(60_000));
  assert.equal(navButtons[0].getAttribute("aria-current"), "true");
  assert.equal(pageViews[0].classList.contains("page-view-active"), true);
  assert.equal(pageViews[1].classList.contains("hidden"), true);
  assert.equal(pageViews[1].getAttribute("aria-hidden"), "true");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
