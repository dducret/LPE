import { i18n, getCopy, translate } from './modules/i18n/index.js';

// DOM References
const elements = {
  feedback: document.getElementById("feedback"),
  loginFeedback: document.getElementById("login-feedback"),
  loginShell: document.getElementById("login-shell"),
  consoleShell: document.getElementById("console-shell"),
  mainWorkspace: document.getElementById("main-workspace"),
  sidebar: document.getElementById("sidebar"),
  sidebarBackdrop: document.getElementById("sidebar-backdrop"),
  sidebarToggle: document.getElementById("sidebar-toggle"),
  mobileSidebarToggle: document.getElementById("mobile-sidebar-toggle"),
  drawerBackdrop: document.getElementById("drawer-backdrop"),
  drawer: document.getElementById("drawer"),
  drawerTitle: document.getElementById("drawer-title"),
  drawerSummary: document.getElementById("drawer-summary"),
  drawerContent: document.getElementById("drawer-content"),
  drawerClose: document.getElementById("drawer-close"),
  localePickers: Array.from(document.querySelectorAll("[data-locale-picker]")),
  navButtons: Array.from(document.querySelectorAll("[data-nav-button]")),
  refresh: document.getElementById("refresh"),
  refreshToolbar: document.getElementById("refresh-toolbar"),
  runDigests: document.getElementById("run-digests"),
  loginForm: document.getElementById("login-form"),
  quarantineSearchForm: document.getElementById("quarantine-search-form"),
  historySearchForm: document.getElementById("history-search-form"),
  createAddressRule: document.getElementById("create-address-rule"),
  createAttachmentRule: document.getElementById("create-attachment-rule"),
  editRecipientVerification: document.getElementById("edit-recipient-verification"),
  editDkimSettings: document.getElementById("edit-dkim-settings"),
  createDkimDomain: document.getElementById("create-dkim-domain"),
  editDigestSettings: document.getElementById("edit-digest-settings"),
  createDigestDefault: document.getElementById("create-digest-default"),
  createDigestOverride: document.getElementById("create-digest-override"),
  nodeName: document.getElementById("node-name"),
  heroSummary: document.getElementById("hero-summary"),
  sidebarNodeName: document.getElementById("sidebar-node-name"),
  sidebarNodeCopy: document.getElementById("sidebar-node-copy"),
  statusBadge: document.getElementById("status-badge"),
  upstreamBadge: document.getElementById("upstream-badge"),
  heroPrimaryRelay: document.getElementById("hero-primary-relay"),
  heroRouteSummary: document.getElementById("hero-route-summary"),
  heroReportingSummary: document.getElementById("hero-reporting-summary"),
  heroReportingCopy: document.getElementById("hero-reporting-copy"),
  operatorEmail: document.getElementById("operator-email"),
  operatorRole: document.getElementById("operator-role"),
  contextOperator: document.getElementById("context-operator"),
  contextRole: document.getElementById("context-role"),
  contextVersion: document.getElementById("context-version"),
  contextLicense: document.getElementById("context-license"),
  contextBuild: document.getElementById("context-build"),
  contextTime: document.getElementById("context-time"),
  metricSystemHealth: document.getElementById("metric-system-health"),
  metricInbound: document.getElementById("metric-inbound"),
  metricDeferred: document.getElementById("metric-deferred"),
  metricQuarantine: document.getElementById("metric-quarantine"),
  metricAttempts: document.getElementById("metric-attempts"),
  metricHeld: document.getElementById("metric-held"),
  metricRoutingRules: document.getElementById("metric-routing-rules"),
  metricDkimDomains: document.getElementById("metric-dkim-domains"),
  metricRecipientVerification: document.getElementById("metric-recipient-verification"),
  queueStatusList: document.getElementById("queue-status-list"),
  scannerStatusList: document.getElementById("scanner-status-list"),
  relayHealthList: document.getElementById("relay-health-list"),
  topSpamRelaysList: document.getElementById("top-spam-relays-list"),
  topVirusRelaysList: document.getElementById("top-virus-relays-list"),
  topVirusesList: document.getElementById("top-viruses-list"),
  scanSummaryList: document.getElementById("scan-summary-list"),
  trafficChart: document.getElementById("traffic-chart"),
  trafficTable: document.getElementById("traffic-table"),
};

const containers = {
  quarantine: document.getElementById("quarantine-list"),
  history: document.getElementById("history-list"),
  addressRules: document.getElementById("address-rules-list"),
  attachmentRules: document.getElementById("attachment-rules-list"),
  recipientVerification: document.getElementById("recipient-verification-status"),
  dkimDomains: document.getElementById("dkim-domain-list"),
  digestSettings: document.getElementById("digest-settings-list"),
  digestDefaults: document.getElementById("digest-defaults-list"),
  digestOverrides: document.getElementById("digest-overrides-list"),
  digestReports: document.getElementById("digest-report-list"),
  platform: document.getElementById("platform-list"),
  audit: document.getElementById("audit-log"),
};

const AUTH_TOKEN_KEY = 'lpeCtAdminToken';
const LAST_ADMIN_EMAIL_KEY = 'lpeCtAdminLastEmail';

// Application State
const state = {
  dashboard: null,
  quarantine: [],
  history: [],
  routeDiagnostics: null,
  reporting: null,
  digestReports: [],
  policyStatus: null,
  selectedTrace: null,
  loading: {
    dashboard: false,
    ops: false,
    auth: false,
    trace: false,
    runDigests: false,
  },
  activeSection: "overview-section",
  drawer: {
    open: false,
    previousFocus: null,
    onClose: null,
  },
};

// Copy and Formatting Helpers
function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatList(values) {
  return (values ?? []).filter(Boolean).join(", ") || getCopy().unset;
}

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return new Intl.NumberFormat(i18n.getLocale()).format(Number(value));
}

function formatScore(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return Number(value).toFixed(1);
}

function formatDateTime(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return getCopy().unset;
  }
  return new Intl.DateTimeFormat(i18n.getLocale(), {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatShortDate(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return getCopy().unset;
  }
  return new Intl.DateTimeFormat(i18n.getLocale(), {
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatMetric(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "-";
  }
  return new Intl.NumberFormat(i18n.getLocale(), { notation: value >= 10000 ? "compact" : "standard" }).format(Number(value));
}

function formatDurationMinutes(seconds) {
  if (seconds === undefined || seconds === null || Number.isNaN(Number(seconds))) {
    return getCopy().unset;
  }
  const minutes = Math.max(1, Math.round(Number(seconds) / 60));
  return `${formatNumber(minutes)} min`;
}

function formatBooleanLabel(value) {
  return value ? getCopy().enabled : getCopy().disabled;
}

function healthPosture(dashboard) {
  const copy = getCopy();
  if (!dashboard) {
    return { label: "-", tone: "muted" };
  }
  if (dashboard.policies?.drain_mode) {
    return { label: copy.statusDrain, tone: "warn" };
  }
  if (dashboard.queues?.upstream_reachable === false) {
    return { label: copy.relayUnreachable, tone: "danger" };
  }
  return { label: copy.statusProduction, tone: "ok" };
}

function getOperatorEmail() {
  return window.localStorage.getItem(LAST_ADMIN_EMAIL_KEY) || getCopy().unset;
}

function getDigestSettings() {
  return state.reporting?.settings ?? state.dashboard?.reporting ?? null;
}

function getTrafficRecords() {
  return [...(state.history ?? []), ...(state.quarantine ?? [])];
}

function getRelayOrPeer(item) {
  return item?.peer || item?.route_target || item?.current?.peer || item?.current?.route?.relay_target || getCopy().unset;
}

function getPolicySignals() {
  return {
    verification: state.policyStatus?.recipient_verification ?? null,
    dkim: state.policyStatus?.dkim ?? null,
    reporting: getDigestSettings(),
  };
}

function showFeedback(message, type = "success") {
  elements.feedback.textContent = message;
  elements.feedback.className = type === "error" ? "feedback error" : type === "warning" ? "feedback warning" : "feedback";
}

function showLoginFeedback(message, type = "error") {
  elements.loginFeedback.textContent = message;
  elements.loginFeedback.className = type === "error" ? "feedback error" : "feedback";
}

function hideFeedback(target = elements.feedback) {
  target.className = "feedback hidden";
  target.textContent = "";
}

function setButtonBusy(button, busy, busyLabel, idleLabel) {
  if (!button) {
    return;
  }
  button.disabled = busy;
  button.dataset.idleLabel = button.dataset.idleLabel || idleLabel || button.textContent;
  button.textContent = busy ? busyLabel : button.dataset.idleLabel;
}

function setSidebarOpen(open) {
  document.body.classList.toggle("sidebar-open", open);
  elements.sidebarBackdrop.classList.toggle("hidden", !open);
  elements.mobileSidebarToggle?.setAttribute("aria-expanded", String(open));
  if (elements.mobileSidebarToggle) {
    elements.mobileSidebarToggle.textContent = open ? getCopy().closeNavigation : getCopy().openNavigation;
  }
}

function setSidebarCollapsed(collapsed) {
  document.body.classList.toggle("sidebar-collapsed", collapsed);
  try {
    window.localStorage.setItem("lpeCtSidebarCollapsed", collapsed ? "true" : "false");
  } catch {}
}

function toggleSidebarState() {
  if (window.innerWidth <= 1024) {
    setSidebarOpen(!document.body.classList.contains("sidebar-open"));
    return;
  }
  setSidebarCollapsed(!document.body.classList.contains("sidebar-collapsed"));
}

// API Helpers
function authHeaders() {
  const token = window.localStorage.getItem(AUTH_TOKEN_KEY);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function parseError(response) {
  let detail = "";
  try {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      const body = await response.json();
      detail = body.error || body.message || body.detail || "";
    } else {
      detail = (await response.text()).trim();
    }
  } catch {}
  const prefix = getCopy().backendErrorPrefix;
  const suffix = detail ? `: ${detail}` : "";
  throw new Error(`${prefix} (${response.status})${suffix}`);
}

async function fetchJson(path, init = {}) {
  const response = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers ?? {}) },
  });
  if (response.status === 401) {
    throw new Error("401");
  }
  if (!response.ok) {
    await parseError(response);
  }
  return response.status === 204 ? null : response.json();
}

async function fetchDashboard() {
  return fetchJson("/api/dashboard");
}

async function putJson(path, payload) {
  return fetchJson(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

// State Selectors and Classification
function currentPolicies() {
  return structuredClone(state.dashboard?.policies ?? {});
}

function currentReporting() {
  return structuredClone(state.reporting?.settings ?? state.dashboard?.reporting ?? {});
}

function statusChipClass(value) {
  if (value === true || value === "present" || value === "active" || value === "enabled") {
    return "status-chip ok";
  }
  if (value === false || value === "missing" || value === "disabled" || value === "misconfigured") {
    return "status-chip danger";
  }
  if (value === "unreadable" || value === "invalid-path" || value === "degraded") {
    return "status-chip warn";
  }
  return "status-chip muted";
}

function labelForAddressRole(role) {
  return role === "sender" ? getCopy().policyRoleSender : getCopy().policyRoleRecipient;
}

function labelForAction(action) {
  return action === "allow" ? getCopy().policyActionAllow : getCopy().policyActionBlock;
}

function labelForAttachmentScope(scope) {
  const copy = getCopy();
  if (scope === "extension") return copy.attachmentScopeExtension;
  if (scope === "mime") return copy.attachmentScopeMime;
  return copy.attachmentScopeDetected;
}

function labelForVerificationBackend(backend) {
  return backend === "private-postgres" ? getCopy().cacheBackendPostgres : getCopy().cacheBackendMemory;
}

function labelForKeyStatus(status) {
  const copy = getCopy();
  switch (status) {
    case "present":
      return copy.dkimKeyStatusPresent;
    case "missing":
      return copy.dkimKeyStatusMissing;
    case "unreadable":
      return copy.dkimKeyStatusUnreadable;
    case "invalid-path":
      return copy.dkimKeyStatusInvalid;
    default:
      return copy.dkimKeyStatusNotConfigured;
  }
}

function buildEmptyState(title, description, actionHtml = "") {
  return `
    <article class="empty-state">
      <div>
        <strong>${escapeHtml(title)}</strong>
        <p>${escapeHtml(description)}</p>
      </div>
      ${actionHtml ? `<div class="empty-state-actions">${actionHtml}</div>` : ""}
    </article>
  `;
}

function buildLoadingRows(count = 2) {
  return Array.from({ length: count })
    .map(
      () => `
        <article class="loading-row" aria-hidden="true">
          <div class="loading-line short"></div>
          <div class="loading-line long"></div>
          <div class="loading-line medium"></div>
        </article>
      `,
    )
    .join("");
}

function setListLoading(container, count = 2) {
  container.innerHTML = buildLoadingRows(count);
}

function clearInvalidFields(form) {
  form.querySelectorAll(".invalid").forEach((field) => field.classList.remove("invalid"));
}

function markInvalid(form, names) {
  names.forEach((name) => {
    const field = form.elements.namedItem(name);
    if (field) {
      field.classList.add("invalid");
      field.setAttribute("aria-invalid", "true");
    }
  });
}

function renderDrawerContent(title, summary, content, opener = document.activeElement, onClose = null) {
  state.drawer.previousFocus = opener instanceof HTMLElement ? opener : null;
  state.drawer.onClose = onClose;
  state.drawer.open = true;
  elements.drawerTitle.textContent = title;
  elements.drawerSummary.textContent = summary;
  elements.drawerContent.innerHTML = content;
  elements.drawerBackdrop.classList.remove("hidden");
  document.body.classList.add("drawer-open");
  requestAnimationFrame(() => {
    const focusable = elements.drawer.querySelector("input, select, textarea, button, [href], [tabindex]:not([tabindex='-1'])");
    (focusable || elements.drawer).focus();
  });
}

function closeDrawer() {
  if (!state.drawer.open) {
    return;
  }
  state.drawer.open = false;
  elements.drawerBackdrop.classList.add("hidden");
  document.body.classList.remove("drawer-open");
  if (typeof state.drawer.onClose === "function") {
    state.drawer.onClose();
  }
  if (state.drawer.previousFocus instanceof HTMLElement) {
    state.drawer.previousFocus.focus();
  }
  state.drawer.onClose = null;
}

function renderMetric(element, value) {
  element.textContent = formatMetric(value);
}

function setAuthenticated(authenticated) {
  elements.consoleShell.classList.toggle("hidden", !authenticated);
  elements.loginShell.classList.toggle("hidden", authenticated);
}

function syncLoadingState() {
  const copy = getCopy();
  if (!state.dashboard) {
    elements.nodeName.textContent = copy.heroLoadingTitle;
    elements.heroSummary.textContent = copy.heroLoadingSummary;
    elements.sidebarNodeName.textContent = copy.heroLoadingTitle;
    elements.sidebarNodeCopy.textContent = copy.heroLoadingSummary;
    elements.contextOperator.textContent = copy.unset;
    elements.contextRole.textContent = copy.operatorRole;
    elements.contextVersion.textContent = copy.unset;
    elements.contextLicense.textContent = "Apache-2.0";
    elements.contextBuild.textContent = copy.unset;
    elements.contextTime.textContent = formatDateTime();
    elements.heroPrimaryRelay.textContent = copy.unset;
    elements.heroRouteSummary.textContent = copy.unset;
    elements.heroReportingSummary.textContent = copy.unset;
    elements.heroReportingCopy.textContent = copy.unset;
    elements.metricSystemHealth.textContent = "-";
    renderOverview();
    Object.values(containers).forEach((container) => setListLoading(container));
    return;
  }
  renderDashboard();
}

function updateNavState(activeSectionId = state.activeSection) {
  state.activeSection = activeSectionId;
  elements.navButtons.forEach((button) => {
    const isActive = button.dataset.scrollTarget === activeSectionId;
    button.setAttribute("aria-current", isActive ? "true" : "false");
  });
}

function registerSectionObserver() {
  const sections = elements.navButtons
    .map((button) => document.getElementById(button.dataset.scrollTarget))
    .filter(Boolean);
  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => right.intersectionRatio - left.intersectionRatio)[0];
      if (visible?.target?.id) {
        updateNavState(visible.target.id);
      }
    },
    { rootMargin: "-20% 0px -60% 0px", threshold: [0.15, 0.4, 0.7] },
  );
  sections.forEach((section) => observer.observe(section));
}

function routeToPolicies(role, action) {
  if (role === "sender" && action === "allow") return "allow_senders";
  if (role === "sender" && action === "block") return "block_senders";
  if (role === "recipient" && action === "allow") return "allow_recipients";
  return "block_recipients";
}

function routeToAttachmentPolicies(scope, action) {
  if (scope === "extension" && action === "allow") return "allow_extensions";
  if (scope === "extension" && action === "block") return "block_extensions";
  if (scope === "mime" && action === "allow") return "allow_mime_types";
  if (scope === "mime" && action === "block") return "block_mime_types";
  if (scope === "detected" && action === "allow") return "allow_detected_types";
  return "block_detected_types";
}

function getAddressRules(policies = state.dashboard?.policies) {
  if (!policies?.address_policy) {
    return [];
  }
  const rules = [];
  policies.address_policy.allow_senders.forEach((value, index) => {
    rules.push({ id: `allow-sender-${index}`, role: "sender", action: "allow", value, index });
  });
  policies.address_policy.block_senders.forEach((value, index) => {
    rules.push({ id: `block-sender-${index}`, role: "sender", action: "block", value, index });
  });
  policies.address_policy.allow_recipients.forEach((value, index) => {
    rules.push({ id: `allow-recipient-${index}`, role: "recipient", action: "allow", value, index });
  });
  policies.address_policy.block_recipients.forEach((value, index) => {
    rules.push({ id: `block-recipient-${index}`, role: "recipient", action: "block", value, index });
  });
  rules.sort((left, right) => left.value.localeCompare(right.value));
  return rules;
}

function getAttachmentRules(policies = state.dashboard?.policies) {
  if (!policies?.attachment_policy) {
    return [];
  }
  const rules = [];
  policies.attachment_policy.allow_extensions.forEach((value, index) => {
    rules.push({ id: `allow-extension-${index}`, scope: "extension", action: "allow", value, index });
  });
  policies.attachment_policy.block_extensions.forEach((value, index) => {
    rules.push({ id: `block-extension-${index}`, scope: "extension", action: "block", value, index });
  });
  policies.attachment_policy.allow_mime_types.forEach((value, index) => {
    rules.push({ id: `allow-mime-${index}`, scope: "mime", action: "allow", value, index });
  });
  policies.attachment_policy.block_mime_types.forEach((value, index) => {
    rules.push({ id: `block-mime-${index}`, scope: "mime", action: "block", value, index });
  });
  policies.attachment_policy.allow_detected_types.forEach((value, index) => {
    rules.push({ id: `allow-detected-${index}`, scope: "detected", action: "allow", value, index });
  });
  policies.attachment_policy.block_detected_types.forEach((value, index) => {
    rules.push({ id: `block-detected-${index}`, scope: "detected", action: "block", value, index });
  });
  rules.sort((left, right) => left.value.localeCompare(right.value));
  return rules;
}

function findAddressRule(ruleId) {
  return getAddressRules().find((rule) => rule.id === ruleId) ?? null;
}

function findAttachmentRule(ruleId) {
  return getAttachmentRules().find((rule) => rule.id === ruleId) ?? null;
}

// Renderers
function renderQuarantine() {
  const copy = getCopy();
  const items = state.quarantine;
  if (!items.length) {
    containers.quarantine.innerHTML = buildEmptyState(
      copy.emptyQuarantineTitle,
      copy.noResults,
      `<button class="secondary-button compact-button" type="button" data-action="refresh-quarantine">${escapeHtml(copy.refresh)}</button>`,
    );
    return;
  }

  containers.quarantine.innerHTML = `
    <div class="history-summary">${translate(copy.searchResults, { count: items.length })}</div>
    ${items
      .map(
        (item) => `
          <article class="record-row">
            <div class="record-head">
              <div>
                <h4 class="record-title">${escapeHtml(item.subject || item.trace_id)}</h4>
                <div class="record-meta">${escapeHtml(item.received_at || copy.unset)} · ${escapeHtml(item.mail_from || copy.unset)} -> ${escapeHtml(formatList(item.rcpt_to ?? []))}</div>
              </div>
              <div class="record-tags">
                <span class="badge danger">${escapeHtml(item.status || copy.unset)}</span>
                <span class="pill">${escapeHtml(item.direction || copy.unset)}</span>
              </div>
            </div>
            <div class="record-copy">${escapeHtml(item.reason || item.internet_message_id || copy.unset)}</div>
            <div class="record-grid">
              <div class="summary-card"><p>${copy.traceLabel}</p><strong>${escapeHtml(item.trace_id)}</strong></div>
              <div class="summary-card"><p>${copy.spamLabel}</p><strong>${escapeHtml(formatScore(item.spam_score))}</strong></div>
              <div class="summary-card"><p>${copy.securityLabel}</p><strong>${escapeHtml(formatScore(item.security_score))}</strong></div>
            </div>
            <div class="record-actions">
              <button class="list-action" type="button" data-action="trace-open" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceOpen}</button>
              <button class="list-action" type="button" data-action="trace-release" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceRelease}</button>
              <button class="list-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceDelete}</button>
            </div>
          </article>
        `,
      )
      .join("")}
  `;
}

function renderHistory() {
  const copy = getCopy();
  const items = state.history;
  if (!items.length) {
    containers.history.innerHTML = buildEmptyState(
      copy.emptyHistoryTitle,
      copy.noResults,
      `<button class="secondary-button compact-button" type="button" data-action="refresh-history">${escapeHtml(copy.refresh)}</button>`,
    );
    return;
  }

  containers.history.innerHTML = `
    <div class="history-summary">${translate(copy.historyResultSummary, { count: items.length })}</div>
    ${items
      .map(
        (item) => `
          <article class="record-row">
            <div class="record-head">
              <div>
                <h4 class="record-title">${escapeHtml(item.subject || item.trace_id)}</h4>
                <div class="record-meta">${escapeHtml(item.latest_event_at || copy.unset)} · ${escapeHtml(item.mail_from || copy.unset)} -> ${escapeHtml(formatList(item.rcpt_to ?? []))}</div>
              </div>
              <div class="record-tags">
                <span class="badge">${escapeHtml(item.queue || copy.unset)}</span>
                <span class="pill">${escapeHtml(item.status || copy.unset)}</span>
                <span class="pill">${escapeHtml(item.direction || copy.unset)}</span>
              </div>
            </div>
            <div class="record-copy">${escapeHtml(item.reason || item.route_target || item.internet_message_id || copy.unset)}</div>
            <div class="record-grid">
              <div class="summary-card"><p>${copy.traceLabel}</p><strong>${escapeHtml(item.trace_id)}</strong></div>
              <div class="summary-card"><p>${copy.eventCountLabel}</p><strong>${escapeHtml(formatNumber(item.event_count))}</strong></div>
              <div class="summary-card"><p>${copy.routeLabel}</p><strong>${escapeHtml(item.route_target || copy.unset)}</strong></div>
            </div>
            <div class="record-tags">
              ${(item.policy_tags ?? []).slice(0, 4).map((tag) => `<span class="pill">${escapeHtml(tag)}</span>`).join("")}
            </div>
            <div class="record-actions">
              <button class="list-action" type="button" data-action="trace-open" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceOpen}</button>
            </div>
          </article>
        `,
      )
      .join("")}
  `;
}

function renderAddressRules() {
  const copy = getCopy();
  const rules = getAddressRules();
  if (!rules.length) {
    containers.addressRules.innerHTML = buildEmptyState(
      copy.emptyAddressRulesTitle,
      copy.noAddressRules,
      `<button class="primary-button compact-button" type="button" data-action="address-create">${escapeHtml(copy.emptyActionCreateRule)}</button>`,
    );
    return;
  }

  containers.addressRules.innerHTML = rules
    .map(
      (rule) => `
        <article class="record-row">
          <div class="record-head">
            <div>
              <h4 class="record-title">${escapeHtml(rule.value)}</h4>
              <div class="record-meta">${escapeHtml(labelForAddressRole(rule.role))} · ${escapeHtml(labelForAction(rule.action))}</div>
            </div>
            <div class="record-tags">
              <span class="pill">${escapeHtml(labelForAddressRole(rule.role))}</span>
              <span class="pill">${escapeHtml(labelForAction(rule.action))}</span>
            </div>
          </div>
          <div class="record-actions">
            <button class="list-action" type="button" data-action="address-edit" data-rule-id="${escapeHtml(rule.id)}">${copy.edit}</button>
            <button class="list-action" type="button" data-action="address-delete" data-rule-id="${escapeHtml(rule.id)}">${copy.remove}</button>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderAttachmentRules() {
  const copy = getCopy();
  const rules = getAttachmentRules();
  if (!rules.length) {
    containers.attachmentRules.innerHTML = buildEmptyState(
      copy.emptyAttachmentRulesTitle,
      copy.noAttachmentRules,
      `<button class="primary-button compact-button" type="button" data-action="attachment-create">${escapeHtml(copy.emptyActionCreateRule)}</button>`,
    );
    return;
  }

  containers.attachmentRules.innerHTML = rules
    .map(
      (rule) => `
        <article class="record-row">
          <div class="record-head">
            <div>
              <h4 class="record-title">${escapeHtml(rule.value)}</h4>
              <div class="record-meta">${escapeHtml(labelForAttachmentScope(rule.scope))} · ${escapeHtml(labelForAction(rule.action))}</div>
            </div>
            <div class="record-tags">
              <span class="pill">${escapeHtml(labelForAttachmentScope(rule.scope))}</span>
              <span class="pill">${escapeHtml(labelForAction(rule.action))}</span>
            </div>
          </div>
          <div class="record-actions">
            <button class="list-action" type="button" data-action="attachment-edit" data-rule-id="${escapeHtml(rule.id)}">${copy.edit}</button>
            <button class="list-action" type="button" data-action="attachment-delete" data-rule-id="${escapeHtml(rule.id)}">${copy.remove}</button>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderRecipientVerification() {
  const copy = getCopy();
  const status = state.policyStatus?.recipient_verification;
  if (!status) {
    containers.recipientVerification.innerHTML = buildEmptyState(copy.verificationTitle, copy.noResults);
    return;
  }

  containers.recipientVerification.innerHTML = `
    <article class="summary-card">
      <strong>${copy.verificationCardTitle}</strong>
      <div class="summary-grid">
        <div>
          <p>${copy.verificationState}</p>
          <span class="${statusChipClass(status.operational_state)}">${escapeHtml(status.operational_state || copy.unset)}</span>
        </div>
        <div>
          <p>${copy.verificationCacheBackend}</p>
          <span class="pill">${escapeHtml(labelForVerificationBackend(status.cache_backend))}</span>
        </div>
        <div>
          <p>${copy.verificationModeFailClosed}</p>
          <span class="pill">${escapeHtml(status.fail_closed ? copy.enabled : copy.disabled)}</span>
        </div>
        <div>
          <p>${copy.verificationCacheTtl}</p>
          <span class="pill">${escapeHtml(formatNumber(status.cache_ttl_seconds))}s</span>
        </div>
      </div>
    </article>
  `;
}

function renderDkim() {
  const copy = getCopy();
  const status = state.policyStatus?.dkim;
  const domains = status?.domains ?? [];
  const profile = `
    <article class="summary-card">
      <strong>${copy.dkimProfileTitle}</strong>
      <div class="summary-grid">
        <div>
          <p>${copy.dkimSigningEnabledLabel}</p>
          <span class="${statusChipClass(status?.enabled ? "enabled" : "disabled")}">${escapeHtml(status?.enabled ? copy.enabled : copy.disabled)}</span>
        </div>
        <div>
          <p>${copy.dkimOverSignLabel}</p>
          <span class="pill">${escapeHtml(status?.over_sign ? copy.enabled : copy.disabled)}</span>
        </div>
        <div class="field-span-full">
          <p>${copy.dkimHeaders}</p>
          <span class="record-copy">${escapeHtml(formatList(status?.headers ?? []))}</span>
        </div>
        <div>
          <p>${copy.dkimExpiration}</p>
          <span class="pill">${escapeHtml(status?.expiration_seconds ?? copy.unset)}</span>
        </div>
      </div>
    </article>
  `;
  const rows = domains.length
    ? domains
        .map(
          (domain, index) => `
            <article class="record-row">
              <div class="record-head">
                <div>
                  <h4 class="record-title">${escapeHtml(domain.domain)}</h4>
                  <div class="record-meta">${escapeHtml(domain.selector)} · ${escapeHtml(domain.private_key_path || copy.unset)}</div>
                </div>
                <div class="record-tags">
                  <span class="${statusChipClass(domain.key_status)}">${escapeHtml(labelForKeyStatus(domain.key_status))}</span>
                  <span class="pill">${escapeHtml(domain.enabled ? copy.enabled : copy.disabled)}</span>
                </div>
              </div>
              <div class="record-actions">
                <button class="list-action" type="button" data-action="dkim-domain-edit" data-index="${index}">${copy.edit}</button>
                <button class="list-action" type="button" data-action="dkim-domain-delete" data-index="${index}">${copy.remove}</button>
              </div>
            </article>
          `,
        )
        .join("")
    : buildEmptyState(
        copy.noDkimDomains,
        copy.dkimSummary,
        `<button class="primary-button compact-button" type="button" data-action="dkim-domain-create">${escapeHtml(copy.emptyActionCreateDomain)}</button>`,
      );
  containers.dkimDomains.innerHTML = profile + rows;
}

function renderDigestDefaults(reporting) {
  const copy = getCopy();
  if (!reporting.domain_defaults.length) {
    return buildEmptyState(
      copy.emptyDigestDefaultsTitle,
      copy.noDigestDefaults,
      `<button class="secondary-button compact-button" type="button" data-action="digest-default-create">${escapeHtml(copy.emptyActionCreateDomain)}</button>`,
    );
  }

  return `
    <article class="summary-card"><strong>${copy.digestDefaultsTitle}</strong></article>
    ${reporting.domain_defaults
      .map(
        (item, index) => `
          <article class="record-row">
            <div class="record-head">
              <div>
                <h4 class="record-title">${escapeHtml(item.domain)}</h4>
                <div class="record-meta">${escapeHtml(formatList(item.recipients))}</div>
              </div>
            </div>
            <div class="record-actions">
              <button class="list-action" type="button" data-action="digest-default-edit" data-index="${index}">${copy.edit}</button>
              <button class="list-action" type="button" data-action="digest-default-delete" data-index="${index}">${copy.remove}</button>
            </div>
          </article>
        `,
      )
      .join("")}
  `;
}

function renderDigestOverrides(reporting) {
  const copy = getCopy();
  if (!reporting.user_overrides.length) {
    return buildEmptyState(
      copy.emptyDigestOverridesTitle,
      copy.noDigestOverrides,
      `<button class="primary-button compact-button" type="button" data-action="digest-override-create">${escapeHtml(copy.emptyActionCreateOverride)}</button>`,
    );
  }

  return `
    <article class="summary-card"><strong>${copy.digestOverridesTitle}</strong></article>
    ${reporting.user_overrides
      .map(
        (item, index) => `
          <article class="record-row">
            <div class="record-head">
              <div>
                <h4 class="record-title">${escapeHtml(item.mailbox)}</h4>
                <div class="record-meta">${escapeHtml(item.recipient)}</div>
              </div>
              <div class="record-tags">
                <span class="pill">${escapeHtml(item.enabled ? copy.enabled : copy.disabled)}</span>
              </div>
            </div>
            <div class="record-actions">
              <button class="list-action" type="button" data-action="digest-override-edit" data-index="${index}">${copy.edit}</button>
              <button class="list-action" type="button" data-action="digest-override-delete" data-index="${index}">${copy.remove}</button>
            </div>
          </article>
        `,
      )
      .join("")}
  `;
}

function renderDigestReportsList(reports) {
  const copy = getCopy();
  if (!reports.length) {
    return buildEmptyState(copy.emptyDigestReportsTitle, copy.noDigestReports);
  }

  return `
    <article class="summary-card"><strong>${copy.digestReportsTitle}</strong></article>
    ${reports
      .map(
        (report) => `
          <article class="record-row">
            <div class="record-head">
              <div>
                <h4 class="record-title">${escapeHtml(report.scope_label)}</h4>
                <div class="record-meta">${escapeHtml(report.generated_at)} · ${escapeHtml(report.recipient)}</div>
              </div>
            </div>
            <div class="record-grid">
              <div class="summary-card"><p>${copy.countLabel}</p><strong>${escapeHtml(formatNumber(report.item_count))}</strong></div>
              <div class="summary-card"><p>${copy.topReasonLabel}</p><strong>${escapeHtml(report.top_reason || copy.unset)}</strong></div>
              <div class="summary-card"><p>${copy.generatedAtLabel}</p><strong>${escapeHtml(report.generated_at)}</strong></div>
            </div>
            <div class="record-actions">
              <button class="list-action" type="button" data-action="digest-open" data-report-id="${escapeHtml(report.report_id)}">${copy.digestOpen}</button>
            </div>
          </article>
        `,
      )
      .join("")}
  `;
}

function renderDigestReporting() {
  const copy = getCopy();
  const reporting = state.reporting?.settings;
  const reports = state.digestReports;
  if (!reporting) {
    containers.digestSettings.innerHTML = buildEmptyState(copy.digestTitle, copy.noResults);
    containers.digestDefaults.innerHTML = buildEmptyState(copy.digestDefaultsTitle, copy.noResults);
    containers.digestOverrides.innerHTML = buildEmptyState(copy.digestOverridesTitle, copy.noResults);
    containers.digestReports.innerHTML = buildEmptyState(copy.digestReportsTitle, copy.noResults);
    return;
  }

  containers.digestSettings.innerHTML = `
    <article class="summary-card">
      <strong>${copy.digestSettingsTitle}</strong>
      <div class="summary-grid">
        <div>
          <p>${copy.digestEnabledLabel}</p>
          <span class="${statusChipClass(reporting.digest_enabled ? "enabled" : "disabled")}">${escapeHtml(reporting.digest_enabled ? copy.enabled : copy.disabled)}</span>
        </div>
        <div>
          <p>${copy.digestIntervalLabel}</p>
          <span class="pill">${escapeHtml(formatNumber(reporting.digest_interval_minutes))} min</span>
        </div>
        <div>
          <p>${copy.digestMaxItemsLabel}</p>
          <span class="pill">${escapeHtml(formatNumber(reporting.digest_max_items))}</span>
        </div>
        <div>
          <p>${copy.digestRetentionLabel}</p>
          <span class="pill">${escapeHtml(formatNumber(reporting.history_retention_days))} d</span>
        </div>
        <div>
          <p>${copy.digestLastRun}</p>
          <span class="record-copy">${escapeHtml(reporting.last_digest_run_at || copy.unset)}</span>
        </div>
        <div>
          <p>${copy.digestNextRun}</p>
          <span class="record-copy">${escapeHtml(reporting.next_digest_run_at || copy.unset)}</span>
        </div>
      </div>
      <p class="record-copy">${escapeHtml(copy.reportingSettingsNote)}</p>
    </article>
  `;

  containers.digestDefaults.innerHTML = renderDigestDefaults(reporting);
  containers.digestOverrides.innerHTML = renderDigestOverrides(reporting);
  containers.digestReports.innerHTML = renderDigestReportsList(reports);
}

function renderPlatform() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    containers.platform.innerHTML = buildLoadingRows(2);
    return;
  }
  containers.platform.innerHTML = `
    <article class="record-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${copy.platformNode}</h4>
          <div class="record-copy">${copy.platformNodeCopy}</div>
        </div>
      </div>
      <div class="record-grid">
        <div class="summary-card"><p>${copy.siteNodeNameLabel}</p><strong>${escapeHtml(dashboard.site.node_name || copy.unset)}</strong></div>
        <div class="summary-card"><p>${copy.siteDmzZoneLabel}</p><strong>${escapeHtml(dashboard.site.dmz_zone || copy.unset)}</strong></div>
        <div class="summary-card"><p>${copy.sitePublishedMxLabel}</p><strong>${escapeHtml(dashboard.site.published_mx || copy.unset)}</strong></div>
      </div>
      <div class="record-actions">
        <button class="list-action" type="button" data-action="platform-edit" data-target="site">${copy.edit}</button>
      </div>
    </article>
    <article class="record-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${copy.platformRelay}</h4>
          <div class="record-copy">${copy.platformRelayCopy}</div>
        </div>
      </div>
      <div class="record-grid">
        <div class="summary-card"><p>${copy.relayPrimaryLabel}</p><strong>${escapeHtml(dashboard.relay.primary_upstream || copy.unset)}</strong></div>
        <div class="summary-card"><p>${copy.relaySecondaryLabel}</p><strong>${escapeHtml(dashboard.relay.secondary_upstream || copy.unset)}</strong></div>
        <div class="summary-card"><p>${copy.relaySyncLabel}</p><strong>${escapeHtml(formatNumber(dashboard.relay.sync_interval_seconds))}</strong></div>
      </div>
      <div class="record-actions">
        <button class="list-action" type="button" data-action="platform-edit" data-target="relay">${copy.edit}</button>
      </div>
    </article>
    <article class="record-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${copy.platformNetwork}</h4>
          <div class="record-copy">${copy.platformNetworkCopy}</div>
        </div>
      </div>
      <div class="record-copy">${escapeHtml(formatList(dashboard.network.allowed_management_cidrs))}</div>
      <div class="record-actions">
        <button class="list-action" type="button" data-action="platform-edit" data-target="network">${copy.edit}</button>
      </div>
    </article>
    <article class="record-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${copy.platformUpdates}</h4>
          <div class="record-copy">${copy.platformUpdatesCopy}</div>
        </div>
      </div>
      <div class="record-copy">${escapeHtml(`${dashboard.updates.channel || copy.unset} · ${dashboard.updates.maintenance_window || copy.unset}`)}</div>
      <div class="record-actions">
        <button class="list-action" type="button" data-action="platform-edit" data-target="updates">${copy.edit}</button>
      </div>
    </article>
  `;
}

function renderAudit() {
  const copy = getCopy();
  const entries = state.dashboard?.audit ?? [];
  if (!entries.length) {
    containers.audit.innerHTML = buildEmptyState(copy.auditTitle, copy.noAuditEntries);
    return;
  }
  containers.audit.innerHTML = entries
    .map(
      (entry) => `
        <article class="audit-entry">
          <strong>${escapeHtml(entry.action)}</strong>
          <span>${escapeHtml(entry.actor)}</span>
          <span>${escapeHtml(entry.timestamp)}</span>
          <p>${escapeHtml(entry.details)}</p>
        </article>
      `,
    )
    .join("");
}

function buildMiniStat(label, value, detail = "") {
  return `
    <article class="mini-stat">
      <div>
        <span>${escapeHtml(label)}</span>
        ${detail ? `<small>${escapeHtml(detail)}</small>` : ""}
      </div>
      <strong>${escapeHtml(value)}</strong>
    </article>
  `;
}

function buildStatusTile(title, stateLabel, tone = "muted", detail = "") {
  return `
    <article class="status-tile">
      <p>${escapeHtml(title)}</p>
      <span class="${statusChipClass(tone === "custom" ? stateLabel : tone)}">${escapeHtml(stateLabel)}</span>
      ${detail ? `<small>${escapeHtml(detail)}</small>` : ""}
    </article>
  `;
}

function buildRankedRows(items) {
  const copy = getCopy();
  if (!items.length) {
    return `<article class="ranked-row"><div class="ranked-index">-</div><div><strong>${escapeHtml(copy.listNoData)}</strong></div><span class="pill muted">${escapeHtml(copy.noOverviewData)}</span></article>`;
  }
  return items
    .map(
      (item, index) => `
        <article class="ranked-row">
          <div class="ranked-index">${index + 1}</div>
          <div>
            <strong>${escapeHtml(item.label)}</strong>
            ${item.detail ? `<p>${escapeHtml(item.detail)}</p>` : ""}
          </div>
          <span class="pill">${escapeHtml(formatNumber(item.count))}</span>
        </article>
      `,
    )
    .join("");
}

function countRankedItems(items, resolveLabel, predicate = () => true, limit = 5) {
  const counts = new Map();
  items.forEach((item) => {
    if (!predicate(item)) {
      return;
    }
    const label = String(resolveLabel(item) ?? "").trim();
    if (!label || label === getCopy().unset) {
      return;
    }
    counts.set(label, (counts.get(label) ?? 0) + 1);
  });
  return [...counts.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, limit)
    .map(([label, count]) => ({ label, count }));
}

function itemIsSecurityFlagged(item) {
  const reason = String(item?.reason ?? item?.current?.reason ?? "").toLowerCase();
  return Number(item?.security_score ?? item?.current?.security_score ?? 0) > 0 || /(virus|malware|payload|phish|suspicious|infect)/.test(reason);
}

function extractThreatLabel(item) {
  const reason = String(item?.reason ?? item?.current?.reason ?? "").trim();
  if (reason) {
    return reason;
  }
  const tag = (item?.policy_tags ?? item?.current?.policy_tags ?? []).find(Boolean);
  return tag || "";
}

function buildTrafficSeries(records) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const days = Array.from({ length: 7 }, (_, index) => {
    const date = new Date(today);
    date.setDate(today.getDate() - (6 - index));
    return {
      key: date.toISOString().slice(0, 10),
      label: formatShortDate(date),
      total: 0,
      quarantine: 0,
      security: 0,
    };
  });
  const dayMap = new Map(days.map((day) => [day.key, day]));
  records.forEach((item) => {
    const rawDate = item.latest_event_at || item.received_at || item.generated_at || item.timestamp;
    if (!rawDate) {
      return;
    }
    const date = new Date(rawDate);
    if (Number.isNaN(date.getTime())) {
      return;
    }
    const key = date.toISOString().slice(0, 10);
    const bucket = dayMap.get(key);
    if (!bucket) {
      return;
    }
    bucket.total += 1;
    if (item.queue === "quarantine" || item.status === "quarantined") {
      bucket.quarantine += 1;
    }
    if (itemIsSecurityFlagged(item)) {
      bucket.security += 1;
    }
  });
  return days;
}

function renderOverview() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    elements.queueStatusList.innerHTML = buildLoadingRows(2);
    elements.scannerStatusList.innerHTML = buildLoadingRows(2);
    elements.relayHealthList.innerHTML = buildLoadingRows(2);
    elements.topSpamRelaysList.innerHTML = buildLoadingRows(1);
    elements.topVirusRelaysList.innerHTML = buildLoadingRows(1);
    elements.topVirusesList.innerHTML = buildLoadingRows(1);
    elements.scanSummaryList.innerHTML = buildLoadingRows(2);
    elements.trafficChart.innerHTML = "";
    elements.trafficTable.innerHTML = `<article class="traffic-row"><strong>${escapeHtml(copy.noTrafficHistory)}</strong></article>`;
    return;
  }

  const operatorEmail = getOperatorEmail();
  const verification = state.policyStatus?.recipient_verification;
  const dkim = state.policyStatus?.dkim;
  const reporting = getDigestSettings();
  const routeRules = state.routeDiagnostics?.routing?.rules ?? dashboard.routing?.rules ?? [];
  const trafficRecords = getTrafficRecords();
  const posture = healthPosture(dashboard);
  const site = dashboard.site ?? {};
  const relay = dashboard.relay ?? {};
  const queues = dashboard.queues ?? {};
  const topSpamRelays = countRankedItems(trafficRecords, getRelayOrPeer, (item) => Number(item.spam_score ?? item.current?.spam_score ?? 0) > 0, 5);
  const topVirusRelays = countRankedItems(trafficRecords, getRelayOrPeer, itemIsSecurityFlagged, 5);
  const topViruses = countRankedItems(
    trafficRecords,
    (item) => extractThreatLabel(item),
    (item) => itemIsSecurityFlagged(item),
    5,
  );
  const trafficSeries = buildTrafficSeries(trafficRecords);
  const trafficMax = Math.max(...trafficSeries.map((entry) => entry.total), 1);

  elements.sidebarNodeName.textContent = site.node_name || copy.heroLoadingTitle;
  elements.sidebarNodeCopy.textContent = translate(copy.heroSummaryTemplate, {
    dmzZone: site.dmz_zone || copy.unset,
    publishedMx: site.published_mx || copy.unset,
    primaryUpstream: relay.primary_upstream || copy.unset,
  });
  elements.operatorEmail.textContent = operatorEmail;
  elements.operatorRole.textContent = copy.operatorRole;
  elements.contextOperator.textContent = operatorEmail;
  elements.contextRole.textContent = copy.operatorRole;
  elements.contextVersion.textContent = dashboard.updates?.last_applied_release || dashboard.updates?.channel || copy.unset;
  elements.contextLicense.textContent = "Apache-2.0";
  elements.contextBuild.textContent = dashboard.updates?.update_source || copy.unset;
  elements.contextTime.textContent = formatDateTime();
  elements.heroPrimaryRelay.textContent = relay.primary_upstream || copy.unset;
  elements.heroRouteSummary.textContent = `${formatNumber(routeRules.length)} ${copy.metricRoutingRules.toLowerCase()}`;
  elements.heroReportingSummary.textContent = reporting
    ? `${formatBooleanLabel(reporting.digest_enabled)} · ${formatNumber(reporting.digest_interval_minutes)} min`
    : copy.unset;
  elements.heroReportingCopy.textContent = reporting?.next_digest_run_at
    ? `${formatNumber(state.digestReports.length)} · ${formatDateTime(reporting.next_digest_run_at)}`
    : `${formatNumber(state.digestReports.length)} · ${copy.unset}`;

  elements.metricSystemHealth.textContent = posture.label;
  renderMetric(elements.metricInbound, queues.inbound_messages);
  renderMetric(elements.metricDeferred, queues.deferred_messages);
  renderMetric(elements.metricQuarantine, queues.quarantined_messages);
  renderMetric(elements.metricAttempts, queues.delivery_attempts_last_hour);
  renderMetric(elements.metricHeld, queues.held_messages);
  renderMetric(elements.metricRoutingRules, routeRules.length);
  renderMetric(elements.metricDkimDomains, dkim?.domains?.length ?? dashboard.policies?.dkim?.domains?.length ?? 0);
  elements.metricRecipientVerification.textContent = verification ? verification.operational_state || copy.unset : "-";

  elements.queueStatusList.innerHTML = [
    buildMiniStat(copy.metricInbound, formatMetric(queues.inbound_messages), copy.queueIncoming),
    buildMiniStat(copy.metricDeferred, formatMetric(queues.deferred_messages), copy.queueDeferredLabel),
    buildMiniStat(copy.metricHeld, formatMetric(queues.held_messages), copy.queueHold),
    buildMiniStat(copy.metricQuarantine, formatMetric(queues.quarantined_messages), copy.queueQuarantineLabel),
    buildMiniStat(copy.metricAttempts, formatMetric(queues.delivery_attempts_last_hour), copy.queueAttemptsLabel),
    buildMiniStat(
      copy.queueReachabilityLabel,
      queues.upstream_reachable ? copy.relayReachable : copy.relayUnreachable,
      relay.primary_upstream || copy.unset,
    ),
  ].join("");

  elements.scannerStatusList.innerHTML = [
    buildStatusTile(copy.scannerRelayLink, queues.upstream_reachable ? copy.active : copy.inactive, queues.upstream_reachable ? "active" : "disabled", relay.primary_upstream || copy.unset),
    buildStatusTile(copy.scannerVerification, verification?.operational_state || copy.unset, "custom", verification ? `${formatNumber(verification.cache_ttl_seconds)}s` : copy.unset),
    buildStatusTile(copy.scannerDkimReadiness, dkim?.operational_state || copy.unset, "custom", `${formatNumber(dkim?.domains?.length ?? 0)} ${copy.metricDkimDomains.toLowerCase()}`),
    buildStatusTile(copy.scannerDigestSchedule, reporting?.digest_enabled ? copy.enabled : copy.disabled, reporting?.digest_enabled ? "enabled" : "disabled", reporting ? `${formatNumber(reporting.digest_interval_minutes)} min` : copy.unset),
  ].join("");

  elements.relayHealthList.innerHTML = [
    buildMiniStat(copy.relayHealthNodeRole, site.role || copy.unset, site.region || copy.unset),
    buildMiniStat(copy.relayHealthMx, site.published_mx || copy.unset, site.dmz_zone || copy.unset),
    buildMiniStat(copy.relayHealthPrimary, relay.primary_upstream || copy.unset, queues.upstream_reachable ? copy.relayReachable : copy.relayUnreachable),
    buildMiniStat(copy.relayHealthSecondary, relay.secondary_upstream || copy.unset, relay.ha_enabled ? copy.enabled : copy.disabled),
    buildMiniStat(copy.relayHealthManagement, site.management_fqdn || copy.unset, site.management_bind || copy.unset),
    buildMiniStat(copy.relayHealthSync, formatDurationMinutes(relay.sync_interval_seconds), relay.core_delivery_base_url || copy.unset),
  ].join("");

  elements.topSpamRelaysList.innerHTML = buildRankedRows(
    topSpamRelays.map((entry) => ({ ...entry, detail: copy.topSpamRelaysHeading })),
  );
  elements.topVirusRelaysList.innerHTML = buildRankedRows(
    topVirusRelays.map((entry) => ({ ...entry, detail: copy.topVirusRelaysHeading })),
  );
  elements.topVirusesList.innerHTML = buildRankedRows(
    topViruses.map((entry) => ({ ...entry, detail: copy.topVirusesHeading })),
  );

  elements.scanSummaryList.innerHTML = [
    buildMiniStat(copy.scanSummaryRetained, formatMetric(state.history.length)),
    buildMiniStat(copy.scanSummaryQuarantine, formatMetric(queues.quarantined_messages)),
    buildMiniStat(copy.scanSummarySuspicious, formatMetric(trafficRecords.filter(itemIsSecurityFlagged).length)),
    buildMiniStat(copy.scanSummarySpam, formatMetric(trafficRecords.filter((item) => Number(item.spam_score ?? item.current?.spam_score ?? 0) > 0).length)),
    buildMiniStat(copy.scanSummaryDigest, formatMetric(state.digestReports.length)),
    buildMiniStat(copy.scanSummaryAudit, formatMetric(dashboard.audit?.length ?? 0)),
  ].join("");

  elements.trafficChart.innerHTML = trafficSeries
    .map((entry) => {
      const totalHeight = Math.max(8, Math.round((entry.total / trafficMax) * 164));
      const quarantineHeight = entry.total ? Math.max(8, Math.round((entry.quarantine / trafficMax) * 164)) : 8;
      const securityHeight = entry.total ? Math.max(8, Math.round((entry.security / trafficMax) * 164)) : 8;
      return `
        <article class="traffic-bar">
          <div class="traffic-bar-total">${escapeHtml(formatNumber(entry.total))}</div>
          <div class="traffic-bar-stack">
            <span class="traffic-bar-segment total" style="height:${totalHeight}px"></span>
            <span class="traffic-bar-segment quarantine" style="height:${entry.quarantine ? quarantineHeight : 8}px;opacity:${entry.quarantine ? "1" : "0.22"}"></span>
            <span class="traffic-bar-segment security" style="height:${entry.security ? securityHeight : 8}px;opacity:${entry.security ? "1" : "0.22"}"></span>
          </div>
          <div class="traffic-bar-label">${escapeHtml(entry.label)}</div>
        </article>
      `;
    })
    .join("");
  elements.trafficTable.innerHTML = trafficSeries.some((entry) => entry.total > 0)
    ? trafficSeries
        .slice()
        .reverse()
        .map(
          (entry) => `
            <article class="traffic-row">
              <strong>${escapeHtml(entry.label)}</strong>
              <small>${escapeHtml(`${copy.metricInbound}: ${formatNumber(entry.total)}`)}</small>
              <small>${escapeHtml(`${copy.metricQuarantine}: ${formatNumber(entry.quarantine)}`)}</small>
              <small>${escapeHtml(`${copy.scanSummarySuspicious}: ${formatNumber(entry.security)}`)}</small>
            </article>
          `,
        )
        .join("")
    : `<article class="traffic-row"><strong>${escapeHtml(copy.noTrafficHistory)}</strong></article>`;
}

function renderDashboard() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    syncLoadingState();
    return;
  }
  const posture = healthPosture(dashboard);

  elements.nodeName.textContent = dashboard.site?.node_name || copy.heroLoadingTitle;
  elements.heroSummary.textContent = translate(copy.heroSummaryTemplate, {
    dmzZone: dashboard.site?.dmz_zone || copy.unset,
    publishedMx: dashboard.site?.published_mx || copy.unset,
    primaryUpstream: dashboard.relay?.primary_upstream || copy.unset,
  });
  elements.statusBadge.textContent = posture.label;
  elements.statusBadge.className = `badge ${posture.tone}`;
  elements.upstreamBadge.textContent = dashboard.queues?.upstream_reachable ? copy.relayReachable : copy.relayUnreachable;
  elements.upstreamBadge.className = dashboard.queues?.upstream_reachable ? "badge ok" : "badge danger";

  elements.metricSystemHealth.textContent = posture.label;
  renderMetric(elements.metricInbound, dashboard.queues?.inbound_messages);
  renderMetric(elements.metricDeferred, dashboard.queues?.deferred_messages);
  renderMetric(elements.metricQuarantine, dashboard.queues?.quarantined_messages);
  renderMetric(elements.metricAttempts, dashboard.queues?.delivery_attempts_last_hour);
  renderOverview();

  renderQuarantine();
  renderHistory();
  renderAddressRules();
  renderAttachmentRules();
  renderRecipientVerification();
  renderDkim();
  renderDigestReporting();
  renderPlatform();
  renderAudit();
}

// Persistence, Validation, and Drawer Workflows
async function savePolicies(policies) {
  state.dashboard = await putJson("/api/policies", policies);
  await loadOps({ silent: true });
}

async function saveReporting(settings) {
  const reporting = await putJson("/api/reporting", settings);
  state.reporting = reporting;
  state.digestReports = reporting.recent_reports ?? [];
  state.dashboard.reporting = reporting.settings;
  renderDashboard();
}

function normalizeDomain(value) {
  return String(value ?? "").trim().toLowerCase();
}

function normalizeEmail(value) {
  return String(value ?? "").trim().toLowerCase();
}

function parseLines(value) {
  return String(value ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function dedupeList(values) {
  return Array.from(new Set(values));
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function isValidDomain(value) {
  return /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i.test(value);
}

function isValidAddressRule(value) {
  return isValidEmail(value) || isValidDomain(value);
}

function isValidMimeType(value) {
  return /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(value);
}

function isValidSelector(value) {
  return /^[a-z0-9][a-z0-9._-]{0,62}$/i.test(value);
}

function buildFormError(errors) {
  if (!errors.length) {
    return "";
  }
  return `
    <div class="form-error-list" role="alert">
      <p>${escapeHtml(getCopy().drawerValidationHeading)}</p>
      <ul>${errors.map((error) => `<li>${escapeHtml(error.message)}</li>`).join("")}</ul>
    </div>
  `;
}

function renderDrawerForm({ title, summary, formId, content, onSubmit, opener }) {
  const copy = getCopy();
  renderDrawerContent(
    title,
    summary,
    `
      <form id="${formId}" class="drawer-form" novalidate>
        <div id="${formId}-errors"></div>
        ${content}
      </form>
    `,
    opener,
  );

  const form = document.getElementById(formId);
  const submitButton = form.querySelector('[type="submit"]');
  const errorContainer = document.getElementById(`${formId}-errors`);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    clearInvalidFields(form);
    errorContainer.innerHTML = "";

    try {
      setButtonBusy(submitButton, true, copy.saving, submitButton.textContent);
      await onSubmit(form, {
        fail(errors) {
          errorContainer.innerHTML = buildFormError(errors);
          markInvalid(
            form,
            errors
              .map((error) => error.field)
              .filter(Boolean),
          );
          throw new Error("__validation__");
        },
      });
    } catch (error) {
      if (!(error instanceof Error && error.message === "__validation__")) {
        errorContainer.innerHTML = buildFormError([{ message: error instanceof Error ? error.message : copy.unknownError }]);
      }
    } finally {
      setButtonBusy(submitButton, false, copy.saving, submitButton.textContent);
    }
  });
}

function openAddressRuleDrawer(ruleId = null, opener = document.activeElement) {
  const copy = getCopy();
  const rule = ruleId ? findAddressRule(ruleId) : { role: "sender", action: "allow", value: "" };
  renderDrawerForm({
    title: ruleId ? copy.edit : copy.createRule,
    summary: copy.addressRulesSummary,
    formId: "address-rule-form",
    opener,
    content: `
      <div class="field-grid">
        <label>
          <span>${copy.addressRuleRoleLabel}</span>
          <select name="role">
            <option value="sender"${rule.role === "sender" ? " selected" : ""}>${copy.policyRoleSender}</option>
            <option value="recipient"${rule.role === "recipient" ? " selected" : ""}>${copy.policyRoleRecipient}</option>
          </select>
        </label>
        <label>
          <span>${copy.addressRuleActionLabel}</span>
          <select name="action">
            <option value="allow"${rule.action === "allow" ? " selected" : ""}>${copy.policyActionAllow}</option>
            <option value="block"${rule.action === "block" ? " selected" : ""}>${copy.policyActionBlock}</option>
          </select>
        </label>
        <label class="field-span-full">
          <span>${copy.addressRuleValueLabel}</span>
          <input name="value" required value="${escapeHtml(rule.value)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${ruleId ? copy.save : copy.create}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextRule = {
        role: form.elements.namedItem("role").value,
        action: form.elements.namedItem("action").value,
        value: normalizeDomain(form.elements.namedItem("value").value),
      };
      const errors = [];
      if (!isValidAddressRule(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAddressRule });
      }
      const policies = currentPolicies();
      const existingRules = getAddressRules(policies)
        .filter((item) => item.id !== ruleId)
        .map((item) => `${item.role}:${item.action}:${normalizeDomain(item.value)}`);
      if (existingRules.includes(`${nextRule.role}:${nextRule.action}:${nextRule.value}`)) {
        errors.push({ field: "value", message: copy.validationDuplicateAddressRule });
      }
      if (errors.length) {
        context.fail(errors);
      }
      if (ruleId) {
        const existing = findAddressRule(ruleId);
        policies.address_policy[routeToPolicies(existing.role, existing.action)].splice(existing.index, 1);
      }
      policies.address_policy[routeToPolicies(nextRule.role, nextRule.action)].push(nextRule.value);
      await savePolicies(policies);
      closeDrawer();
      showFeedback(ruleId ? copy.recordSaved : copy.recordCreated);
    },
  });
}

async function deleteAddressRule(ruleId) {
  const copy = getCopy();
  const rule = findAddressRule(ruleId);
  if (!rule) {
    return;
  }
  const policies = currentPolicies();
  policies.address_policy[routeToPolicies(rule.role, rule.action)].splice(rule.index, 1);
  await savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

function openAttachmentRuleDrawer(ruleId = null, opener = document.activeElement) {
  const copy = getCopy();
  const rule = ruleId ? findAttachmentRule(ruleId) : { scope: "extension", action: "block", value: "" };
  renderDrawerForm({
    title: ruleId ? copy.edit : copy.createRule,
    summary: copy.attachmentRulesSummary,
    formId: "attachment-rule-form",
    opener,
    content: `
      <div class="field-grid">
        <label>
          <span>${copy.attachmentRuleScopeLabel}</span>
          <select name="scope">
            <option value="extension"${rule.scope === "extension" ? " selected" : ""}>${copy.attachmentScopeExtension}</option>
            <option value="mime"${rule.scope === "mime" ? " selected" : ""}>${copy.attachmentScopeMime}</option>
            <option value="detected"${rule.scope === "detected" ? " selected" : ""}>${copy.attachmentScopeDetected}</option>
          </select>
        </label>
        <label>
          <span>${copy.attachmentRuleActionLabel}</span>
          <select name="action">
            <option value="allow"${rule.action === "allow" ? " selected" : ""}>${copy.policyActionAllow}</option>
            <option value="block"${rule.action === "block" ? " selected" : ""}>${copy.policyActionBlock}</option>
          </select>
        </label>
        <label class="field-span-full">
          <span>${copy.attachmentRuleValueLabel}</span>
          <input name="value" required value="${escapeHtml(rule.value)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${ruleId ? copy.save : copy.create}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextRule = {
        scope: form.elements.namedItem("scope").value,
        action: form.elements.namedItem("action").value,
        value: String(form.elements.namedItem("value").value).trim().toLowerCase(),
      };
      const errors = [];
      if (!nextRule.value) {
        errors.push({ field: "value", message: copy.validationAttachmentRule });
      } else if (nextRule.scope === "extension" && !/^[.]?[a-z0-9]+$/i.test(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAttachmentExtension });
      } else if (nextRule.scope === "mime" && !isValidMimeType(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAttachmentMime });
      } else if (nextRule.scope === "detected" && !/^[a-z0-9._-]+$/i.test(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationDetectedType });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      if (ruleId) {
        const existing = findAttachmentRule(ruleId);
        policies.attachment_policy[routeToAttachmentPolicies(existing.scope, existing.action)].splice(existing.index, 1);
      }
      policies.attachment_policy[routeToAttachmentPolicies(nextRule.scope, nextRule.action)].push(nextRule.value);
      await savePolicies(policies);
      closeDrawer();
      showFeedback(ruleId ? copy.recordSaved : copy.recordCreated);
    },
  });
}

async function deleteAttachmentRule(ruleId) {
  const copy = getCopy();
  const rule = findAttachmentRule(ruleId);
  if (!rule) {
    return;
  }
  const policies = currentPolicies();
  policies.attachment_policy[routeToAttachmentPolicies(rule.scope, rule.action)].splice(rule.index, 1);
  await savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

function openRecipientVerificationDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().recipient_verification;
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.verificationSummary,
    formId: "recipient-verification-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.recipientVerificationEnabledLabel}</span>
        <input name="enabled" type="checkbox"${settings.enabled ? " checked" : ""} />
      </label>
      <label class="toggle-field">
        <span>${copy.recipientVerificationFailClosedLabel}</span>
        <input name="fail_closed" type="checkbox"${settings.fail_closed ? " checked" : ""} />
      </label>
      <label>
        <span>${copy.recipientVerificationTtlLabel}</span>
        <input name="cache_ttl_seconds" type="number" min="1" value="${escapeHtml(settings.cache_ttl_seconds)}" />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const ttl = Number(form.elements.namedItem("cache_ttl_seconds").value);
      if (!Number.isInteger(ttl) || ttl < 1) {
        context.fail([{ field: "cache_ttl_seconds", message: copy.validationPositiveInteger }]);
      }
      const policies = currentPolicies();
      policies.recipient_verification.enabled = form.elements.namedItem("enabled").checked;
      policies.recipient_verification.fail_closed = form.elements.namedItem("fail_closed").checked;
      policies.recipient_verification.cache_ttl_seconds = ttl;
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

function openDkimSettingsDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  renderDrawerForm({
    title: copy.editSigningProfile,
    summary: copy.dkimSummary,
    formId: "dkim-settings-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.dkimSigningEnabledLabel}</span>
        <input name="enabled" type="checkbox"${settings.enabled ? " checked" : ""} />
      </label>
      <label class="toggle-field">
        <span>${copy.dkimOverSignLabel}</span>
        <input name="over_sign" type="checkbox"${settings.over_sign ? " checked" : ""} />
      </label>
      <label>
        <span>${copy.dkimHeadersLabel}</span>
        <textarea name="headers" rows="4">${escapeHtml((settings.headers ?? []).join("\n"))}</textarea>
      </label>
      <label>
        <span>${copy.dkimExpirationLabel}</span>
        <input name="expiration_seconds" type="number" min="0" value="${escapeHtml(settings.expiration_seconds ?? "")}" />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const expirationValue = String(form.elements.namedItem("expiration_seconds").value).trim();
      const headers = dedupeList(parseLines(form.elements.namedItem("headers").value).map((value) => value.toLowerCase()));
      const errors = [];
      if (form.elements.namedItem("enabled").checked && !headers.length) {
        errors.push({ field: "headers", message: copy.validationHeaders });
      }
      if (expirationValue && (!Number.isInteger(Number(expirationValue)) || Number(expirationValue) < 0)) {
        errors.push({ field: "expiration_seconds", message: copy.validationPositiveInteger });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      policies.dkim.enabled = form.elements.namedItem("enabled").checked;
      policies.dkim.over_sign = form.elements.namedItem("over_sign").checked;
      policies.dkim.headers = headers;
      policies.dkim.expiration_seconds = expirationValue ? Number(expirationValue) : null;
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

function openDkimDomainDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  const domain = index === null ? { domain: "", selector: "", private_key_path: "", enabled: true } : settings.domains[index];
  renderDrawerForm({
    title: index === null ? copy.createDomain : copy.edit,
    summary: copy.dkimSummary,
    formId: "dkim-domain-form",
    opener,
    content: `
      <label>
        <span>${copy.dkimDomainLabel}</span>
        <input name="domain" required value="${escapeHtml(domain.domain)}" />
      </label>
      <label>
        <span>${copy.dkimSelectorLabel}</span>
        <input name="selector" required value="${escapeHtml(domain.selector)}" />
      </label>
      <label>
        <span>${copy.dkimKeyPathLabel}</span>
        <input name="private_key_path" required value="${escapeHtml(domain.private_key_path)}" />
      </label>
      <label class="toggle-field">
        <span>${copy.enabled}</span>
        <input name="enabled" type="checkbox"${domain.enabled ? " checked" : ""} />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextDomain = {
        domain: normalizeDomain(form.elements.namedItem("domain").value),
        selector: String(form.elements.namedItem("selector").value).trim().toLowerCase(),
        private_key_path: String(form.elements.namedItem("private_key_path").value).trim(),
        enabled: form.elements.namedItem("enabled").checked,
      };
      const errors = [];
      if (!isValidDomain(nextDomain.domain)) {
        errors.push({ field: "domain", message: copy.validationDomain });
      }
      if (!isValidSelector(nextDomain.selector)) {
        errors.push({ field: "selector", message: copy.validationSelector });
      }
      if (!nextDomain.private_key_path) {
        errors.push({ field: "private_key_path", message: copy.validationKeyPath });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      if (index === null) {
        policies.dkim.domains.push(nextDomain);
      } else {
        policies.dkim.domains[index] = nextDomain;
      }
      await savePolicies(policies);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

async function deleteDkimDomain(index) {
  const copy = getCopy();
  const policies = currentPolicies();
  policies.dkim.domains.splice(index, 1);
  await savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

function openDigestSettingsDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.digestSummary,
    formId: "digest-settings-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.reportingEnabledLabel}</span>
        <input name="digest_enabled" type="checkbox"${settings.digest_enabled ? " checked" : ""} />
      </label>
      <div class="field-grid">
        <label>
          <span>${copy.digestIntervalLabel}</span>
          <input name="digest_interval_minutes" type="number" min="1" value="${escapeHtml(settings.digest_interval_minutes)}" />
        </label>
        <label>
          <span>${copy.digestMaxItemsLabel}</span>
          <input name="digest_max_items" type="number" min="1" value="${escapeHtml(settings.digest_max_items)}" />
        </label>
        <label>
          <span>${copy.digestRetentionLabel}</span>
          <input name="history_retention_days" type="number" min="1" value="${escapeHtml(settings.history_retention_days)}" />
        </label>
      </div>
      <p class="helper-text">${escapeHtml(copy.reportingSettingsNote)}</p>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const interval = Number(form.elements.namedItem("digest_interval_minutes").value);
      const maxItems = Number(form.elements.namedItem("digest_max_items").value);
      const retention = Number(form.elements.namedItem("history_retention_days").value);
      const errors = [];
      if (!Number.isInteger(interval) || interval < 1) errors.push({ field: "digest_interval_minutes", message: copy.validationPositiveInteger });
      if (!Number.isInteger(maxItems) || maxItems < 1) errors.push({ field: "digest_max_items", message: copy.validationPositiveInteger });
      if (!Number.isInteger(retention) || retention < 1) errors.push({ field: "history_retention_days", message: copy.validationPositiveInteger });
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      settingsToSave.digest_enabled = form.elements.namedItem("digest_enabled").checked;
      settingsToSave.digest_interval_minutes = interval;
      settingsToSave.digest_max_items = maxItems;
      settingsToSave.history_retention_days = retention;
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

function openDigestDefaultDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { domain: "", recipients: [] } : settings.domain_defaults[index];
  renderDrawerForm({
    title: index === null ? copy.createDomainDefault : copy.edit,
    summary: copy.digestDefaultsTitle,
    formId: "digest-default-form",
    opener,
    content: `
      <label>
        <span>${copy.dkimDomainLabel}</span>
        <input name="domain" required value="${escapeHtml(item.domain)}" />
      </label>
      <label>
        <span>${copy.domainDefaultsLabel}</span>
        <textarea name="recipients" rows="4">${escapeHtml((item.recipients ?? []).join("\n"))}</textarea>
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const domain = normalizeDomain(form.elements.namedItem("domain").value);
      const recipients = dedupeList(parseLines(form.elements.namedItem("recipients").value).map(normalizeEmail));
      const errors = [];
      if (!isValidDomain(domain)) {
        errors.push({ field: "domain", message: copy.validationDomain });
      }
      if (!recipients.length) {
        errors.push({ field: "recipients", message: copy.validationRecipients });
      }
      recipients.forEach((recipient) => {
        if (!isValidEmail(recipient)) {
          errors.push({ field: "recipients", message: copy.validationEmail });
        }
      });
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      const nextItem = { domain, recipients };
      if (index === null) {
        settingsToSave.domain_defaults.push(nextItem);
      } else {
        settingsToSave.domain_defaults[index] = nextItem;
      }
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

async function deleteDigestDefault(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.domain_defaults.splice(index, 1);
  await saveReporting(settings);
  showFeedback(copy.recordDeleted);
}

function openDigestOverrideDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { mailbox: "", recipient: "", enabled: true } : settings.user_overrides[index];
  renderDrawerForm({
    title: index === null ? copy.createOverride : copy.edit,
    summary: copy.digestOverridesTitle,
    formId: "digest-override-form",
    opener,
    content: `
      <label>
        <span>${copy.overrideMailboxLabel}</span>
        <input name="mailbox" required value="${escapeHtml(item.mailbox)}" />
      </label>
      <label>
        <span>${copy.overrideRecipientLabel}</span>
        <input name="recipient" required value="${escapeHtml(item.recipient)}" />
      </label>
      <label class="toggle-field">
        <span>${copy.enabled}</span>
        <input name="enabled" type="checkbox"${item.enabled ? " checked" : ""} />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const mailbox = normalizeEmail(form.elements.namedItem("mailbox").value);
      const recipient = normalizeEmail(form.elements.namedItem("recipient").value);
      const errors = [];
      if (!isValidEmail(mailbox)) {
        errors.push({ field: "mailbox", message: copy.validationMailbox });
      }
      if (!isValidEmail(recipient)) {
        errors.push({ field: "recipient", message: copy.validationEmail });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      const nextItem = {
        mailbox,
        recipient,
        enabled: form.elements.namedItem("enabled").checked,
      };
      if (index === null) {
        settingsToSave.user_overrides.push(nextItem);
      } else {
        settingsToSave.user_overrides[index] = nextItem;
      }
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

async function deleteDigestOverride(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.user_overrides.splice(index, 1);
  await saveReporting(settings);
  showFeedback(copy.recordDeleted);
}

function getPlatformDrawerConfigs(dashboard, copy) {
  return {
    site: {
      title: copy.platformNode,
      summary: copy.platformNodeCopy,
      submitPath: "/api/site",
      content: `
        <div class="field-grid">
          <label><span>${copy.siteNodeNameLabel}</span><input name="node_name" required value="${escapeHtml(dashboard.site.node_name)}" /></label>
          <label><span>${copy.siteRoleLabel}</span><input name="role" required value="${escapeHtml(dashboard.site.role)}" /></label>
          <label><span>${copy.siteRegionLabel}</span><input name="region" value="${escapeHtml(dashboard.site.region)}" /></label>
          <label><span>${copy.siteDmzZoneLabel}</span><input name="dmz_zone" value="${escapeHtml(dashboard.site.dmz_zone)}" /></label>
          <label><span>${copy.sitePublishedMxLabel}</span><input name="published_mx" required value="${escapeHtml(dashboard.site.published_mx)}" /></label>
          <label><span>${copy.siteManagementFqdnLabel}</span><input name="management_fqdn" required value="${escapeHtml(dashboard.site.management_fqdn)}" /></label>
          <label><span>${copy.sitePublicSmtpLabel}</span><input name="public_smtp_bind" required value="${escapeHtml(dashboard.site.public_smtp_bind)}" /></label>
          <label><span>${copy.siteManagementBindLabel}</span><input name="management_bind" required value="${escapeHtml(dashboard.site.management_bind)}" /></label>
        </div>
      `,
      payload: (form) => Object.fromEntries(new FormData(form).entries()),
      validate: () => [],
    },
    relay: {
      title: copy.platformRelay,
      summary: copy.platformRelayCopy,
      submitPath: "/api/relay",
      content: `
        <label class="toggle-field"><span>${copy.relayHaLabel}</span><input name="ha_enabled" type="checkbox"${dashboard.relay.ha_enabled ? " checked" : ""} /></label>
        <label><span>${copy.relayPrimaryLabel}</span><input name="primary_upstream" value="${escapeHtml(dashboard.relay.primary_upstream)}" /></label>
        <label><span>${copy.relaySecondaryLabel}</span><input name="secondary_upstream" value="${escapeHtml(dashboard.relay.secondary_upstream)}" /></label>
        <label><span>${copy.relayCoreDeliveryLabel}</span><input name="core_delivery_base_url" value="${escapeHtml(dashboard.relay.core_delivery_base_url)}" /></label>
        <label class="toggle-field"><span>${copy.relayMutualTlsLabel}</span><input name="mutual_tls_required" type="checkbox"${dashboard.relay.mutual_tls_required ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.relayFallbackLabel}</span><input name="fallback_to_hold_queue" type="checkbox"${dashboard.relay.fallback_to_hold_queue ? " checked" : ""} /></label>
        <label><span>${copy.relaySyncLabel}</span><input name="sync_interval_seconds" type="number" min="1" value="${escapeHtml(dashboard.relay.sync_interval_seconds)}" /></label>
        <label><span>${copy.relayDependencyLabel}</span><textarea name="lan_dependency_note" rows="4">${escapeHtml(dashboard.relay.lan_dependency_note)}</textarea></label>
      `,
      payload: (form) => ({
        ha_enabled: form.elements.namedItem("ha_enabled").checked,
        primary_upstream: form.elements.namedItem("primary_upstream").value,
        secondary_upstream: form.elements.namedItem("secondary_upstream").value,
        core_delivery_base_url: form.elements.namedItem("core_delivery_base_url").value,
        mutual_tls_required: form.elements.namedItem("mutual_tls_required").checked,
        fallback_to_hold_queue: form.elements.namedItem("fallback_to_hold_queue").checked,
        sync_interval_seconds: Number(form.elements.namedItem("sync_interval_seconds").value),
        lan_dependency_note: form.elements.namedItem("lan_dependency_note").value,
      }),
      validate: (form) => {
        const value = Number(form.elements.namedItem("sync_interval_seconds").value);
        return !Number.isInteger(value) || value < 1 ? [{ field: "sync_interval_seconds", message: copy.validationPositiveInteger }] : [];
      },
    },
    network: {
      title: copy.platformNetwork,
      summary: copy.platformNetworkCopy,
      submitPath: "/api/network",
      content: `
        <label><span>${copy.networkManagementCidrsLabel}</span><textarea name="allowed_management_cidrs" rows="4">${escapeHtml((dashboard.network.allowed_management_cidrs ?? []).join("\n"))}</textarea></label>
        <label><span>${copy.networkUpstreamCidrsLabel}</span><textarea name="allowed_upstream_cidrs" rows="4">${escapeHtml((dashboard.network.allowed_upstream_cidrs ?? []).join("\n"))}</textarea></label>
        <label><span>${copy.networkSmartHostsLabel}</span><textarea name="outbound_smart_hosts" rows="4">${escapeHtml((dashboard.network.outbound_smart_hosts ?? []).join("\n"))}</textarea></label>
        <label class="toggle-field"><span>${copy.networkPublicListenerLabel}</span><input name="public_listener_enabled" type="checkbox"${dashboard.network.public_listener_enabled ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.networkSubmissionListenerLabel}</span><input name="submission_listener_enabled" type="checkbox"${dashboard.network.submission_listener_enabled ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.networkProxyProtocolLabel}</span><input name="proxy_protocol_enabled" type="checkbox"${dashboard.network.proxy_protocol_enabled ? " checked" : ""} /></label>
        <label><span>${copy.networkConcurrentLabel}</span><input name="max_concurrent_sessions" type="number" min="1" value="${escapeHtml(dashboard.network.max_concurrent_sessions)}" /></label>
      `,
      payload: (form) => ({
        allowed_management_cidrs: parseLines(form.elements.namedItem("allowed_management_cidrs").value),
        allowed_upstream_cidrs: parseLines(form.elements.namedItem("allowed_upstream_cidrs").value),
        outbound_smart_hosts: parseLines(form.elements.namedItem("outbound_smart_hosts").value),
        public_listener_enabled: form.elements.namedItem("public_listener_enabled").checked,
        submission_listener_enabled: form.elements.namedItem("submission_listener_enabled").checked,
        proxy_protocol_enabled: form.elements.namedItem("proxy_protocol_enabled").checked,
        max_concurrent_sessions: Number(form.elements.namedItem("max_concurrent_sessions").value),
      }),
      validate: (form) => {
        const value = Number(form.elements.namedItem("max_concurrent_sessions").value);
        return !Number.isInteger(value) || value < 1 ? [{ field: "max_concurrent_sessions", message: copy.validationPositiveInteger }] : [];
      },
    },
    updates: {
      title: copy.platformUpdates,
      summary: copy.platformUpdatesCopy,
      submitPath: "/api/updates",
      content: `
        <label><span>${copy.updatesChannelLabel}</span><input name="channel" required value="${escapeHtml(dashboard.updates.channel)}" /></label>
        <label class="toggle-field"><span>${copy.updatesAutoDownloadLabel}</span><input name="auto_download" type="checkbox"${dashboard.updates.auto_download ? " checked" : ""} /></label>
        <label><span>${copy.updatesWindowLabel}</span><input name="maintenance_window" required value="${escapeHtml(dashboard.updates.maintenance_window)}" /></label>
        <label><span>${copy.updatesLastReleaseLabel}</span><input name="last_applied_release" value="${escapeHtml(dashboard.updates.last_applied_release)}" /></label>
        <label><span>${copy.updatesSourceLabel}</span><input name="update_source" value="${escapeHtml(dashboard.updates.update_source)}" /></label>
      `,
      payload: (form) => ({
        channel: form.elements.namedItem("channel").value,
        auto_download: form.elements.namedItem("auto_download").checked,
        maintenance_window: form.elements.namedItem("maintenance_window").value,
        last_applied_release: form.elements.namedItem("last_applied_release").value,
        update_source: form.elements.namedItem("update_source").value,
      }),
      validate: () => [],
    },
  };
}

function openPlatformDrawer(target, opener = document.activeElement) {
  const copy = getCopy();
  const dashboard = state.dashboard;
  const configs = getPlatformDrawerConfigs(dashboard, copy);
  const config = configs[target];
  renderDrawerForm({
    title: config.title,
    summary: config.summary,
    formId: "platform-form",
    opener,
    content: `
      ${config.content}
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const errors = config.validate(form);
      if (errors.length) {
        context.fail(errors);
      }
      state.dashboard = await putJson(config.submitPath, config.payload(form));
      await loadOps({ silent: true });
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

function renderTraceDrawer(trace, opener = document.activeElement) {
  const copy = getCopy();
  if (!trace) {
    renderDrawerContent(copy.traceSummaryTitle, copy.noTraceLoaded, `<p class="record-copy">${escapeHtml(copy.noTraceLoaded)}</p>`, opener);
    return;
  }
  const current = trace.current ?? {};
  const technicalStatus = current.technical_status ? escapeHtml(JSON.stringify(current.technical_status, null, 2)) : escapeHtml(copy.unset);
  const authSummary = current.auth_summary ? escapeHtml(JSON.stringify(current.auth_summary, null, 2)) : escapeHtml(copy.unset);
  const dsn = current.dsn ? escapeHtml(JSON.stringify(current.dsn, null, 2)) : "";
  const historyItems = (trace.history ?? [])
    .map(
      (item) => `
        <div class="trace-item">
          <strong>${escapeHtml(item.timestamp || copy.unset)}</strong>
          <p>${escapeHtml(`${item.queue || copy.unset} · ${item.status || copy.unset}`)}</p>
          <p>${escapeHtml(item.reason || item.route_target || item.peer || copy.unset)}</p>
        </div>
      `,
    )
    .join("");
  const decisionItems = (current.decision_trace ?? [])
    .map(
      (item) => `
        <div class="trace-item">
          <strong>${escapeHtml(item.stage || copy.unset)}</strong>
          <p>${escapeHtml(item.outcome || copy.unset)}</p>
          <p>${escapeHtml(item.detail || copy.unset)}</p>
        </div>
      `,
    )
    .join("");
  renderDrawerContent(
    current.subject || trace.trace_id,
    `${current.mail_from || copy.unset} -> ${formatList(current.rcpt_to ?? [])}`,
    `
      <div class="record-actions">
        ${current.trace_id || trace.trace_id ? `<button class="list-action" type="button" data-action="trace-retry" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRetry}</button>` : ""}
        ${current.queue === "quarantine" || current.queue === "held" ? `<button class="list-action" type="button" data-action="trace-release" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRelease}</button>` : ""}
        ${current.queue === "quarantine" ? `<button class="list-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceDelete}</button>` : ""}
      </div>
      <section class="trace-section">
        <h4>${copy.traceSummaryTitle}</h4>
        <div class="summary-grid">
          <div><p>${copy.traceLabel}</p><span class="record-copy">${escapeHtml(trace.trace_id)}</span></div>
          <div><p>${copy.statusLabel}</p><span class="record-copy">${escapeHtml(current.status || copy.unset)}</span></div>
          <div><p>${copy.queueLabel}</p><span class="record-copy">${escapeHtml(current.queue || copy.unset)}</span></div>
          <div><p>${copy.routeLabel}</p><span class="record-copy">${escapeHtml(current.route?.relay_target || copy.unset)}</span></div>
          <div><p>${copy.spamLabel}</p><span class="record-copy">${escapeHtml(formatScore(current.spam_score))}</span></div>
          <div><p>${copy.securityLabel}</p><span class="record-copy">${escapeHtml(formatScore(current.security_score))}</span></div>
        </div>
      </section>
      <div class="trace-columns">
        <section class="trace-section">
          <h4>${copy.tracePolicyTitle}</h4>
          <div class="trace-list">${decisionItems || `<div class="trace-item"><p>${escapeHtml(copy.unset)}</p></div>`}</div>
        </section>
        <section class="trace-section">
          <h4>${copy.traceTechnicalTitle}</h4>
          <div class="trace-list">
            <div class="trace-item"><strong>${copy.authLabel}</strong><pre>${authSummary}</pre></div>
            <div class="trace-item"><strong>${copy.technicalLabel}</strong><pre>${technicalStatus}</pre></div>
            ${dsn ? `<div class="trace-item"><strong>${copy.dsnLabel}</strong><pre>${dsn}</pre></div>` : ""}
          </div>
        </section>
      </div>
      <div class="trace-columns">
        <section class="trace-section">
          <h4>${copy.traceHeadersTitle}</h4>
          <div class="trace-list">
            ${(current.headers ?? [])
              .map(
                ([name, value]) => `
                  <div class="trace-item">
                    <strong>${escapeHtml(name)}</strong>
                    <p>${escapeHtml(value)}</p>
                  </div>
                `,
              )
              .join("") || `<div class="trace-item"><p>${escapeHtml(copy.unset)}</p></div>`}
          </div>
        </section>
        <section class="trace-section">
          <h4>${copy.traceBodyTitle}</h4>
          <pre>${escapeHtml(current.body_excerpt || copy.unset)}</pre>
        </section>
      </div>
      <section class="trace-section">
        <h4>${copy.traceHistoryTitle}</h4>
        <div class="trace-list">${historyItems || `<div class="trace-item"><p>${escapeHtml(copy.traceNoHistory)}</p></div>`}</div>
      </section>
    `,
    opener,
  );
}

// Async Actions and Lifecycle
async function loadTrace(traceId, opener = document.activeElement) {
  const copy = getCopy();
  state.loading.trace = true;
  renderDrawerContent(copy.traceSummaryTitle, copy.loadingTrace, buildLoadingRows(2), opener);
  try {
    state.selectedTrace = await fetchJson(`/api/history/${traceId}`);
    renderTraceDrawer(state.selectedTrace, opener);
  } catch (error) {
    renderDrawerContent(copy.traceSummaryTitle, copy.noTraceLoaded, `<p class="record-copy">${escapeHtml(error instanceof Error ? error.message : copy.unknownError)}</p>`, opener);
  } finally {
    state.loading.trace = false;
  }
}

async function triggerTraceAction(traceId, action) {
  const copy = getCopy();
  showFeedback(translate(copy.traceActionRunning, { action, traceId }), "warning");
  await fetchJson(`/api/traces/${traceId}/${action}`, { method: "POST" });
  showFeedback(translate(copy.traceActionCompleted, { traceId }));
  await loadOps({ silent: true });
  try {
    await loadTrace(traceId, elements.drawerClose);
  } catch {
    closeDrawer();
  }
}

async function openDigestReport(reportId, opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.digestOpen, copy.loadingRecords, buildLoadingRows(1), opener);
  const report = await fetchJson(`/api/reporting/digests/${reportId}`);
  renderDrawerContent(
    report.summary.scope_label,
    `${report.summary.generated_at} · ${report.summary.recipient}`,
    `<section class="trace-section"><pre class="digest-content">${escapeHtml(report.content)}</pre></section>`,
    opener,
  );
}

async function loadOps({ silent = false } = {}) {
  const copy = getCopy();
  state.loading.ops = true;
  if (!silent) {
    setButtonBusy(elements.refreshToolbar, true, copy.refreshing, copy.refreshState);
    setButtonBusy(elements.refresh, true, copy.refreshing, copy.refresh);
    Object.values(containers).forEach((container) => setListLoading(container));
  }
  try {
    const quarantineParams = new URLSearchParams(new FormData(elements.quarantineSearchForm));
    const historyParams = new URLSearchParams(new FormData(elements.historySearchForm));
    const [quarantine, history, routes, reporting, digestReports, policyStatus] = await Promise.all([
      fetchJson(`/api/quarantine?${quarantineParams.toString()}`),
      fetchJson(`/api/history?${historyParams.toString()}`),
      fetchJson("/api/routes/diagnostics"),
      fetchJson("/api/reporting"),
      fetchJson("/api/reporting/digests"),
      fetchJson("/api/policies/status"),
    ]);
    state.quarantine = quarantine ?? [];
    state.history = history?.items ?? [];
    state.routeDiagnostics = routes;
    state.reporting = reporting;
    state.digestReports = digestReports ?? [];
    state.policyStatus = policyStatus;
    renderDashboard();
  } finally {
    state.loading.ops = false;
    setButtonBusy(elements.refreshToolbar, false, copy.refreshing, copy.refreshState);
    setButtonBusy(elements.refresh, false, copy.refreshing, copy.refresh);
  }
}

async function load() {
  const copy = getCopy();
  state.loading.dashboard = true;
  setButtonBusy(elements.refreshToolbar, true, copy.refreshing, copy.refreshState);
  setButtonBusy(elements.refresh, true, copy.refreshing, copy.refresh);
  syncLoadingState();
  try {
    state.dashboard = await fetchDashboard();
    await loadOps({ silent: true });
    setAuthenticated(true);
    hideFeedback();
  } catch (error) {
    if (error instanceof Error && error.message === "401") {
      window.localStorage.removeItem(AUTH_TOKEN_KEY);
      setAuthenticated(false);
      showLoginFeedback(copy.authRequired, "error");
      return;
    }
    setAuthenticated(Boolean(window.localStorage.getItem(AUTH_TOKEN_KEY)));
    showFeedback(error instanceof Error ? error.message : copy.unknownError, "error");
  } finally {
    state.loading.dashboard = false;
    setButtonBusy(elements.refreshToolbar, false, copy.refreshing, copy.refreshState);
    setButtonBusy(elements.refresh, false, copy.refreshing, copy.refresh);
  }
}

async function loginAdmin() {
  const copy = getCopy();
  const form = elements.loginForm;
  const payload = Object.fromEntries(new FormData(form).entries());
  const email = normalizeEmail(payload.email);
  const password = String(payload.password ?? "");
  if (!isValidEmail(email) || !password.trim()) {
    showLoginFeedback(copy.loginValidation, "error");
    return;
  }
  state.loading.auth = true;
  setButtonBusy(form.querySelector('button[type="submit"]'), true, copy.signingIn, copy.signIn);
  try {
    const response = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    if (!response.ok) {
      if (response.status === 502) {
        throw new Error(copy.login502);
      }
      if (response.status === 401 || response.status === 403) {
        throw new Error(copy.loginFailed);
      }
      await parseError(response);
    }
    const body = await response.json();
    window.localStorage.setItem(AUTH_TOKEN_KEY, body.token);
    window.localStorage.setItem(LAST_ADMIN_EMAIL_KEY, email);
    showLoginFeedback(copy.authenticated, "success");
    await load();
  } finally {
    state.loading.auth = false;
    setButtonBusy(form.querySelector('button[type="submit"]'), false, copy.signingIn, copy.signIn);
  }
}

function hydrateLoginForm() {
  const email = window.localStorage.getItem(LAST_ADMIN_EMAIL_KEY);
  if (email) {
    const field = elements.loginForm.elements.namedItem("email");
    if (field && !field.value) {
      field.value = email;
    }
  }
}

function runAction(promiseFactory) {
  void promiseFactory().catch((error) => showFeedback(error instanceof Error ? error.message : getCopy().unknownError, "error"));
}

function getActionHandlers(actionTarget) {
  const { traceId, ruleId, index, reportId, target } = actionTarget.dataset;
  return {
    "drawer-close": () => closeDrawer(),
    "trace-open": () => runAction(() => loadTrace(traceId, actionTarget)),
    "trace-release": () => runAction(() => triggerTraceAction(traceId, "release")),
    "trace-delete": () => runAction(() => triggerTraceAction(traceId, "delete")),
    "trace-retry": () => runAction(() => triggerTraceAction(traceId, "retry")),
    "address-create": () => openAddressRuleDrawer(null, actionTarget),
    "address-edit": () => openAddressRuleDrawer(ruleId, actionTarget),
    "address-delete": () => runAction(() => deleteAddressRule(ruleId)),
    "attachment-create": () => openAttachmentRuleDrawer(null, actionTarget),
    "attachment-edit": () => openAttachmentRuleDrawer(ruleId, actionTarget),
    "attachment-delete": () => runAction(() => deleteAttachmentRule(ruleId)),
    "dkim-domain-create": () => openDkimDomainDrawer(null, actionTarget),
    "dkim-domain-edit": () => openDkimDomainDrawer(Number(index), actionTarget),
    "dkim-domain-delete": () => runAction(() => deleteDkimDomain(Number(index))),
    "digest-default-create": () => openDigestDefaultDrawer(null, actionTarget),
    "digest-default-edit": () => openDigestDefaultDrawer(Number(index), actionTarget),
    "digest-default-delete": () => runAction(() => deleteDigestDefault(Number(index))),
    "digest-override-create": () => openDigestOverrideDrawer(null, actionTarget),
    "digest-override-edit": () => openDigestOverrideDrawer(Number(index), actionTarget),
    "digest-override-delete": () => runAction(() => deleteDigestOverride(Number(index))),
    "digest-open": () => runAction(() => openDigestReport(reportId, actionTarget)),
    "platform-edit": () => openPlatformDrawer(target, actionTarget),
    "refresh-quarantine": () => runAction(() => loadOps()),
    "refresh-history": () => runAction(() => loadOps()),
  };
}

function handleBodyClick(event) {
  const actionTarget = event.target.closest("[data-action]");
  if (actionTarget) {
    const handler = getActionHandlers(actionTarget)[actionTarget.dataset.action];
    if (handler) {
      handler();
    }
    return;
  }

  const scrollTarget = event.target.closest("[data-scroll-target]");
  if (scrollTarget) {
    const section = document.getElementById(scrollTarget.dataset.scrollTarget);
    if (section) {
      section.scrollIntoView({ behavior: "smooth", block: "start" });
      updateNavState(section.id);
      setSidebarOpen(false);
    }
  }
}

function trapDrawerFocus(event) {
  if (event.key !== "Tab" || !state.drawer.open) {
    return;
  }
  const focusable = Array.from(
    elements.drawer.querySelectorAll("button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])"),
  ).filter((item) => !item.disabled);
  if (!focusable.length) {
    event.preventDefault();
    return;
  }
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

function setLocale(locale) {
  i18n.setLocale(locale);
  hydrateLocaleSpecificState();
  if (state.dashboard) {
    renderDashboard();
  } else {
    syncLoadingState();
  }
}

function hydrateLocaleSpecificState() {
  if (elements.mobileSidebarToggle) {
    elements.mobileSidebarToggle.textContent = document.body.classList.contains("sidebar-open")
      ? getCopy().closeNavigation
      : getCopy().openNavigation;
  }
  elements.operatorRole.textContent = getCopy().operatorRole;
  if (state.dashboard) {
    renderOverview();
  }
}

// Event Wiring and Bootstrap
elements.loginForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void loginAdmin().catch((error) => showLoginFeedback(error instanceof Error ? error.message : getCopy().unknownError, "error"));
});

elements.refresh.addEventListener("click", () => {
  void load();
});

elements.refreshToolbar.addEventListener("click", () => {
  void load();
});

elements.runDigests.addEventListener("click", async () => {
  const copy = getCopy();
  setButtonBusy(elements.runDigests, true, copy.runningDigests, copy.runDigests);
  try {
    const result = await fetchJson("/api/reporting/digests/run", { method: "POST" });
    showFeedback(translate(copy.digestGeneratedSummary, { count: result.generated_reports?.length ?? 0 }));
    await loadOps({ silent: true });
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, "error");
  } finally {
    setButtonBusy(elements.runDigests, false, copy.runningDigests, copy.runDigests);
  }
});

elements.quarantineSearchForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void loadOps().catch((error) => showFeedback(error instanceof Error ? error.message : getCopy().unknownError, "error"));
});

elements.historySearchForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void loadOps().catch((error) => showFeedback(error instanceof Error ? error.message : getCopy().unknownError, "error"));
});

elements.createAddressRule.addEventListener("click", (event) => openAddressRuleDrawer(null, event.currentTarget));
elements.createAttachmentRule.addEventListener("click", (event) => openAttachmentRuleDrawer(null, event.currentTarget));
elements.editRecipientVerification.addEventListener("click", (event) => openRecipientVerificationDrawer(event.currentTarget));
elements.editDkimSettings.addEventListener("click", (event) => openDkimSettingsDrawer(event.currentTarget));
elements.createDkimDomain.addEventListener("click", (event) => openDkimDomainDrawer(null, event.currentTarget));
elements.editDigestSettings.addEventListener("click", (event) => openDigestSettingsDrawer(event.currentTarget));
elements.createDigestDefault.addEventListener("click", (event) => openDigestDefaultDrawer(null, event.currentTarget));
elements.createDigestOverride.addEventListener("click", (event) => openDigestOverrideDrawer(null, event.currentTarget));

document.body.addEventListener("click", handleBodyClick);

elements.drawerClose.addEventListener("click", closeDrawer);
elements.drawerBackdrop.addEventListener("click", (event) => {
  if (event.target === elements.drawerBackdrop) {
    closeDrawer();
  }
});

elements.sidebarBackdrop.addEventListener("click", () => setSidebarOpen(false));
elements.sidebarToggle?.addEventListener("click", toggleSidebarState);
elements.mobileSidebarToggle?.addEventListener("click", toggleSidebarState);

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (state.drawer.open) {
      closeDrawer();
      return;
    }
    if (document.body.classList.contains("sidebar-open")) {
      setSidebarOpen(false);
    }
  }
  trapDrawerFocus(event);
});

i18n.bindLocalePickers(elements.localePickers, setLocale);
hydrateLoginForm();
registerSectionObserver();
updateNavState("overview-section");
try {
  setSidebarCollapsed(window.localStorage.getItem("lpeCtSidebarCollapsed") === "true");
} catch {}
setLocale(i18n.getLocale());
syncLoadingState();
window.setInterval(() => {
  if (!elements.contextTime) {
    return;
  }
  elements.contextTime.textContent = formatDateTime();
}, 60000);
window.addEventListener("resize", () => {
  if (window.innerWidth > 1024) {
    setSidebarOpen(false);
  }
});
void load();


