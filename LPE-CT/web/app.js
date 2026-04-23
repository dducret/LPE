const feedback = document.getElementById("feedback");
const loginFeedback = document.getElementById("login-feedback");
const loginShell = document.getElementById("login-shell");
const consoleShell = document.getElementById("console-shell");
const drawerBackdrop = document.getElementById("drawer-backdrop");
const drawerTitle = document.getElementById("drawer-title");
const drawerSummary = document.getElementById("drawer-summary");
const drawerContent = document.getElementById("drawer-content");
const localePickers = Array.from(document.querySelectorAll("[data-locale-picker]"));
const scrollButtons = Array.from(document.querySelectorAll("[data-scroll-target]"));
const { createI18n, defineLocaleCatalog } = window.LpeCtI18n;

const AUTH_TOKEN_KEY = "lpeCtAdminToken";
const LAST_ADMIN_EMAIL_KEY = "lpeCtAdminLastEmail";
const LOCALE_KEY = "lpe.locale";
const supportedLocales = ["en", "fr", "de", "it", "es"];
const localeLabels = { en: "English", fr: "Francais", de: "Deutsch", it: "Italiano", es: "Espanol" };

const baseMessages = {
  pageTitle: "LPE-CT Management Console",
  languageLabel: "Language",
  brand: "La Poste Electronique",
  loginTitle: "LPE-CT Management Login",
  loginCopy: "Authenticate with the management administrator configured for this sorting center.",
  adminEmail: "Admin email",
  password: "Password",
  signIn: "Sign in",
  consoleTitle: "Sorting Center",
  consoleIntro:
    "Unified control plane for quarantine, transport history, perimeter policy, recipient verification, DKIM, and digest reporting.",
  refresh: "Refresh",
  refreshState: "Refresh state",
  heroKicker: "DMZ sorting center",
  heroLoadingTitle: "Loading...",
  heroLoadingSummary: "Reading sorting-center state.",
  pillBoundary: "DMZ boundary",
  pillTrace: "Traceable flow",
  pillPolicy: "Operator policy",
  metricInbound: "Inbound",
  metricDeferred: "Deferred",
  metricQuarantine: "Quarantine",
  metricAttempts: "Attempts / h",
  navOperationsHeading: "Operations",
  navPolicyHeading: "Policy",
  navReportingHeading: "Reporting",
  navOverview: "Overview",
  navQuarantine: "Quarantine",
  navHistory: "Mail history",
  navAddressRules: "Allow / block lists",
  navAttachmentRules: "Attachment rules",
  navVerification: "Recipient verification",
  navDkim: "DKIM domains",
  navDigest: "Digest reports",
  navPlatform: "Node and transport",
  navAudit: "Audit journal",
  quarantineTitle: "Quarantine management",
  quarantineSummary:
    "Search retained messages, inspect trace evidence, and release or delete from the same operator surface.",
  historyTitle: "Mail history and reporting",
  historySummary:
    "Search retained inbound and outbound flow with disposition, route, trace, and policy context.",
  addressRulesTitle: "Allow and block lists",
  addressRulesSummary:
    "Maintain sender and recipient policy entries with explicit scope and effective action.",
  attachmentRulesTitle: "Attachment filtering rules",
  attachmentRulesSummary:
    "Distinguish filename extension, MIME type, and Magika detected-type controls without leaving the policy workspace.",
  verificationTitle: "Recipient verification",
  verificationSummary:
    "Show verification mode, failure posture, cache behavior, and the current operational backend clearly.",
  dkimTitle: "DKIM domain configuration",
  dkimSummary:
    "Manage per-domain selectors and key references, and inspect signing readiness from the management interface.",
  digestTitle: "Digest reports",
  digestSummary:
    "Operate domain defaults, mailbox overrides, retained report artifacts, and the scheduling controls used by the sorting center.",
  platformTitle: "Node and transport profile",
  platformSummary:
    "Keep node identity, relay, network, and update settings available without breaking the primary mail-flow workspace.",
  auditTitle: "Recent journal",
  auditSummary: "Recent management changes recorded by the sorting center.",
  createRule: "Create rule",
  createDomain: "Create domain",
  createDomainDefault: "Create domain default",
  createOverride: "Create override",
  editSettings: "Edit settings",
  editSigningProfile: "Edit signing profile",
  runDigests: "Run digests now",
  search: "Search",
  allDirections: "All directions",
  allQueues: "All queues",
  quarantineSearchPlaceholder: "Search trace, sender, recipient, subject, or Message-Id",
  historySearchPlaceholder: "Search trace, sender, recipient, subject, route, or policy",
  domainFilterPlaceholder: "Filter domain",
  dispositionFilterPlaceholder: "Disposition or policy tag",
  drawerDefaultTitle: "Details",
  drawerDefaultSummary: "Inspect details or edit the selected record.",
  close: "Close",
  authRequired: "Management authentication required.",
  authenticated: "Authenticated.",
  login502: "Management API unreachable (502). Check lpe-ct.service and nginx.",
  unknownError: "Unknown error.",
  statusDrain: "Drain mode",
  statusProduction: "Production",
  relayReachable: "LAN relay reachable",
  relayUnreachable: "LAN relay unreachable",
  unset: "unset",
  noResults: "No records matched the current filters.",
  noAddressRules: "No allow or block rules are configured.",
  noAttachmentRules: "No attachment policy rules are configured.",
  noDkimDomains: "No DKIM domains are configured.",
  noDigestDefaults: "No domain digest defaults are configured.",
  noDigestOverrides: "No mailbox digest overrides are configured.",
  noDigestReports: "No digest reports generated yet.",
  traceOpen: "Open",
  traceRelease: "Release",
  traceDelete: "Delete",
  traceRetry: "Retry",
  edit: "Edit",
  remove: "Delete",
  enabled: "Enabled",
  disabled: "Disabled",
  active: "Active",
  inactive: "Inactive",
  create: "Create",
  save: "Save",
  cancel: "Cancel",
  recordSaved: "Changes saved.",
  traceActionCompleted: "Trace action completed for {traceId}.",
  digestGeneratedSummary: "{count} digest report(s) generated.",
  historyResultSummary: "{count} trace(s) matched",
  heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · primary relay {primaryUpstream}",
  policyRoleSender: "Sender",
  policyRoleRecipient: "Recipient",
  policyActionAllow: "Allow",
  policyActionBlock: "Block",
  attachmentScopeExtension: "Extension",
  attachmentScopeMime: "MIME type",
  attachmentScopeDetected: "Detected file type",
  verificationModeFailClosed: "Fail closed",
  verificationModeFailOpen: "Fail open",
  cacheBackendMemory: "Memory-only cache",
  cacheBackendPostgres: "Private PostgreSQL cache",
  dkimKeyStatusPresent: "Key file present",
  dkimKeyStatusMissing: "Key file missing",
  dkimKeyStatusUnreadable: "Key file unreadable",
  dkimKeyStatusInvalid: "Invalid key path",
  dkimKeyStatusNotConfigured: "No key path",
  platformNode: "Node identity",
  platformRelay: "Relay toward LAN",
  platformNetwork: "Network surface",
  platformUpdates: "Updates",
  platformNodeCopy: "Name, role, region, DMZ zone, MX, and management endpoints.",
  platformRelayCopy: "Primary and secondary upstreams, hold queue behavior, and core delivery path.",
  platformNetworkCopy: "Allowed CIDRs, listeners, smart hosts, and concurrent session limits.",
  platformUpdatesCopy: "Git-first update channel, maintenance window, and download behavior.",
  verificationCardTitle: "Recipient verification mode",
  verificationCacheTtl: "Cache TTL",
  verificationCacheBackend: "Cache backend",
  verificationState: "Operational state",
  dkimProfileTitle: "Signing profile",
  dkimHeaders: "Headers",
  dkimExpiration: "Expiration",
  digestSettingsTitle: "Digest schedule",
  digestDefaultsTitle: "Domain defaults",
  digestOverridesTitle: "Mailbox overrides",
  digestReportsTitle: "Recent digest artifacts",
  digestEnabledLabel: "Digest generation",
  digestIntervalLabel: "Interval (minutes)",
  digestMaxItemsLabel: "Max items per digest",
  digestRetentionLabel: "History retention (days)",
  digestLastRun: "Last run",
  digestNextRun: "Next run",
  domainDefaultsLabel: "Recipients",
  overrideRecipientLabel: "Digest recipient",
  overrideMailboxLabel: "Mailbox",
  addressRuleValueLabel: "Address or domain",
  addressRuleRoleLabel: "Rule scope",
  addressRuleActionLabel: "Effective action",
  attachmentRuleValueLabel: "Rule value",
  attachmentRuleScopeLabel: "Rule type",
  attachmentRuleActionLabel: "Effective action",
  recipientVerificationEnabledLabel: "Recipient verification enabled",
  recipientVerificationFailClosedLabel: "Fail closed on bridge errors",
  recipientVerificationTtlLabel: "Cache TTL (seconds)",
  dkimSigningEnabledLabel: "DKIM signing enabled",
  dkimOverSignLabel: "Over-sign headers",
  dkimHeadersLabel: "Signed headers",
  dkimExpirationLabel: "Expiration (seconds, optional)",
  dkimDomainLabel: "Domain",
  dkimSelectorLabel: "Selector",
  dkimKeyPathLabel: "Private key path",
  reportingEnabledLabel: "Digest reports enabled",
  reportingSettingsNote:
    "Language and content-format options are not exposed by the current backend contract; this UI manages the available scheduling and recipient controls.",
  siteNodeNameLabel: "Node name",
  siteRoleLabel: "Role",
  siteRegionLabel: "Region",
  siteDmzZoneLabel: "DMZ zone",
  sitePublishedMxLabel: "Published MX",
  siteManagementFqdnLabel: "Management FQDN",
  sitePublicSmtpLabel: "Public SMTP bind",
  siteManagementBindLabel: "Management bind",
  relayHaLabel: "HA enabled",
  relayPrimaryLabel: "Primary upstream",
  relaySecondaryLabel: "Secondary upstream",
  relayCoreDeliveryLabel: "Core delivery base URL",
  relayMutualTlsLabel: "Mutual TLS required",
  relayFallbackLabel: "Fallback to hold queue",
  relaySyncLabel: "Sync interval (seconds)",
  relayDependencyLabel: "LAN dependency note",
  networkManagementCidrsLabel: "Allowed management CIDRs",
  networkUpstreamCidrsLabel: "Allowed upstream CIDRs",
  networkSmartHostsLabel: "Outbound smart hosts",
  networkPublicListenerLabel: "Public listener enabled",
  networkSubmissionListenerLabel: "Submission listener enabled",
  networkProxyProtocolLabel: "Proxy protocol enabled",
  networkConcurrentLabel: "Max concurrent sessions",
  updatesChannelLabel: "Channel",
  updatesAutoDownloadLabel: "Auto download",
  updatesWindowLabel: "Maintenance window",
  updatesLastReleaseLabel: "Last applied release",
  updatesSourceLabel: "Update source",
  traceSummaryTitle: "Trace summary",
  tracePolicyTitle: "Policy evidence",
  traceTechnicalTitle: "Transport details",
  traceHeadersTitle: "Headers",
  traceBodyTitle: "Body excerpt",
  traceHistoryTitle: "Flow history",
  traceNoHistory: "No retained history for this trace.",
  digestOpen: "Open digest",
  noTraceLoaded: "No trace selected.",
};

const localizedMessages = {
  fr: {
    pageTitle: "Console de management LPE-CT",
    languageLabel: "Langue",
    loginTitle: "Connexion management LPE-CT",
    signIn: "Se connecter",
    refresh: "Actualiser",
    search: "Rechercher",
    close: "Fermer",
    consoleTitle: "Centre de Tri",
  },
  de: {
    pageTitle: "LPE-CT Verwaltungsoberflaeche",
    languageLabel: "Sprache",
    loginTitle: "LPE-CT Verwaltungsanmeldung",
    signIn: "Anmelden",
    refresh: "Aktualisieren",
    search: "Suchen",
    close: "Schliessen",
    consoleTitle: "Sortierzentrum",
  },
  it: {
    pageTitle: "Console di gestione LPE-CT",
    languageLabel: "Lingua",
    loginTitle: "Accesso gestione LPE-CT",
    signIn: "Accedi",
    refresh: "Aggiorna",
    search: "Cerca",
    close: "Chiudi",
    consoleTitle: "Centro di Smistamento",
  },
  es: {
    pageTitle: "Consola de gestion LPE-CT",
    languageLabel: "Idioma",
    loginTitle: "Acceso de gestion LPE-CT",
    signIn: "Iniciar sesion",
    refresh: "Actualizar",
    search: "Buscar",
    close: "Cerrar",
    consoleTitle: "Centro de Clasificacion",
  },
};

const messages = defineLocaleCatalog({
  supportedLocales,
  defaultLocale: "en",
  base: baseMessages,
  localized: localizedMessages,
});

const i18n = createI18n({
  storageKey: LOCALE_KEY,
  supportedLocales,
  localeLabels,
  messages,
});

const state = {
  dashboard: null,
  quarantine: [],
  history: [],
  routeDiagnostics: null,
  reporting: null,
  digestReports: [],
  policyStatus: null,
  selectedTrace: null,
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

function getCopy() {
  return i18n.getCopy();
}

function translate(template, values = {}) {
  return i18n.translate(template, values);
}

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

function showFeedback(message, isError) {
  feedback.textContent = message;
  feedback.className = isError ? "feedback error" : "feedback";
}

function showLoginFeedback(message, isError) {
  loginFeedback.textContent = message;
  loginFeedback.className = isError ? "feedback error" : "feedback";
}

function hideFeedback() {
  feedback.className = "feedback hidden";
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

async function fetchJson(path, init = {}) {
  const response = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers ?? {}) },
  });
  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }
  return response.json();
}

async function putJson(path, payload) {
  return fetchJson(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

function currentPolicies() {
  return structuredClone(state.dashboard.policies);
}

function currentReporting() {
  return structuredClone(state.reporting?.settings ?? state.dashboard.reporting);
}

function openDrawer(title, summary, content) {
  drawerTitle.textContent = title;
  drawerSummary.textContent = summary;
  drawerContent.innerHTML = content;
  drawerBackdrop.classList.remove("hidden");
}

function closeDrawer() {
  drawerBackdrop.classList.add("hidden");
}

function syncLoadingState() {
  if (state.dashboard) {
    return;
  }
  const copy = getCopy();
  document.getElementById("node-name").textContent = copy.heroLoadingTitle;
  document.getElementById("hero-summary").textContent = copy.heroLoadingSummary;
}

function setLocale(locale) {
  i18n.setLocale(locale);
  if (state.dashboard) {
    renderDashboard();
  } else {
    syncLoadingState();
  }
}

function renderMetric(id, value) {
  document.getElementById(id).textContent = String(value ?? "-");
}

function statusChipClass(value) {
  if (value === "present" || value === "active" || value === "enabled") {
    return "status-chip ok";
  }
  if (value === "missing" || value === "disabled") {
    return "status-chip danger";
  }
  if (value === "unreadable" || value === "invalid-path") {
    return "status-chip warn";
  }
  return "status-chip muted";
}

function labelForAddressRole(role) {
  const copy = getCopy();
  return role === "sender" ? copy.policyRoleSender : copy.policyRoleRecipient;
}

function labelForAction(action) {
  const copy = getCopy();
  return action === "allow" ? copy.policyActionAllow : copy.policyActionBlock;
}

function labelForAttachmentScope(scope) {
  const copy = getCopy();
  if (scope === "extension") return copy.attachmentScopeExtension;
  if (scope === "mime") return copy.attachmentScopeMime;
  return copy.attachmentScopeDetected;
}

function labelForVerificationBackend(backend) {
  const copy = getCopy();
  return backend === "private-postgres" ? copy.cacheBackendPostgres : copy.cacheBackendMemory;
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

function getAddressRules(policies = state.dashboard?.policies) {
  if (!policies) {
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
  if (!policies) {
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

function renderEmpty(container, message) {
  container.innerHTML = `<div class="record-row"><p class="record-copy">${escapeHtml(message)}</p></div>`;
}

function renderQuarantine() {
  const copy = getCopy();
  const items = state.quarantine;
  if (!items.length) {
    renderEmpty(containers.quarantine, copy.noResults);
    return;
  }
  containers.quarantine.innerHTML = items
    .map(
      (item) => `
        <article class="record-row">
          <div class="record-head">
            <div>
              <h4 class="record-title">${escapeHtml(item.subject || item.trace_id)}</h4>
              <div class="record-meta">${escapeHtml(item.received_at)} · ${escapeHtml(item.mail_from)} -> ${escapeHtml(item.rcpt_to.join(", "))}</div>
            </div>
            <div class="record-tags">
              <span class="badge danger">${escapeHtml(item.status)}</span>
              <span class="pill">${escapeHtml(item.direction)}</span>
            </div>
          </div>
          <div class="record-copy">${escapeHtml(item.reason || item.internet_message_id || copy.unset)}</div>
          <div class="record-tags">
            <span class="pill">spam ${escapeHtml(item.spam_score.toFixed(1))}</span>
            <span class="pill">security ${escapeHtml(item.security_score.toFixed(1))}</span>
            <span class="pill">${escapeHtml(item.trace_id)}</span>
          </div>
          <div class="record-actions">
            <button class="list-action" type="button" data-action="trace-open" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceOpen}</button>
            <button class="list-action" type="button" data-action="trace-release" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceRelease}</button>
            <button class="list-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(item.trace_id)}">${copy.traceDelete}</button>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderHistory() {
  const copy = getCopy();
  const items = state.history;
  if (!items.length) {
    renderEmpty(containers.history, copy.noResults);
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
                <div class="record-meta">${escapeHtml(item.latest_event_at)} · ${escapeHtml(item.mail_from)} -> ${escapeHtml(item.rcpt_to.join(", "))}</div>
              </div>
              <div class="record-tags">
                <span class="badge">${escapeHtml(item.queue)}</span>
                <span class="pill">${escapeHtml(item.status)}</span>
                <span class="pill">${escapeHtml(item.direction)}</span>
              </div>
            </div>
            <div class="record-copy">${escapeHtml(item.reason || item.route_target || item.internet_message_id || copy.unset)}</div>
            <div class="record-tags">
              <span class="pill">events ${escapeHtml(item.event_count)}</span>
              <span class="pill">${escapeHtml(item.trace_id)}</span>
              ${(item.policy_tags ?? []).slice(0, 3).map((tag) => `<span class="pill">${escapeHtml(tag)}</span>`).join("")}
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
    renderEmpty(containers.addressRules, copy.noAddressRules);
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
    renderEmpty(containers.attachmentRules, copy.noAttachmentRules);
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
    renderEmpty(containers.recipientVerification, copy.noResults);
    return;
  }
  containers.recipientVerification.innerHTML = `
    <article class="summary-card">
      <strong>${copy.verificationCardTitle}</strong>
      <div class="summary-grid">
        <div>
          <p>${copy.verificationState}</p>
          <span class="${statusChipClass(status.operational_state)}">${escapeHtml(status.enabled ? copy.enabled : copy.disabled)}</span>
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
          <span class="pill">${escapeHtml(status.cache_ttl_seconds)}s</span>
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
  const domainRows = domains.length
    ? domains
        .map(
          (domain, index) => `
            <article class="record-row">
              <div class="record-head">
                <div>
                  <h4 class="record-title">${escapeHtml(domain.domain)}</h4>
                  <div class="record-meta">${escapeHtml(domain.selector)} · ${escapeHtml(domain.private_key_path)}</div>
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
    : `<div class="record-row"><p class="record-copy">${escapeHtml(copy.noDkimDomains)}</p></div>`;
  containers.dkimDomains.innerHTML = profile + domainRows;
}

function renderDigestReporting() {
  const copy = getCopy();
  const reporting = state.reporting?.settings;
  const reports = state.digestReports;
  if (!reporting) {
    renderEmpty(containers.digestSettings, copy.noResults);
    renderEmpty(containers.digestDefaults, copy.noResults);
    renderEmpty(containers.digestOverrides, copy.noResults);
    renderEmpty(containers.digestReports, copy.noResults);
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
          <span class="pill">${escapeHtml(reporting.digest_interval_minutes)} min</span>
        </div>
        <div>
          <p>${copy.digestMaxItemsLabel}</p>
          <span class="pill">${escapeHtml(reporting.digest_max_items)}</span>
        </div>
        <div>
          <p>${copy.digestRetentionLabel}</p>
          <span class="pill">${escapeHtml(reporting.history_retention_days)} d</span>
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

  containers.digestDefaults.innerHTML = `
    <article class="summary-card"><strong>${copy.digestDefaultsTitle}</strong></article>
    ${
      reporting.domain_defaults.length
        ? reporting.domain_defaults
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
            .join("")
        : `<div class="record-row"><p class="record-copy">${escapeHtml(copy.noDigestDefaults)}</p></div>`
    }
  `;

  containers.digestOverrides.innerHTML = `
    <article class="summary-card"><strong>${copy.digestOverridesTitle}</strong></article>
    ${
      reporting.user_overrides.length
        ? reporting.user_overrides
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
            .join("")
        : `<div class="record-row"><p class="record-copy">${escapeHtml(copy.noDigestOverrides)}</p></div>`
    }
  `;

  containers.digestReports.innerHTML = `
    <article class="summary-card"><strong>${copy.digestReportsTitle}</strong></article>
    ${
      reports.length
        ? reports
            .map(
              (report) => `
                <article class="record-row">
                  <div class="record-head">
                    <div>
                      <h4 class="record-title">${escapeHtml(report.scope_label)}</h4>
                      <div class="record-meta">${escapeHtml(report.generated_at)} · ${escapeHtml(report.recipient)}</div>
                    </div>
                  </div>
                  <div class="record-copy">${escapeHtml(`${report.item_count} items${report.top_reason ? ` · ${report.top_reason}` : ""}`)}</div>
                  <div class="record-actions">
                    <button class="list-action" type="button" data-action="digest-open" data-report-id="${escapeHtml(report.report_id)}">${copy.digestOpen}</button>
                  </div>
                </article>
              `,
            )
            .join("")
        : `<div class="record-row"><p class="record-copy">${escapeHtml(copy.noDigestReports)}</p></div>`
    }
  `;
}

function renderPlatform() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  containers.platform.innerHTML = `
    <article class="record-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${copy.platformNode}</h4>
          <div class="record-copy">${copy.platformNodeCopy}</div>
        </div>
      </div>
      <div class="record-meta">${escapeHtml(dashboard.site.node_name)} · ${escapeHtml(dashboard.site.management_fqdn)} · ${escapeHtml(dashboard.site.public_smtp_bind)}</div>
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
      <div class="record-meta">${escapeHtml(dashboard.relay.primary_upstream || copy.unset)} · ${escapeHtml(dashboard.relay.secondary_upstream || copy.unset)}</div>
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
      <div class="record-meta">${escapeHtml(formatList(dashboard.network.allowed_management_cidrs))}</div>
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
      <div class="record-meta">${escapeHtml(dashboard.updates.channel)} · ${escapeHtml(dashboard.updates.maintenance_window)}</div>
      <div class="record-actions">
        <button class="list-action" type="button" data-action="platform-edit" data-target="updates">${copy.edit}</button>
      </div>
    </article>
  `;
}

function renderAudit() {
  containers.audit.innerHTML = state.dashboard.audit
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

function renderDashboard() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  document.getElementById("node-name").textContent = dashboard.site.node_name;
  document.getElementById("hero-summary").textContent = translate(copy.heroSummaryTemplate, {
    dmzZone: dashboard.site.dmz_zone || copy.unset,
    publishedMx: dashboard.site.published_mx || copy.unset,
    primaryUpstream: dashboard.relay.primary_upstream || copy.unset,
  });
  const statusBadge = document.getElementById("status-badge");
  const upstreamBadge = document.getElementById("upstream-badge");
  statusBadge.textContent = dashboard.policies.drain_mode ? copy.statusDrain : copy.statusProduction;
  statusBadge.className = dashboard.policies.drain_mode ? "badge warn" : "badge ok";
  upstreamBadge.textContent = dashboard.queues.upstream_reachable ? copy.relayReachable : copy.relayUnreachable;
  upstreamBadge.className = dashboard.queues.upstream_reachable ? "badge ok" : "badge danger";

  renderMetric("metric-inbound", dashboard.queues.inbound_messages);
  renderMetric("metric-deferred", dashboard.queues.deferred_messages);
  renderMetric("metric-quarantine", dashboard.queues.quarantined_messages);
  renderMetric("metric-attempts", dashboard.queues.delivery_attempts_last_hour);

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

async function savePolicies(policies) {
  const dashboard = await putJson("/api/policies", policies);
  state.dashboard = dashboard;
  hideFeedback();
  await loadOps();
}

async function saveReporting(settings) {
  const reporting = await putJson("/api/reporting", settings);
  state.reporting = reporting;
  state.digestReports = reporting.recent_reports ?? [];
  state.dashboard.reporting = reporting.settings;
  renderDashboard();
}

function findAddressRule(ruleId) {
  return getAddressRules().find((rule) => rule.id === ruleId) ?? null;
}

function findAttachmentRule(ruleId) {
  return getAttachmentRules().find((rule) => rule.id === ruleId) ?? null;
}

function openAddressRuleDrawer(ruleId = null) {
  const copy = getCopy();
  const rule = ruleId ? findAddressRule(ruleId) : { role: "sender", action: "allow", value: "" };
  openDrawer(
    ruleId ? copy.edit : copy.createRule,
    copy.addressRulesSummary,
    `
      <form id="address-rule-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("address-rule-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const nextRule = {
      role: form.get("role"),
      action: form.get("action"),
      value: String(form.get("value") ?? "").trim(),
    };
    const policies = currentPolicies();
    if (ruleId) {
      const existing = findAddressRule(ruleId);
      const currentList = policies.address_policy[routeToPolicies(existing.role, existing.action)];
      currentList.splice(existing.index, 1);
    }
    policies.address_policy[routeToPolicies(nextRule.role, nextRule.action)].push(nextRule.value);
    try {
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
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
  try {
    await savePolicies(policies);
    showFeedback(copy.recordSaved, false);
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
}

function openAttachmentRuleDrawer(ruleId = null) {
  const copy = getCopy();
  const rule = ruleId ? findAttachmentRule(ruleId) : { scope: "extension", action: "block", value: "" };
  openDrawer(
    ruleId ? copy.edit : copy.createRule,
    copy.attachmentRulesSummary,
    `
      <form id="attachment-rule-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("attachment-rule-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const nextRule = {
      scope: form.get("scope"),
      action: form.get("action"),
      value: String(form.get("value") ?? "").trim(),
    };
    const policies = currentPolicies();
    if (ruleId) {
      const existing = findAttachmentRule(ruleId);
      policies.attachment_policy[routeToAttachmentPolicies(existing.scope, existing.action)].splice(existing.index, 1);
    }
    policies.attachment_policy[routeToAttachmentPolicies(nextRule.scope, nextRule.action)].push(nextRule.value);
    try {
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
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
  try {
    await savePolicies(policies);
    showFeedback(copy.recordSaved, false);
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
}

function openRecipientVerificationDrawer() {
  const copy = getCopy();
  const settings = currentPolicies().recipient_verification;
  openDrawer(
    copy.editSettings,
    copy.verificationSummary,
    `
      <form id="recipient-verification-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("recipient-verification-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const policies = currentPolicies();
    policies.recipient_verification.enabled = form.elements.namedItem("enabled").checked;
    policies.recipient_verification.fail_closed = form.elements.namedItem("fail_closed").checked;
    policies.recipient_verification.cache_ttl_seconds = Number(form.elements.namedItem("cache_ttl_seconds").value);
    try {
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

function openDkimSettingsDrawer() {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  openDrawer(
    copy.editSigningProfile,
    copy.dkimSummary,
    `
      <form id="dkim-settings-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("dkim-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const policies = currentPolicies();
    const expirationValue = String(form.elements.namedItem("expiration_seconds").value).trim();
    policies.dkim.enabled = form.elements.namedItem("enabled").checked;
    policies.dkim.over_sign = form.elements.namedItem("over_sign").checked;
    policies.dkim.headers = String(form.elements.namedItem("headers").value)
      .split("\n")
      .map((value) => value.trim())
      .filter(Boolean);
    policies.dkim.expiration_seconds = expirationValue ? Number(expirationValue) : null;
    try {
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

function openDkimDomainDrawer(index = null) {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  const domain = index === null ? { domain: "", selector: "", private_key_path: "", enabled: true } : settings.domains[index];
  openDrawer(
    index === null ? copy.createDomain : copy.edit,
    copy.dkimSummary,
    `
      <form id="dkim-domain-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("dkim-domain-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const policies = currentPolicies();
    const nextDomain = {
      domain: String(form.elements.namedItem("domain").value).trim(),
      selector: String(form.elements.namedItem("selector").value).trim(),
      private_key_path: String(form.elements.namedItem("private_key_path").value).trim(),
      enabled: form.elements.namedItem("enabled").checked,
    };
    if (index === null) {
      policies.dkim.domains.push(nextDomain);
    } else {
      policies.dkim.domains[index] = nextDomain;
    }
    try {
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

async function deleteDkimDomain(index) {
  const copy = getCopy();
  const policies = currentPolicies();
  policies.dkim.domains.splice(index, 1);
  try {
    await savePolicies(policies);
    showFeedback(copy.recordSaved, false);
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
}

function openDigestSettingsDrawer() {
  const copy = getCopy();
  const settings = currentReporting();
  openDrawer(
    copy.editSettings,
    copy.digestSummary,
    `
      <form id="digest-settings-form" class="drawer-form">
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
        <p class="record-copy">${escapeHtml(copy.reportingSettingsNote)}</p>
        <div class="record-actions">
          <button class="primary-button compact-button" type="submit">${copy.save}</button>
        </div>
      </form>
    `,
  );
  document.getElementById("digest-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const settingsToSave = currentReporting();
    settingsToSave.digest_enabled = form.elements.namedItem("digest_enabled").checked;
    settingsToSave.digest_interval_minutes = Number(form.elements.namedItem("digest_interval_minutes").value);
    settingsToSave.digest_max_items = Number(form.elements.namedItem("digest_max_items").value);
    settingsToSave.history_retention_days = Number(form.elements.namedItem("history_retention_days").value);
    try {
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

function openDigestDefaultDrawer(index = null) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { domain: "", recipients: [] } : settings.domain_defaults[index];
  openDrawer(
    index === null ? copy.createDomainDefault : copy.edit,
    copy.digestDefaultsTitle,
    `
      <form id="digest-default-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("digest-default-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const settingsToSave = currentReporting();
    const nextItem = {
      domain: String(form.elements.namedItem("domain").value).trim(),
      recipients: String(form.elements.namedItem("recipients").value)
        .split("\n")
        .map((value) => value.trim())
        .filter(Boolean),
    };
    if (index === null) {
      settingsToSave.domain_defaults.push(nextItem);
    } else {
      settingsToSave.domain_defaults[index] = nextItem;
    }
    try {
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

async function deleteDigestDefault(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.domain_defaults.splice(index, 1);
  try {
    await saveReporting(settings);
    showFeedback(copy.recordSaved, false);
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
}

function openDigestOverrideDrawer(index = null) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { mailbox: "", recipient: "", enabled: true } : settings.user_overrides[index];
  openDrawer(
    index === null ? copy.createOverride : copy.edit,
    copy.digestOverridesTitle,
    `
      <form id="digest-override-form" class="drawer-form">
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
        </div>
      </form>
    `,
  );
  document.getElementById("digest-override-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const settingsToSave = currentReporting();
    const nextItem = {
      mailbox: String(form.elements.namedItem("mailbox").value).trim(),
      recipient: String(form.elements.namedItem("recipient").value).trim(),
      enabled: form.elements.namedItem("enabled").checked,
    };
    if (index === null) {
      settingsToSave.user_overrides.push(nextItem);
    } else {
      settingsToSave.user_overrides[index] = nextItem;
    }
    try {
      await saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(copy.recordSaved, false);
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

async function deleteDigestOverride(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.user_overrides.splice(index, 1);
  try {
    await saveReporting(settings);
    showFeedback(copy.recordSaved, false);
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
}

function openPlatformDrawer(target) {
  const copy = getCopy();
  const dashboard = state.dashboard;
  const configs = {
    site: {
      title: copy.platformNode,
      summary: copy.platformNodeCopy,
      submitPath: "/api/site",
      content: `
        <form id="platform-form" class="drawer-form">
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
          <div class="record-actions"><button class="primary-button compact-button" type="submit">${copy.save}</button></div>
        </form>
      `,
      payload: (form) => Object.fromEntries(new FormData(form).entries()),
    },
    relay: {
      title: copy.platformRelay,
      summary: copy.platformRelayCopy,
      submitPath: "/api/relay",
      content: `
        <form id="platform-form" class="drawer-form">
          <label class="toggle-field"><span>${copy.relayHaLabel}</span><input name="ha_enabled" type="checkbox"${dashboard.relay.ha_enabled ? " checked" : ""} /></label>
          <label><span>${copy.relayPrimaryLabel}</span><input name="primary_upstream" value="${escapeHtml(dashboard.relay.primary_upstream)}" /></label>
          <label><span>${copy.relaySecondaryLabel}</span><input name="secondary_upstream" value="${escapeHtml(dashboard.relay.secondary_upstream)}" /></label>
          <label><span>${copy.relayCoreDeliveryLabel}</span><input name="core_delivery_base_url" value="${escapeHtml(dashboard.relay.core_delivery_base_url)}" /></label>
          <label class="toggle-field"><span>${copy.relayMutualTlsLabel}</span><input name="mutual_tls_required" type="checkbox"${dashboard.relay.mutual_tls_required ? " checked" : ""} /></label>
          <label class="toggle-field"><span>${copy.relayFallbackLabel}</span><input name="fallback_to_hold_queue" type="checkbox"${dashboard.relay.fallback_to_hold_queue ? " checked" : ""} /></label>
          <label><span>${copy.relaySyncLabel}</span><input name="sync_interval_seconds" type="number" min="1" value="${escapeHtml(dashboard.relay.sync_interval_seconds)}" /></label>
          <label><span>${copy.relayDependencyLabel}</span><textarea name="lan_dependency_note" rows="4">${escapeHtml(dashboard.relay.lan_dependency_note)}</textarea></label>
          <div class="record-actions"><button class="primary-button compact-button" type="submit">${copy.save}</button></div>
        </form>
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
    },
    network: {
      title: copy.platformNetwork,
      summary: copy.platformNetworkCopy,
      submitPath: "/api/network",
      content: `
        <form id="platform-form" class="drawer-form">
          <label><span>${copy.networkManagementCidrsLabel}</span><textarea name="allowed_management_cidrs" rows="4">${escapeHtml((dashboard.network.allowed_management_cidrs ?? []).join("\n"))}</textarea></label>
          <label><span>${copy.networkUpstreamCidrsLabel}</span><textarea name="allowed_upstream_cidrs" rows="4">${escapeHtml((dashboard.network.allowed_upstream_cidrs ?? []).join("\n"))}</textarea></label>
          <label><span>${copy.networkSmartHostsLabel}</span><textarea name="outbound_smart_hosts" rows="4">${escapeHtml((dashboard.network.outbound_smart_hosts ?? []).join("\n"))}</textarea></label>
          <label class="toggle-field"><span>${copy.networkPublicListenerLabel}</span><input name="public_listener_enabled" type="checkbox"${dashboard.network.public_listener_enabled ? " checked" : ""} /></label>
          <label class="toggle-field"><span>${copy.networkSubmissionListenerLabel}</span><input name="submission_listener_enabled" type="checkbox"${dashboard.network.submission_listener_enabled ? " checked" : ""} /></label>
          <label class="toggle-field"><span>${copy.networkProxyProtocolLabel}</span><input name="proxy_protocol_enabled" type="checkbox"${dashboard.network.proxy_protocol_enabled ? " checked" : ""} /></label>
          <label><span>${copy.networkConcurrentLabel}</span><input name="max_concurrent_sessions" type="number" min="1" value="${escapeHtml(dashboard.network.max_concurrent_sessions)}" /></label>
          <div class="record-actions"><button class="primary-button compact-button" type="submit">${copy.save}</button></div>
        </form>
      `,
      payload: (form) => ({
        allowed_management_cidrs: String(form.elements.namedItem("allowed_management_cidrs").value)
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean),
        allowed_upstream_cidrs: String(form.elements.namedItem("allowed_upstream_cidrs").value)
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean),
        outbound_smart_hosts: String(form.elements.namedItem("outbound_smart_hosts").value)
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean),
        public_listener_enabled: form.elements.namedItem("public_listener_enabled").checked,
        submission_listener_enabled: form.elements.namedItem("submission_listener_enabled").checked,
        proxy_protocol_enabled: form.elements.namedItem("proxy_protocol_enabled").checked,
        max_concurrent_sessions: Number(form.elements.namedItem("max_concurrent_sessions").value),
      }),
    },
    updates: {
      title: copy.platformUpdates,
      summary: copy.platformUpdatesCopy,
      submitPath: "/api/updates",
      content: `
        <form id="platform-form" class="drawer-form">
          <label><span>${copy.updatesChannelLabel}</span><input name="channel" required value="${escapeHtml(dashboard.updates.channel)}" /></label>
          <label class="toggle-field"><span>${copy.updatesAutoDownloadLabel}</span><input name="auto_download" type="checkbox"${dashboard.updates.auto_download ? " checked" : ""} /></label>
          <label><span>${copy.updatesWindowLabel}</span><input name="maintenance_window" required value="${escapeHtml(dashboard.updates.maintenance_window)}" /></label>
          <label><span>${copy.updatesLastReleaseLabel}</span><input name="last_applied_release" value="${escapeHtml(dashboard.updates.last_applied_release)}" /></label>
          <label><span>${copy.updatesSourceLabel}</span><input name="update_source" value="${escapeHtml(dashboard.updates.update_source)}" /></label>
          <div class="record-actions"><button class="primary-button compact-button" type="submit">${copy.save}</button></div>
        </form>
      `,
      payload: (form) => ({
        channel: form.elements.namedItem("channel").value,
        auto_download: form.elements.namedItem("auto_download").checked,
        maintenance_window: form.elements.namedItem("maintenance_window").value,
        last_applied_release: form.elements.namedItem("last_applied_release").value,
        update_source: form.elements.namedItem("update_source").value,
      }),
    },
  };
  const config = configs[target];
  openDrawer(config.title, config.summary, config.content);
  document.getElementById("platform-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      state.dashboard = await putJson(config.submitPath, config.payload(event.currentTarget));
      closeDrawer();
      hideFeedback();
      await loadOps();
    } catch (error) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
    }
  });
}

function renderTraceDrawer(trace) {
  const copy = getCopy();
  if (!trace) {
    openDrawer(copy.traceSummaryTitle, copy.noTraceLoaded, `<p class="record-copy">${copy.noTraceLoaded}</p>`);
    return;
  }
  const current = trace.current;
  const technicalStatus = current?.technical_status ? escapeHtml(JSON.stringify(current.technical_status, null, 2)) : escapeHtml(copy.unset);
  const authSummary = current?.auth_summary ? escapeHtml(JSON.stringify(current.auth_summary, null, 2)) : escapeHtml(copy.unset);
  const dsn = current?.dsn ? escapeHtml(JSON.stringify(current.dsn, null, 2)) : "";
  const historyItems = (trace.history ?? [])
    .map(
      (item) => `
        <div class="trace-item">
          <strong>${escapeHtml(item.timestamp)}</strong>
          <p>${escapeHtml(`${item.queue} · ${item.status}`)}</p>
          <p>${escapeHtml(item.reason || item.route_target || item.peer || copy.unset)}</p>
        </div>
      `,
    )
    .join("");
  const decisionItems = (current?.decision_trace ?? [])
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
  openDrawer(
    current?.subject || trace.trace_id,
    `${current?.mail_from || copy.unset} -> ${formatList(current?.rcpt_to ?? [])}`,
    `
      <div class="record-actions">
        ${
          current
            ? `<button class="list-action" type="button" data-action="trace-retry" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRetry}</button>`
            : ""
        }
        ${
          current?.queue === "quarantine" || current?.queue === "held"
            ? `<button class="list-action" type="button" data-action="trace-release" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRelease}</button>`
            : ""
        }
        ${
          current?.queue === "quarantine"
            ? `<button class="list-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceDelete}</button>`
            : ""
        }
      </div>
      <section class="trace-section">
        <h4>${copy.traceSummaryTitle}</h4>
        <div class="summary-grid">
          <div><p>Trace</p><span class="record-copy">${escapeHtml(trace.trace_id)}</span></div>
          <div><p>Status</p><span class="record-copy">${escapeHtml(current?.status || copy.unset)}</span></div>
          <div><p>Queue</p><span class="record-copy">${escapeHtml(current?.queue || copy.unset)}</span></div>
          <div><p>Route</p><span class="record-copy">${escapeHtml(current?.route?.relay_target || copy.unset)}</span></div>
          <div><p>Spam</p><span class="record-copy">${escapeHtml(current?.spam_score ?? copy.unset)}</span></div>
          <div><p>Security</p><span class="record-copy">${escapeHtml(current?.security_score ?? copy.unset)}</span></div>
        </div>
      </section>
      <div class="trace-columns">
        <section class="trace-section">
          <h4>${copy.tracePolicyTitle}</h4>
          <div class="trace-list">${decisionItems || `<div class="trace-item"><p>${copy.unset}</p></div>`}</div>
        </section>
        <section class="trace-section">
          <h4>${copy.traceTechnicalTitle}</h4>
          <div class="trace-list">
            <div class="trace-item"><strong>Auth</strong><pre>${authSummary}</pre></div>
            <div class="trace-item"><strong>Technical</strong><pre>${technicalStatus}</pre></div>
            ${dsn ? `<div class="trace-item"><strong>DSN</strong><pre>${dsn}</pre></div>` : ""}
          </div>
        </section>
      </div>
      <div class="trace-columns">
        <section class="trace-section">
          <h4>${copy.traceHeadersTitle}</h4>
          <div class="trace-list">
            ${(current?.headers ?? [])
              .map(
                ([name, value]) => `
                  <div class="trace-item">
                    <strong>${escapeHtml(name)}</strong>
                    <p>${escapeHtml(value)}</p>
                  </div>
                `,
              )
              .join("") || `<div class="trace-item"><p>${copy.unset}</p></div>`}
          </div>
        </section>
        <section class="trace-section">
          <h4>${copy.traceBodyTitle}</h4>
          <pre>${escapeHtml(current?.body_excerpt || copy.unset)}</pre>
        </section>
      </div>
      <section class="trace-section">
        <h4>${copy.traceHistoryTitle}</h4>
        <div class="trace-list">${historyItems || `<div class="trace-item"><p>${copy.traceNoHistory}</p></div>`}</div>
      </section>
    `,
  );
}

async function loadTrace(traceId) {
  state.selectedTrace = await fetchJson(`/api/history/${traceId}`);
  renderTraceDrawer(state.selectedTrace);
}

async function triggerTraceAction(traceId, action) {
  const copy = getCopy();
  await fetchJson(`/api/traces/${traceId}/${action}`, { method: "POST" });
  showFeedback(translate(copy.traceActionCompleted, { traceId }), false);
  await loadOps();
  try {
    await loadTrace(traceId);
  } catch {
    closeDrawer();
  }
}

async function openDigestReport(reportId) {
  const copy = getCopy();
  const report = await fetchJson(`/api/reporting/digests/${reportId}`);
  openDrawer(
    report.summary.scope_label,
    `${report.summary.generated_at} · ${report.summary.recipient}`,
    `<section class="trace-section"><pre class="digest-content">${escapeHtml(report.content)}</pre></section>`,
  );
}

async function loadOps() {
  const quarantineParams = new URLSearchParams(new FormData(document.getElementById("quarantine-search-form")));
  const historyParams = new URLSearchParams(new FormData(document.getElementById("history-search-form")));
  const [quarantine, history, routes, reporting, digestReports, policyStatus] = await Promise.all([
    fetchJson(`/api/quarantine?${quarantineParams.toString()}`),
    fetchJson(`/api/history?${historyParams.toString()}`),
    fetchJson("/api/routes/diagnostics"),
    fetchJson("/api/reporting"),
    fetchJson("/api/reporting/digests"),
    fetchJson("/api/policies/status"),
  ]);
  state.quarantine = quarantine;
  state.history = history.items ?? [];
  state.routeDiagnostics = routes;
  state.reporting = reporting;
  state.digestReports = digestReports;
  state.policyStatus = policyStatus;
  renderDashboard();
}

async function load() {
  try {
    state.dashboard = await fetchDashboard();
    await loadOps();
    loginShell.classList.add("hidden");
    consoleShell.classList.remove("hidden");
    hideFeedback();
  } catch (error) {
    if (error instanceof Error && error.message.includes("401")) {
      window.localStorage.removeItem(AUTH_TOKEN_KEY);
      consoleShell.classList.add("hidden");
      loginShell.classList.remove("hidden");
      showLoginFeedback(getCopy().authRequired, true);
      return;
    }
    showFeedback(error instanceof Error ? error.message : getCopy().unknownError, true);
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
    if (response.status === 502) {
      throw new Error(getCopy().login502);
    }
    throw new Error(`login request failed: ${response.status}`);
  }
  const body = await response.json();
  window.localStorage.setItem(AUTH_TOKEN_KEY, body.token);
  if (typeof payload.email === "string" && payload.email.trim()) {
    window.localStorage.setItem(LAST_ADMIN_EMAIL_KEY, payload.email.trim());
  }
  showLoginFeedback(getCopy().authenticated, false);
  await load();
}

document.getElementById("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void loginAdmin().catch((error) => {
    showLoginFeedback(error instanceof Error ? error.message : getCopy().unknownError, true);
  });
});

document.getElementById("refresh").addEventListener("click", () => {
  void load();
});

document.getElementById("refresh-toolbar").addEventListener("click", () => {
  void load();
});

document.getElementById("run-digests").addEventListener("click", async () => {
  const copy = getCopy();
  try {
    const result = await fetchJson("/api/reporting/digests/run", { method: "POST" });
    showFeedback(translate(copy.digestGeneratedSummary, { count: result.generated_reports?.length ?? 0 }), false);
    await loadOps();
  } catch (error) {
    showFeedback(error instanceof Error ? error.message : copy.unknownError, true);
  }
});

document.getElementById("quarantine-search-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void loadOps();
});

document.getElementById("history-search-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void loadOps();
});

document.getElementById("create-address-rule").addEventListener("click", () => openAddressRuleDrawer());
document.getElementById("create-attachment-rule").addEventListener("click", () => openAttachmentRuleDrawer());
document.getElementById("edit-recipient-verification").addEventListener("click", () => openRecipientVerificationDrawer());
document.getElementById("edit-dkim-settings").addEventListener("click", () => openDkimSettingsDrawer());
document.getElementById("create-dkim-domain").addEventListener("click", () => openDkimDomainDrawer());
document.getElementById("edit-digest-settings").addEventListener("click", () => openDigestSettingsDrawer());
document.getElementById("create-digest-default").addEventListener("click", () => openDigestDefaultDrawer());
document.getElementById("create-digest-override").addEventListener("click", () => openDigestOverrideDrawer());

document.body.addEventListener("click", (event) => {
  const actionTarget = event.target.closest("[data-action]");
  if (actionTarget) {
    const { action, traceId, ruleId, index, reportId, target } = actionTarget.dataset;
    if (action === "trace-open") {
      void loadTrace(traceId);
    } else if (action === "trace-release") {
      void triggerTraceAction(traceId, "release");
    } else if (action === "trace-delete") {
      void triggerTraceAction(traceId, "delete");
    } else if (action === "trace-retry") {
      void triggerTraceAction(traceId, "retry");
    } else if (action === "address-edit") {
      openAddressRuleDrawer(ruleId);
    } else if (action === "address-delete") {
      void deleteAddressRule(ruleId);
    } else if (action === "attachment-edit") {
      openAttachmentRuleDrawer(ruleId);
    } else if (action === "attachment-delete") {
      void deleteAttachmentRule(ruleId);
    } else if (action === "dkim-domain-edit") {
      openDkimDomainDrawer(Number(index));
    } else if (action === "dkim-domain-delete") {
      void deleteDkimDomain(Number(index));
    } else if (action === "digest-default-edit") {
      openDigestDefaultDrawer(Number(index));
    } else if (action === "digest-default-delete") {
      void deleteDigestDefault(Number(index));
    } else if (action === "digest-override-edit") {
      openDigestOverrideDrawer(Number(index));
    } else if (action === "digest-override-delete") {
      void deleteDigestOverride(Number(index));
    } else if (action === "digest-open") {
      void openDigestReport(reportId);
    } else if (action === "platform-edit") {
      openPlatformDrawer(target);
    }
    return;
  }

  const scrollTarget = event.target.closest("[data-scroll-target]");
  if (scrollTarget) {
    document.getElementById(scrollTarget.dataset.scrollTarget)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

document.getElementById("drawer-close").addEventListener("click", closeDrawer);
drawerBackdrop.addEventListener("click", (event) => {
  if (event.target === drawerBackdrop) {
    closeDrawer();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !drawerBackdrop.classList.contains("hidden")) {
    closeDrawer();
  }
});

i18n.bindLocalePickers(localePickers, setLocale);
scrollButtons.forEach((button) => button.classList.remove("is-active"));
setLocale(i18n.getLocale());
syncLoadingState();
void load();
