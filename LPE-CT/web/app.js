import { i18n, getCopy, translate } from './modules/i18n/index.js?v=20260502-outbound-ehlo';
import { DEFAULT_PAGE_ID, activatePageView, pageIdFromHash, renderPageModules } from "./modules/pages/index.js?v=20260501-health-check-output";
import { elements, containers, AUTH_TOKEN_KEY, LAST_ADMIN_EMAIL_KEY, DASHBOARD_REFRESH_INTERVAL_MS, DEFAULT_QUARANTINE_COLUMN_WIDTHS, DEFAULT_HISTORY_COLUMN_WIDTHS, MIN_HISTORY_COLUMN_WIDTH, DEFAULT_LOG_COLUMN_WIDTHS, state } from "./modules/app/context.js?v=20260502-outbound-ehlo";
import { escapeHtml, formatList, isValidHostname, parseProviderChain, antivirusProviderChain, labelForAntivirusProvider, formatAntivirusProviders, formatNumber, formatScore, formatDetailedScore, formatDateTime, parseHistoryTimestamp, formatHistoryDateTime, displayTraceId, displayClientAddress, displayMailAddress, historySizeBytes, formatLongTraceDateTime, traceHeaderValue, traceHeadersText, traceTextValue, traceObjectValue, traceContentClassification, traceBooleanLabel, tracePolicyFlag, traceMessageSize, traceAttachmentItems, formatShortDate, formatMetric, formatPercent, formatBytes, formatCompactBytes, firstRecipient, humanizeStatus, formatHistoryType, historyColumns, quarantineTraceId, quarantineDate, quarantineScoreValue, traceQueueCanBeDeleted, quarantineColumns, quarantineGridTemplate, historyGridTemplate, sortQuarantineItems, sortHistoryItems, quarantineSortIndicator, sortIndicator, setQuarantineSort, setHistorySort, logTableState, logGridTemplate, sortLogItems, logSortIndicator, setLogSort, renderLogTable, auditColumns, messageLogColumns, emailAlertLogColumns, hostLogDate, hostLogColumns, hostLogActionButton, renderHostLogTable, formatDurationMinutes, formatUptime, formatReportingUptime, formatBooleanLabel, healthPosture, getOperatorEmail, getDigestSettings, getTrafficRecords, getRelayOrPeer, getPolicySignals, dedupeList, currentPolicies, currentReporting, statusChipClass, labelForAddressRole, labelForAction, labelForAttachmentScope, labelForVerificationBackend, labelForKeyStatus } from "./modules/app/format.js?v=20260502-outbound-ehlo";
import { showFeedback, showLoginFeedback, hideFeedback, setButtonBusy, setSidebarOpen, setSidebarCollapsed, toggleSidebarState, buildEmptyState, buildLoadingRows, setListLoading, clearInvalidFields, markInvalid, renderDrawerContent, closeDrawer, renderMetric, setText, setClassName, setAuthenticated } from "./modules/app/ui.js?v=20260502-outbound-ehlo";
import { authHeaders, parseError, fetchJson, fetchOptionalJson, fetchBlob, fetchDashboard, putJson, postJson } from "./modules/app/api.js?v=20260502-outbound-ehlo";
import { renderSystemInformation, renderPlatform, renderMailLog, renderAudit, renderMessageLog, renderEmailAlertLog, renderLogTableById, publicTlsSettings, getRuntimeSystem, getHostClockDate, renderHostClock } from "./modules/app/system.js?v=20260502-outbound-ehlo";
import { routeToPolicies, routeToAttachmentPolicies, getAddressRules, getAttachmentRules, findAddressRule, findAttachmentRule, selectedQuarantineItems, pruneQuarantineSelection, renderQuarantine, renderHistory, renderFilteringPolicy, renderVirusFiltering, renderAddressRules, renderAttachmentRules, renderRecipientVerification, renderDkim, renderDigestDefaults, renderDigestOverrides, renderDigestReportsList, renderDigestReporting } from "./modules/app/lists.js?v=20260502-outbound-ehlo";
import { renderOverview } from "./modules/app/dashboard.js?v=20260502-outbound-ehlo";
import { configureTraceActions, openHostLog, downloadHostLog, deleteHostLog, loadTrace, loadQuarantineTrace, setQuarantineDialogTab, triggerTraceAction, runQuarantineBulkAction, openDigestReport, openDiagnostic, runHealthCheck, connectSupport, flushMailQueue, runDiagnosticTool, runSpamTest, runServiceAction } from "./modules/app/trace-actions.js?v=20260502-outbound-ehlo";
import { configurePolicyDrawers, renderDrawerForm, normalizeDomain, normalizeEmail, parseLines, isValidEmail, isValidDomain, openAddressRuleDrawer, deleteAddressRule, openAttachmentRuleDrawer, deleteAttachmentRule, openFilteringPolicyDrawer, openVirusFilteringDrawer, openRecipientVerificationDrawer, openDkimSettingsDrawer, openDkimDomainDrawer, deleteDkimDomain, openDigestSettingsDrawer, openDigestDefaultDrawer, deleteDigestDefault, openDigestOverrideDrawer, deleteDigestOverride } from "./modules/app/policy-drawers.js?v=20260502-outbound-ehlo";

function syncLoadingState() {
  const copy = getCopy();
  if (!state.dashboard) {
    setText(elements.nodeName, copy.heroLoadingTitle);
    setText(elements.heroSummary, copy.heroLoadingSummary);
    setText(elements.contextOperator, copy.unset);
    setText(elements.contextRole, copy.operatorRole);
    setText(elements.contextVersion, copy.unset);
    setText(elements.contextLicense, "Apache-2.0");
    setText(elements.contextBuild, copy.unset);
    renderHostClock();
    setText(elements.heroPrimaryRelay, copy.unset);
    setText(elements.heroRouteSummary, copy.unset);
    setText(elements.heroReportingSummary, copy.unset);
    setText(elements.heroReportingCopy, copy.unset);
    setText(elements.metricSystemHealth, "-");
    renderOverview();
    Object.values(containers).forEach((container) => setListLoading(container));
    return;
  }
  renderDashboard();
}

function pageFromHash() {
  return pageIdFromHash(window.location?.hash);
}

function syncPageTabs(activePage = state.activePage) {
  elements.pageTabPanels.forEach((panel) => {
    const [pageId, tabId] = String(panel.dataset.pageTabPanel ?? "").split(":");
    const isActive = pageId === activePage && state.pageTabs[pageId] === tabId;
    panel.classList.toggle("hidden", !isActive);
    panel.classList.toggle("page-view-active", isActive);
    panel.setAttribute("aria-hidden", String(!isActive));
  });
  elements.pageTabButtons.forEach((button) => {
    const pageId = button.dataset.tabPage;
    const tabId = button.dataset.tabId;
    const isActive = pageId === activePage && state.pageTabs[pageId] === tabId;
    button.classList.toggle("tab-button-active", isActive);
    button.setAttribute("aria-selected", String(isActive));
  });
}

function setActivePage(page = state.activePage, options = {}) {
  const targetPage = activatePageView(page, {
    pageViews: elements.pageViews,
    navButtons: elements.navButtons,
  });
  state.activePage = targetPage;
  syncPageTabs(targetPage);
  if (options.updateHash) {
    try {
      window.history?.replaceState(null, "", `#${targetPage}`);
    } catch {}
  }
  if (options.focus) {
    elements.mainWorkspace?.focus();
  }
}

function updateNavState(activePage = state.activePage) {
  setActivePage(activePage);
}

function registerSectionObserver() {
  setActivePage(pageFromHash());
}

const PAGE_RENDERERS = {
  overview: renderOverview,
  quarantine: renderQuarantine,
  history: renderHistory,
  virusFiltering: renderVirusFiltering,
  filteringPolicy: renderFilteringPolicy,
  addressRules: renderAddressRules,
  attachmentRules: renderAttachmentRules,
  recipientVerification: renderRecipientVerification,
  dkim: renderDkim,
  systemInformation: renderSystemInformation,
  digestReporting: renderDigestReporting,
  platform: renderPlatform,
  mailLog: renderMailLog,
  audit: renderAudit,
  messageLog: renderMessageLog,
  emailAlertLog: renderEmailAlertLog,
};

function renderDashboard() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    syncLoadingState();
    return;
  }
  const posture = healthPosture(dashboard);

  setText(elements.nodeName, dashboard.site?.node_name || copy.heroLoadingTitle);
  setText(elements.heroSummary, translate(copy.heroSummaryTemplate, {
    dmzZone: dashboard.site?.dmz_zone || copy.unset,
    publishedMx: dashboard.site?.published_mx || copy.unset,
    primaryUpstream: dashboard.relay?.primary_upstream || copy.unset,
  }));
  setText(elements.statusBadge, posture.label);
  setClassName(elements.statusBadge, `badge ${posture.tone}`);
  setText(elements.upstreamBadge, dashboard.queues?.upstream_reachable ? copy.relayReachable : copy.relayUnreachable);
  setClassName(elements.upstreamBadge, dashboard.queues?.upstream_reachable ? "badge ok" : "badge danger");

  setText(elements.metricSystemHealth, posture.label);
  renderMetric(elements.metricInbound, dashboard.queues?.inbound_messages);
  renderMetric(elements.metricDeferred, dashboard.queues?.deferred_messages);
  renderMetric(elements.metricQuarantine, dashboard.queues?.quarantined_messages);
  renderMetric(elements.metricAttempts, dashboard.queues?.delivery_attempts_last_hour);
  renderOverview();
  renderSystemInformation();

  renderPageModules(PAGE_RENDERERS);
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

async function syncNtp() {
  const copy = getCopy();
  showFeedback(copy.actionInProgress, "warning");
  const result = await postJson("/api/system-time/sync");
  await loadOps({ silent: true });
  renderPlatform();
  showFeedback(result?.detail || copy.ntpSyncRequested, result?.status === "failed" ? "warning" : "success");
}

async function runAptUpgrade() {
  const copy = getCopy();
  showFeedback(copy.aptUpgradeRunning, "warning");
  const result = await postJson("/api/system-updates/apt-upgrade");
  await loadOps({ silent: true });
  renderPlatform();
  showFeedback(result?.detail || copy.aptUpgradeComplete, result?.status === "failed" ? "warning" : "success");
}

async function runPowerAction(action) {
  const copy = getCopy();
  const isShutdown = action === "shutdown";
  const message = isShutdown ? copy.shutdownConfirm : copy.restartConfirm;
  if (!(window.confirm?.(message) ?? false)) {
    return;
  }
  const result = await postJson(`/api/system-power/${encodeURIComponent(action)}`);
  showFeedback(result?.detail || copy.powerActionRequested, "warning");
}

function currentAcceptedDomains() {
  return structuredClone(state.dashboard?.accepted_domains ?? []);
}

function findAcceptedDomain(domainId) {
  return currentAcceptedDomains().find((domain) => domain.id === domainId) ?? null;
}

function acceptedDomainPayloadFromForm(form) {
  return {
    domain: normalizeDomain(form.elements.namedItem("domain").value),
    destination_server: String(form.elements.namedItem("destination_server").value).trim(),
    verification_type: form.elements.namedItem("verification_type").value,
    rbl_checks: form.elements.namedItem("rbl_checks").checked,
    spf_checks: form.elements.namedItem("spf_checks").checked,
    greylisting: form.elements.namedItem("greylisting").checked,
    accept_null_reverse_path: form.elements.namedItem("accept_null_reverse_path").checked,
    verified: form.elements.namedItem("verified").checked,
  };
}

function validateAcceptedDomainPayload(payload, existingId = null) {
  const copy = getCopy();
  const errors = [];
  if (!isValidDomain(payload.domain)) {
    errors.push({ field: "domain", message: copy.validationDomain });
  }
  if (!payload.destination_server) {
    errors.push({ field: "destination_server", message: copy.validationDestinationServer });
  }
  if (currentAcceptedDomains().some((domain) => domain.id !== existingId && normalizeDomain(domain.domain) === payload.domain)) {
    errors.push({ field: "domain", message: copy.validationDuplicateDomain });
  }
  return errors;
}

function openAcceptedDomainDrawer(domainId = null, opener = document.activeElement) {
  const copy = getCopy();
  const domain = domainId
    ? findAcceptedDomain(domainId)
    : {
        domain: "",
        destination_server: "",
        verification_type: "none",
        rbl_checks: true,
        spf_checks: true,
        greylisting: true,
        accept_null_reverse_path: true,
        verified: false,
      };
  if (!domain) {
    showFeedback(copy.acceptedDomainNotFound, "error");
    return;
  }
  renderDrawerForm({
    title: domainId ? copy.edit : copy.addAcceptedDomain,
    summary: copy.systemSetupRelayDomainsSummary,
    formId: "accepted-domain-form",
    opener,
    content: `
      <div class="field-grid">
        <label>
          <span>${copy.acceptedDomainColumnDomain}</span>
          <input name="domain" required value="${escapeHtml(domain.domain)}" />
        </label>
        <label>
          <span>${copy.acceptedDomainColumnDestination}</span>
          <input name="destination_server" required value="${escapeHtml(domain.destination_server)}" />
        </label>
        <label class="field-span-full">
          <span>${copy.acceptedDomainColumnVerification}</span>
          <select name="verification_type">
            <option value="none"${domain.verification_type === "none" ? " selected" : ""}>${copy.acceptedDomainVerificationNone}</option>
            <option value="dynamic"${domain.verification_type === "dynamic" ? " selected" : ""}>${copy.acceptedDomainVerificationDynamic}</option>
            <option value="ldap"${domain.verification_type === "ldap" ? " selected" : ""}>${copy.acceptedDomainVerificationLdap}</option>
            <option value="allowed"${domain.verification_type === "allowed" ? " selected" : ""}>${copy.acceptedDomainVerificationAllowed}</option>
          </select>
        </label>
        <label class="toggle-field"><span>${copy.acceptedDomainColumnRbl}</span><input name="rbl_checks" type="checkbox"${domain.rbl_checks ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.acceptedDomainColumnSpf}</span><input name="spf_checks" type="checkbox"${domain.spf_checks ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.acceptedDomainColumnGreylisting}</span><input name="greylisting" type="checkbox"${domain.greylisting ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.acceptedDomainColumnNullReversePath}</span><input name="accept_null_reverse_path" type="checkbox"${(domain.accept_null_reverse_path ?? true) ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.acceptedDomainColumnVerified}</span><input name="verified" type="checkbox"${domain.verified ? " checked" : ""} /></label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${domainId ? copy.save : copy.create}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const payload = acceptedDomainPayloadFromForm(form);
      const errors = validateAcceptedDomainPayload(payload, domainId);
      if (errors.length) {
        context.fail(errors);
      }
      const path = domainId ? `/api/accepted-domains/${encodeURIComponent(domainId)}` : "/api/accepted-domains";
      const saved = domainId ? await putJson(path, payload) : await postJson(path, payload);
      const domains = currentAcceptedDomains().filter((item) => item.id !== saved.id);
      domains.push(saved);
      domains.sort((left, right) => left.domain.localeCompare(right.domain));
      state.dashboard.accepted_domains = domains;
      renderPlatform();
      closeDrawer();
      showFeedback(domainId ? copy.recordSaved : copy.recordCreated);
    },
  });
}

function openAcceptedDomainImportDrawer(opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerForm({
    title: copy.importAcceptedDomains,
    summary: copy.acceptedDomainImportSummary,
    formId: "accepted-domain-import-form",
    opener,
    content: `
      <label>
        <span>${copy.importAcceptedDomains}</span>
        <textarea name="domains" rows="10" placeholder="${escapeHtml(copy.acceptedDomainImportPlaceholder)}"></textarea>
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.import}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const rows = parseLines(form.elements.namedItem("domains").value);
      const domains = rows.map((row) => {
        const [
          domain,
          destinationServer,
          verificationType = "none",
          rbl = "yes",
          spf = "yes",
          greylisting = "yes",
          verified = "no",
          nullReversePath = "yes",
        ] = row
          .split(",")
          .map((value) => value.trim());
        return {
          domain: normalizeDomain(domain),
          destination_server: destinationServer,
          verification_type: verificationType.toLowerCase(),
          rbl_checks: /^(yes|true|1|on)$/i.test(rbl),
          spf_checks: /^(yes|true|1|on)$/i.test(spf),
          greylisting: /^(yes|true|1|on)$/i.test(greylisting),
          accept_null_reverse_path: /^(yes|true|1|on)$/i.test(nullReversePath),
          verified: /^(yes|true|1|on)$/i.test(verified),
        };
      });
      const errors = [];
      if (!domains.length) {
        errors.push({ field: "domains", message: copy.validationImportDomains });
      }
      domains.forEach((domain) => {
        errors.push(...validateAcceptedDomainPayload(domain).map((error) => ({ ...error, field: "domains" })));
      });
      if (errors.length) {
        context.fail(errors);
      }
      state.dashboard.accepted_domains = await postJson("/api/accepted-domains/import", { domains });
      renderPlatform();
      closeDrawer();
      showFeedback(copy.recordCreated);
    },
  });
}

async function deleteAcceptedDomain(domainId) {
  const copy = getCopy();
  await fetchJson(`/api/accepted-domains/${encodeURIComponent(domainId)}`, { method: "DELETE" });
  state.dashboard.accepted_domains = currentAcceptedDomains().filter((domain) => domain.id !== domainId);
  renderPlatform();
  showFeedback(copy.recordDeleted);
}

async function testAcceptedDomain(domainId) {
  const copy = getCopy();
  const result = await postJson(`/api/accepted-domains/${encodeURIComponent(domainId)}/test`);
  if (result.verified) {
    state.dashboard.accepted_domains = currentAcceptedDomains().map((domain) =>
      domain.id === domainId ? { ...domain, verified: true } : domain
    );
    renderPlatform();
  }
  showFeedback(`${result.domain}: ${result.detail}`, result.verified ? "success" : "warning");
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
        <label><span>${copy.relayPrimaryLabel}</span><input name="primary_upstream" value="${escapeHtml(dashboard.relay.primary_upstream)}" /></label>
        <label><span>${copy.relaySecondaryLabel}</span><input name="secondary_upstream" value="${escapeHtml(dashboard.relay.secondary_upstream)}" /></label>
        <label><span>${copy.relayOutboundEhloLabel}</span><input name="outbound_ehlo_name" required placeholder="mx.example.com" value="${escapeHtml(dashboard.relay.outbound_ehlo_name)}" /></label>
        <label><span>${copy.relayCoreDeliveryLabel}</span><input name="core_delivery_base_url" value="${escapeHtml(dashboard.relay.core_delivery_base_url)}" /></label>
        <label class="toggle-field"><span>${copy.relayMutualTlsLabel}</span><input name="mutual_tls_required" type="checkbox"${dashboard.relay.mutual_tls_required ? " checked" : ""} /></label>
        <label class="toggle-field"><span>${copy.relayFallbackLabel}</span><input name="fallback_to_hold_queue" type="checkbox"${dashboard.relay.fallback_to_hold_queue ? " checked" : ""} /></label>
        <label><span>${copy.relaySyncLabel}</span><input name="sync_interval_seconds" type="number" min="1" value="${escapeHtml(dashboard.relay.sync_interval_seconds)}" /></label>
        <label><span>${copy.relayDependencyLabel}</span><textarea name="lan_dependency_note" rows="4">${escapeHtml(dashboard.relay.lan_dependency_note)}</textarea></label>
      `,
      payload: (form) => ({
        primary_upstream: form.elements.namedItem("primary_upstream").value.trim(),
        secondary_upstream: form.elements.namedItem("secondary_upstream").value.trim(),
        outbound_ehlo_name: form.elements.namedItem("outbound_ehlo_name").value.trim(),
        core_delivery_base_url: form.elements.namedItem("core_delivery_base_url").value,
        mutual_tls_required: form.elements.namedItem("mutual_tls_required").checked,
        fallback_to_hold_queue: form.elements.namedItem("fallback_to_hold_queue").checked,
        sync_interval_seconds: Number(form.elements.namedItem("sync_interval_seconds").value),
        lan_dependency_note: form.elements.namedItem("lan_dependency_note").value,
      }),
      validate: (form) => {
        const errors = [];
        const syncInterval = Number(form.elements.namedItem("sync_interval_seconds").value);
        if (!Number.isInteger(syncInterval) || syncInterval < 1) {
          errors.push({ field: "sync_interval_seconds", message: copy.validationPositiveInteger });
        }
        const ehloName = form.elements.namedItem("outbound_ehlo_name").value.trim();
        if (!isValidHostname(ehloName)) {
          errors.push({ field: "outbound_ehlo_name", message: copy.validationFqdn });
        }
        return errors;
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
        public_tls: publicTlsSettings(),
      }),
      validate: (form) => {
        const value = Number(form.elements.namedItem("max_concurrent_sessions").value);
        return !Number.isInteger(value) || value < 1 ? [{ field: "max_concurrent_sessions", message: copy.validationPositiveInteger }] : [];
      },
    },
    time: {
      title: copy.systemSetupTime,
      summary: copy.systemSetupTimeSummary,
      submitPath: "/api/system-time/ntp",
      content: `
        <label class="toggle-field"><span>${copy.ntpEnabledLabel}</span><input name="enabled" type="checkbox"${dashboard.system?.ntp?.enabled ? " checked" : ""} /></label>
        <label><span>${copy.ntpServersLabel}</span><textarea name="servers" rows="5">${escapeHtml((dashboard.system?.ntp?.servers ?? []).join("\n"))}</textarea></label>
      `,
      payload: (form) => ({
        enabled: form.elements.namedItem("enabled").checked,
        servers: parseLines(form.elements.namedItem("servers").value),
      }),
      validate: (form) => {
        const enabled = form.elements.namedItem("enabled").checked;
        const servers = parseLines(form.elements.namedItem("servers").value);
        return enabled && !servers.length ? [{ field: "servers", message: copy.validationNtpServers }] : [];
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
      const result = await putJson(config.submitPath, config.payload(form));
      if (result?.site && result?.network) {
        state.dashboard = result;
      }
      await loadOps({ silent: true });
      closeDrawer();
      showFeedback(result?.detail || copy.recordSaved, result?.status === "failed" ? "warning" : "success");
    },
  });
}

async function readSelectedTextFile(input) {
  const file = input.files?.[0];
  if (!file) {
    return "";
  }
  return file.text();
}

function openPublicTlsUploadDrawer(opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerForm({
    title: copy.publicTlsUploadAction,
    summary: copy.publicTlsUploadSummary,
    formId: "public-tls-upload-form",
    opener,
    content: `
      <label>
        <span>${copy.publicTlsProfileNameLabel}</span>
        <input name="name" required value="" />
      </label>
      <label>
        <span>${copy.publicTlsCertificateFileLabel}</span>
        <input name="certificate_pem" type="file" accept=".pem,.crt,.cer" required />
      </label>
      <label>
        <span>${copy.publicTlsPrivateKeyFileLabel}</span>
        <input name="private_key_pem" type="file" accept=".pem,.key" required />
      </label>
      <label class="toggle-field">
        <span>${copy.publicTlsActivateLabel}</span>
        <input name="activate" type="checkbox" checked />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.publicTlsUploadAction}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const name = String(form.elements.namedItem("name").value).trim();
      const certificateInput = form.elements.namedItem("certificate_pem");
      const privateKeyInput = form.elements.namedItem("private_key_pem");
      const errors = [];
      if (!name) {
        errors.push({ field: "name", message: copy.validationPublicTlsName });
      }
      if (!certificateInput.files?.length) {
        errors.push({ field: "certificate_pem", message: copy.validationPublicTlsCertificate });
      }
      if (!privateKeyInput.files?.length) {
        errors.push({ field: "private_key_pem", message: copy.validationPublicTlsKey });
      }
      if (errors.length) {
        context.fail(errors);
      }
      state.dashboard = await postJson("/api/public-tls/profiles", {
        name,
        certificate_pem: await readSelectedTextFile(certificateInput),
        private_key_pem: await readSelectedTextFile(privateKeyInput),
        activate: form.elements.namedItem("activate").checked,
      });
      await loadOps({ silent: true });
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

async function selectPublicTlsProfile(profileId) {
  state.dashboard = await putJson("/api/public-tls/select", { profile_id: profileId });
  await loadOps({ silent: true });
  showFeedback(getCopy().recordSaved);
}

async function disablePublicTlsProfile() {
  state.dashboard = await putJson("/api/public-tls/select", { profile_id: null });
  await loadOps({ silent: true });
  showFeedback(getCopy().recordSaved);
}

async function deletePublicTlsProfile(profileId) {
  await fetchJson(`/api/public-tls/profiles/${encodeURIComponent(profileId)}`, { method: "DELETE" });
  state.dashboard = await fetchDashboard();
  await loadOps({ silent: true });
  showFeedback(getCopy().recordDeleted);
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
    const [quarantine, history, routes, reporting, digestReports, policyStatus, systemServices, mailLogs, interfaceLogs, messageLogs] = await Promise.all([
      fetchJson(`/api/quarantine?${quarantineParams.toString()}`),
      fetchJson(`/api/history?${historyParams.toString()}`),
      fetchJson("/api/routes/diagnostics"),
      fetchJson("/api/reporting"),
      fetchJson("/api/reporting/digests"),
      fetchJson("/api/policies/status"),
      fetchOptionalJson("/api/system-diagnostics/services", { items: [] }),
      fetchJson("/api/host-logs/mail"),
      fetchJson("/api/host-logs/interface"),
      fetchJson("/api/host-logs/messages"),
    ]);
    state.quarantine = quarantine ?? [];
    pruneQuarantineSelection();
    state.history = history?.items ?? [];
    state.routeDiagnostics = routes;
    state.reporting = reporting;
    state.digestReports = digestReports ?? [];
    state.policyStatus = policyStatus;
    state.systemServices = systemServices?.items ?? [];
    state.hostLogs.mail = mailLogs?.items ?? [];
    state.hostLogs.interface = interfaceLogs?.items ?? [];
    state.hostLogs.messages = messageLogs?.items ?? [];
    renderDashboard();
  } finally {
    state.loading.ops = false;
    setButtonBusy(elements.refreshToolbar, false, copy.refreshing, copy.refreshState);
    setButtonBusy(elements.refresh, false, copy.refreshing, copy.refresh);
  }
}

configureTraceActions({ loadOps, savePolicies });
configurePolicyDrawers({ savePolicies, saveReporting });

async function load({ silent = false } = {}) {
  if (state.loading.dashboard) {
    return;
  }
  const copy = getCopy();
  state.loading.dashboard = true;
  if (!silent) {
    setButtonBusy(elements.refreshToolbar, true, copy.refreshing, copy.refreshState);
    setButtonBusy(elements.refresh, true, copy.refreshing, copy.refresh);
    syncLoadingState();
  }
  try {
    state.dashboard = await fetchDashboard();
    state.hostClockLoadedAt = Date.now();
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
    if (!silent) {
      showFeedback(error instanceof Error ? error.message : copy.unknownError, "error");
    }
  } finally {
    state.loading.dashboard = false;
    if (!silent) {
      setButtonBusy(elements.refreshToolbar, false, copy.refreshing, copy.refreshState);
      setButtonBusy(elements.refresh, false, copy.refreshing, copy.refresh);
    }
  }
}

function refreshDashboardOnSchedule() {
  if (!window.localStorage.getItem(AUTH_TOKEN_KEY) || state.loading.auth || state.loading.dashboard) {
    return;
  }
  void load({ silent: true });
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

function setPageTab(actionTarget) {
  const { tabPage, tabId } = actionTarget.dataset;
  if (!tabPage || !tabId || !Object.prototype.hasOwnProperty.call(state.pageTabs, tabPage)) {
    return;
  }
  state.pageTabs[tabPage] = tabId;
  syncPageTabs(state.activePage);
}

function setSystemSetupTab(actionTarget) {
  const { tabId, tabLevel } = actionTarget.dataset;
  const primaryTabs = new Set(["network", "time", "mailRelay", "mailAuthentication", "systemUpdates", "shutdownRestart"]);
  const nestedTabs = {
    network: new Set(["ip", "dns", "static-routes", "ipv6"]),
    mailRelay: new Set([
      "general",
      "domains",
      "ip-controls",
      "sender-controls",
      "outbound",
      "smtp-settings",
      "esmtp-settings",
      "greylisting",
      "dkim-signing",
    ]),
    mailAuthentication: new Set(["spf", "dkim", "dmarc", "arc"]),
  };
  if (tabLevel === "primary" && primaryTabs.has(tabId)) {
    state.systemSetup.primaryTab = tabId;
    renderPlatform();
    return;
  }
  const primaryTab = state.systemSetup.primaryTab;
  if (tabLevel === "secondary" && nestedTabs[primaryTab]?.has(tabId)) {
    state.systemSetup.nestedTabs[primaryTab] = tabId;
    renderPlatform();
  }
}

function getActionHandlers(actionTarget) {
  const { traceId, ruleId, index, reportId, target, domainId, profileId, sortKey, logTable, logCategory, logId, bulkAction, tabId, diagnosticKind, diagnosticTool, serviceId, serviceAction, powerAction } = actionTarget.dataset;
  return {
    "drawer-close": () => closeDrawer(),
    "trace-open": () => runAction(() => loadTrace(traceId, actionTarget)),
    "quarantine-open": () => runAction(() => loadQuarantineTrace(traceId, actionTarget)),
    "quarantine-dialog-tab": () => setQuarantineDialogTab(tabId),
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
    "accepted-domain-create": () => openAcceptedDomainDrawer(null, actionTarget),
    "accepted-domain-edit": () => openAcceptedDomainDrawer(domainId, actionTarget),
    "accepted-domain-delete": () => runAction(() => deleteAcceptedDomain(domainId)),
    "accepted-domain-test": () => runAction(() => testAcceptedDomain(domainId)),
    "accepted-domain-import": () => openAcceptedDomainImportDrawer(actionTarget),
    "public-tls-upload": () => openPublicTlsUploadDrawer(actionTarget),
    "public-tls-select": () => runAction(() => selectPublicTlsProfile(profileId)),
    "public-tls-disable": () => runAction(() => disablePublicTlsProfile()),
    "public-tls-delete": () => runAction(() => deletePublicTlsProfile(profileId)),
    "platform-edit": () => openPlatformDrawer(target, actionTarget),
    "ntp-sync": () => runAction(() => syncNtp()),
    "apt-upgrade": () => runAction(() => runAptUpgrade()),
    "power-action": () => runAction(() => runPowerAction(powerAction)),
    "quarantine-bulk": () => runAction(() => runQuarantineBulkAction(bulkAction)),
    "quarantine-sort": () => setQuarantineSort(sortKey),
    "history-sort": () => setHistorySort(sortKey),
    "log-sort": () => setLogSort(logTable, sortKey),
    "host-log-view": () => runAction(() => openHostLog(logCategory, logId, actionTarget)),
    "host-log-download": () => runAction(() => downloadHostLog(logCategory, logId)),
    "host-log-delete": () => runAction(() => deleteHostLog(logCategory, logId)),
    "diagnostic-show": () => runAction(() => openDiagnostic(diagnosticKind, actionTarget)),
    "spam-test-show": () => runAction(() => runSpamTest(actionTarget)),
    "diagnostic-tool-run": () => runAction(() => runDiagnosticTool(diagnosticTool, actionTarget)),
    "support-connect": () => runAction(() => connectSupport(actionTarget)),
    "health-check-run": () => runAction(() => runHealthCheck(actionTarget)),
    "flush-mail-queue": () => runAction(() => flushMailQueue(actionTarget)),
    "system-service-action": () => runAction(() => runServiceAction(serviceId, serviceAction)),
    "page-tab": () => setPageTab(actionTarget),
    "system-setup-tab": () => setSystemSetupTab(actionTarget),
    "refresh-quarantine": () => runAction(() => loadOps()),
    "refresh-history": () => runAction(() => loadOps()),
  };
}

function handleBodyClick(event) {
  if (event.target.closest("[data-quarantine-select], [data-quarantine-select-all]")) {
    return;
  }
  const actionTarget = event.target.closest("[data-action]");
  if (actionTarget) {
    const handler = getActionHandlers(actionTarget)[actionTarget.dataset.action];
    if (handler) {
      handler();
    }
    return;
  }

  const pageTarget = event.target.closest("[data-page-target]");
  if (pageTarget) {
    setActivePage(pageTarget.dataset.pageTarget, { focus: true, updateHash: true });
    setSidebarOpen(false);
  }
}

function handleBodyChange(event) {
  const selectAll = event.target.closest("[data-quarantine-select-all]");
  if (selectAll) {
    state.quarantineSelection.clear();
    if (selectAll.checked) {
      state.quarantine.forEach((item) => {
        const traceId = quarantineTraceId(item);
        if (traceId) {
          state.quarantineSelection.add(traceId);
        }
      });
    }
    renderQuarantine();
    return;
  }

  const selection = event.target.closest("[data-quarantine-select]");
  if (!selection) {
    return;
  }
  const traceId = selection.dataset.traceId;
  if (!traceId) {
    return;
  }
  if (selection.checked) {
    state.quarantineSelection.add(traceId);
  } else {
    state.quarantineSelection.delete(traceId);
  }
  renderQuarantine();
}

function startHistoryColumnResize(event) {
  const resizer = event.target.closest("[data-history-resizer]");
  if (!resizer) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  const columnIndex = Number(resizer.dataset.columnIndex);
  if (!Number.isInteger(columnIndex)) {
    return;
  }
  const startX = event.clientX;
  const startWidth = state.historyColumnWidths[columnIndex] ?? DEFAULT_HISTORY_COLUMN_WIDTHS[columnIndex] ?? MIN_HISTORY_COLUMN_WIDTH;
  const handleMove = (moveEvent) => {
    const nextWidth = Math.max(MIN_HISTORY_COLUMN_WIDTH, startWidth + moveEvent.clientX - startX);
    state.historyColumnWidths[columnIndex] = nextWidth;
    const gridTemplate = historyGridTemplate();
    containers.history
      .querySelectorAll(".history-table-header, .history-message-row")
      .forEach((row) => row.style.setProperty("--history-grid-columns", gridTemplate));
  };
  const handleEnd = () => {
    window.removeEventListener("pointermove", handleMove);
    window.removeEventListener("pointerup", handleEnd);
  };
  window.addEventListener("pointermove", handleMove);
  window.addEventListener("pointerup", handleEnd);
}

function startLogColumnResize(event) {
  const resizer = event.target.closest("[data-log-resizer]");
  if (!resizer) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  const { logTable } = resizer.dataset;
  const table = logTableState(logTable);
  const columnIndex = Number(resizer.dataset.columnIndex);
  if (!Number.isInteger(columnIndex)) {
    return;
  }
  const startX = event.clientX;
  const fallbackWidth = DEFAULT_LOG_COLUMN_WIDTHS[logTable]?.[columnIndex] ?? MIN_HISTORY_COLUMN_WIDTH;
  const startWidth = table.columnWidths[columnIndex] ?? fallbackWidth;
  const container = {
    mail: containers.mailLog,
    interface: containers.audit,
    messages: containers.messageLog,
    emailAlerts: containers.emailAlertLog,
  }[logTable];
  const handleMove = (moveEvent) => {
    const nextWidth = Math.max(MIN_HISTORY_COLUMN_WIDTH, startWidth + moveEvent.clientX - startX);
    table.columnWidths[columnIndex] = nextWidth;
    const gridTemplate = logGridTemplate(logTable);
    container
      ?.querySelectorAll(".log-table-header, .log-message-row")
      .forEach((row) => row.style.setProperty("--log-grid-columns", gridTemplate));
  };
  const handleEnd = () => {
    window.removeEventListener("pointermove", handleMove);
    window.removeEventListener("pointerup", handleEnd);
  };
  window.addEventListener("pointermove", handleMove);
  window.addEventListener("pointerup", handleEnd);
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
elements.editVirusFiltering.addEventListener("click", (event) => openVirusFilteringDrawer(event.currentTarget));
elements.editFilteringPolicy.addEventListener("click", (event) => openFilteringPolicyDrawer(event.currentTarget));
elements.editRecipientVerification.addEventListener("click", (event) => openRecipientVerificationDrawer(event.currentTarget));
elements.editDkimSettings.addEventListener("click", (event) => openDkimSettingsDrawer(event.currentTarget));
elements.createDkimDomain.addEventListener("click", (event) => openDkimDomainDrawer(null, event.currentTarget));
elements.editDigestSettings.addEventListener("click", (event) => openDigestSettingsDrawer(event.currentTarget));
elements.createDigestDefault.addEventListener("click", (event) => openDigestDefaultDrawer(null, event.currentTarget));
elements.createDigestOverride.addEventListener("click", (event) => openDigestOverrideDrawer(null, event.currentTarget));

document.body.addEventListener("click", handleBodyClick);
document.body.addEventListener("change", handleBodyChange);
document.body.addEventListener("pointerdown", startHistoryColumnResize);
document.body.addEventListener("pointerdown", startLogColumnResize);

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
  if ((event.key === "Enter" || event.key === " ") && event.target?.closest?.("[data-action='quarantine-open']")) {
    event.preventDefault();
    handleBodyClick(event);
    return;
  }
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
updateNavState(pageFromHash());
try {
  setSidebarCollapsed(window.localStorage.getItem("lpeCtSidebarCollapsed") === "true");
} catch {}
setLocale(i18n.getLocale());
syncLoadingState();
window.setInterval(() => {
  renderHostClock();
}, 1000);
window.setInterval(refreshDashboardOnSchedule, DASHBOARD_REFRESH_INTERVAL_MS);
window.addEventListener("resize", () => {
  if (window.innerWidth > 1024) {
    setSidebarOpen(false);
  }
});
window.addEventListener("hashchange", () => {
  setActivePage(pageFromHash(), { focus: true });
});
void load();


