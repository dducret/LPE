import { getCopy, translate } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { containers, state } from "./context.js?v=20260502-outbound-ehlo";
import { buildEmptyState, buildLoadingRows } from "./ui.js?v=20260502-outbound-ehlo";
import { escapeHtml, formatList, formatNumber, formatScore, formatDateTime, formatShortDate, humanizeStatus, formatAntivirusProviders, antivirusProviderChain, labelForAntivirusProvider, displayTraceId, displayClientAddress, displayMailAddress, historySizeBytes, formatBytes, firstRecipient, formatHistoryType, historyColumns, quarantineTraceId, quarantineDate, quarantineScoreValue, quarantineColumns, quarantineGridTemplate, historyGridTemplate, sortQuarantineItems, sortHistoryItems, quarantineSortIndicator, sortIndicator, auditColumns, messageLogColumns, formatDurationMinutes, formatBooleanLabel, getDigestSettings, statusChipClass, labelForAddressRole, labelForAction, labelForAttachmentScope, labelForVerificationBackend, labelForKeyStatus } from "./format.js?v=20260502-outbound-ehlo";

export function routeToPolicies(role, action) {
  if (role === "sender" && action === "allow") return "allow_senders";
  if (role === "sender" && action === "block") return "block_senders";
  if (role === "recipient" && action === "allow") return "allow_recipients";
  return "block_recipients";
}

export function routeToAttachmentPolicies(scope, action) {
  if (scope === "extension" && action === "allow") return "allow_extensions";
  if (scope === "extension" && action === "block") return "block_extensions";
  if (scope === "mime" && action === "allow") return "allow_mime_types";
  if (scope === "mime" && action === "block") return "block_mime_types";
  if (scope === "detected" && action === "allow") return "allow_detected_types";
  return "block_detected_types";
}

export function getAddressRules(policies = state.dashboard?.policies) {
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

export function getAttachmentRules(policies = state.dashboard?.policies) {
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

export function findAddressRule(ruleId) {
  return getAddressRules().find((rule) => rule.id === ruleId) ?? null;
}

export function findAttachmentRule(ruleId) {
  return getAttachmentRules().find((rule) => rule.id === ruleId) ?? null;
}

export function selectedQuarantineItems() {
  return state.quarantine.filter((item) => state.quarantineSelection.has(quarantineTraceId(item)));
}

export function pruneQuarantineSelection() {
  const currentIds = new Set(state.quarantine.map(quarantineTraceId).filter(Boolean));
  state.quarantineSelection.forEach((traceId) => {
    if (!currentIds.has(traceId)) {
      state.quarantineSelection.delete(traceId);
    }
  });
}

// Renderers
export function renderQuarantine() {
  const copy = getCopy();
  const items = state.quarantine;
  pruneQuarantineSelection();
  const columns = quarantineColumns(copy);
  const sortedItems = sortQuarantineItems(items, columns);
  const gridTemplate = quarantineGridTemplate();
  const selectedCount = selectedQuarantineItems().length;
  const allSelected = items.length > 0 && selectedCount === items.length;
  const actionDisabled = selectedCount === 0 ? " disabled" : "";
  if (!items.length) {
    containers.quarantine.innerHTML = buildEmptyState(
      copy.emptyQuarantineTitle,
      copy.noResults,
      `<button class="secondary-button compact-button" type="button" data-action="refresh-quarantine">${escapeHtml(copy.refresh)}</button>`,
    );
    return;
  }

  containers.quarantine.innerHTML = `
    <div class="quarantine-bulk-actions" role="toolbar" aria-label="${escapeHtml(copy.quarantineBulkActions)}">
      <button class="secondary-button compact-button" type="button" data-action="quarantine-bulk" data-bulk-action="release"${actionDisabled}>${escapeHtml(copy.traceRelease)}</button>
      <button class="secondary-button compact-button" type="button" data-action="quarantine-bulk" data-bulk-action="allow"${actionDisabled}>${escapeHtml(copy.policyActionAllow)}</button>
      <button class="secondary-button compact-button" type="button" data-action="quarantine-bulk" data-bulk-action="block"${actionDisabled}>${escapeHtml(copy.policyActionBlock)}</button>
      <button class="secondary-button compact-button" type="button" data-action="quarantine-bulk" data-bulk-action="delete"${actionDisabled}>${escapeHtml(copy.traceDelete)}</button>
    </div>
    <div class="history-summary">${translate(copy.searchResults, { count: items.length })}</div>
    <div class="quarantine-table-header" style="--quarantine-grid-columns: ${escapeHtml(gridTemplate)}">
      ${columns
        .map(
          (column, index) => `
            <span class="quarantine-column-heading">
              ${
                index === 0
                  ? `<input class="quarantine-select-checkbox" type="checkbox" data-quarantine-select-all aria-label="${escapeHtml(copy.quarantineSelectAll)}"${allSelected ? " checked" : ""} />`
                  : ""
              }
              <button type="button" data-action="quarantine-sort" data-sort-key="${escapeHtml(column.key)}" aria-sort="${state.quarantineSort.key === column.key ? state.quarantineSort.direction : "none"}">${escapeHtml(column.label)}${escapeHtml(quarantineSortIndicator(column.key))}</button>
            </span>
          `,
        )
        .join("")}
    </div>
    ${sortedItems
      .map(
        (item) => {
          const traceId = quarantineTraceId(item);
          const selected = state.quarantineSelection.has(traceId);
          return `
            <div class="quarantine-message-row${selected ? " selected" : ""}" role="button" tabindex="0" data-action="quarantine-open" data-trace-id="${escapeHtml(traceId)}" aria-label="${escapeHtml(`${copy.traceOpen}: ${traceId}`)}" style="--quarantine-grid-columns: ${escapeHtml(gridTemplate)}">
              <span>
                <input class="quarantine-select-checkbox" type="checkbox" data-quarantine-select data-trace-id="${escapeHtml(traceId)}" aria-label="${escapeHtml(`${copy.quarantineColumnSelection}: ${traceId}`)}"${selected ? " checked" : ""} />
              </span>
              ${columns
                .slice(1)
                .map((column) => `<span title="${escapeHtml(column.value(item))}">${escapeHtml(column.value(item))}</span>`)
                .join("")}
            </div>
          `;
        },
      )
      .join("")}
  `;
}

export function renderHistory() {
  const copy = getCopy();
  const items = state.history;
  const columns = historyColumns(copy);
  const sortedItems = sortHistoryItems(items, columns);
  const gridTemplate = historyGridTemplate();
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
    <div class="history-table-header" style="--history-grid-columns: ${escapeHtml(gridTemplate)}">
      ${columns
        .map(
          (column, index) => `
            <span class="history-column-heading">
              <button type="button" data-action="history-sort" data-sort-key="${escapeHtml(column.key)}" aria-sort="${state.historySort.key === column.key ? state.historySort.direction : "none"}">${escapeHtml(column.label)}${escapeHtml(sortIndicator(column.key))}</button>
              <span class="history-column-resizer" role="separator" aria-orientation="vertical" data-history-resizer data-column-index="${index}"></span>
            </span>
          `,
        )
        .join("")}
    </div>
    ${sortedItems
      .map(
        (item) => `
          <button class="history-message-row" type="button" data-action="trace-open" data-trace-id="${escapeHtml(item.trace_id)}" aria-label="${escapeHtml(`${copy.traceOpen}: ${item.trace_id}`)}" style="--history-grid-columns: ${escapeHtml(gridTemplate)}">
            ${columns.map((column) => `<span>${escapeHtml(column.value(item))}</span>`).join("")}
          </button>
        `,
      )
      .join("")}
  `;
}

export function renderFilteringPolicy() {
  const copy = getCopy();
  const policies = state.dashboard?.policies ?? {};
  const rows = [
    { label: copy.requireSpfLabel, value: policies.require_spf ? copy.enabled : copy.disabled },
    { label: copy.requireDmarcLabel, value: policies.require_dmarc_enforcement ? copy.enabled : copy.disabled },
    { label: copy.requireDkimAlignmentLabel, value: policies.require_dkim_alignment ? copy.enabled : copy.disabled },
    { label: copy.deferOnAuthTempfailLabel, value: policies.defer_on_auth_tempfail ? copy.enabled : copy.disabled },
    { label: copy.bayespamEnabledLabel, value: policies.bayespam_enabled ? copy.enabled : copy.disabled },
    { label: copy.reputationEnabledLabel, value: policies.reputation_enabled ? copy.enabled : copy.disabled },
    { label: copy.spamQuarantineThresholdLabel, value: formatScore(policies.spam_quarantine_threshold) },
    { label: copy.spamRejectThresholdLabel, value: formatScore(policies.spam_reject_threshold) },
    { label: copy.reputationQuarantineThresholdLabel, value: formatNumber(policies.reputation_quarantine_threshold) },
    { label: copy.reputationRejectThresholdLabel, value: formatNumber(policies.reputation_reject_threshold) },
  ];
  containers.filteringPolicy.innerHTML = `
    <article class="summary-card">
      <strong>${escapeHtml(copy.filteringPolicyTitle)}</strong>
      <div class="summary-grid">
        ${rows
          .map(
            (row) => `
              <div>
                <p>${escapeHtml(row.label)}</p>
                <span class="pill">${escapeHtml(row.value)}</span>
              </div>
            `,
          )
          .join("")}
      </div>
    </article>
  `;
}

export function renderVirusFiltering() {
  const copy = getCopy();
  const policies = state.dashboard?.policies ?? {};
  const providerChain = antivirusProviderChain(policies);
  const enabled = Boolean(policies.antivirus_enabled);
  const rows = [
    {
      label: copy.virusFilteringEnabledLabel,
      html: `<span class="${statusChipClass(enabled ? "enabled" : "disabled")}">${escapeHtml(enabled ? copy.enabled : copy.disabled)}</span>`,
    },
    {
      label: copy.virusFailClosedLabel,
      html: `<span class="${statusChipClass(policies.antivirus_fail_closed ? "enabled" : "disabled")}">${escapeHtml(policies.antivirus_fail_closed ? copy.enabled : copy.disabled)}</span>`,
    },
    { label: copy.virusProviderChainLabel, html: `<span class="record-copy">${escapeHtml(formatAntivirusProviders(providerChain))}</span>` },
    { label: copy.virusScopeLabel, html: `<span class="pill">${escapeHtml(copy.virusScopeInboundOutbound)}</span>` },
  ];
  containers.virusFiltering.innerHTML = `
    <article class="summary-card">
      <strong>${escapeHtml(copy.virusFilteringTitle)}</strong>
      <div class="summary-grid">
        ${rows
          .map(
            (row) => `
              <div>
                <p>${escapeHtml(row.label)}</p>
                ${row.html}
              </div>
            `,
          )
          .join("")}
      </div>
    </article>
  `;
}

export function renderAddressRules() {
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

export function renderAttachmentRules() {
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

export function renderRecipientVerification() {
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

export function renderDkim() {
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

export function renderDigestDefaults(reporting) {
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

export function renderDigestOverrides(reporting) {
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

export function renderDigestReportsList(reports) {
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

export function renderDigestReporting() {
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

