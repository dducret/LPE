import { getCopy, translate } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { elements, state } from "./context.js?v=20260502-outbound-ehlo";
import { fetchBlob, fetchJson, postJson } from "./api.js?v=20260502-outbound-ehlo";
import { closeDrawer, buildLoadingRows, renderDrawerContent, showFeedback } from "./ui.js?v=20260502-outbound-ehlo";
import { renderLogTableById, renderSystemInformation } from "./system.js?v=20260502-outbound-ehlo";
import { selectedQuarantineItems } from "./lists.js?v=20260502-outbound-ehlo";
import { currentPolicies, displayClientAddress, displayMailAddress, escapeHtml, formatBytes, formatDetailedScore, formatHistoryDateTime, formatList, formatLongTraceDateTime, formatNumber, formatScore, historySizeBytes, humanizeStatus, labelForAction, quarantineDate, quarantineScoreValue, quarantineTraceId, statusChipClass, traceAttachmentItems, traceBooleanLabel, traceContentClassification, traceHeaderValue, traceHeadersText, traceMessageSize, traceObjectValue, tracePolicyFlag, traceQueueCanBeDeleted, traceTextValue } from "./format.js?v=20260502-outbound-ehlo";

const callbacks = {
  loadOps: async () => {},
  savePolicies: async () => {},
};

export function configureTraceActions(options) {
  callbacks.loadOps = options.loadOps;
  callbacks.savePolicies = options.savePolicies;
}

export function quarantineDialogTabButton(tabId, label, activeTab) {
  return `
    <button class="tab-button${activeTab === tabId ? " tab-button-active" : ""}" type="button" data-action="quarantine-dialog-tab" data-tab-id="${escapeHtml(tabId)}" aria-selected="${activeTab === tabId ? "true" : "false"}">
      ${escapeHtml(label)}
    </button>
  `;
}

export function quarantineDetailRows(rows) {
  return rows
    .map(
      ([label, value]) => `
        <div>
          <dt>${escapeHtml(label)}:</dt>
          <dd>${escapeHtml(traceTextValue(value))}</dd>
        </div>
      `,
    )
    .join("");
}

export function renderQuarantineDetails(trace, current, retainedHistory) {
  const copy = getCopy();
  const latestHistoryEvent = retainedHistory.length ? retainedHistory[retainedHistory.length - 1] : null;
  const senderAddress = displayMailAddress(current.mail_from);
  const statusText = String(current.status || current.queue || "").toLowerCase();
  const reasonText = String(current.reason || "").toLowerCase();
  const smtpResponse =
    current.smtp_response ||
    current.smtp_response_text ||
    current.reason ||
    traceObjectValue(current.technical_status) ||
    traceObjectValue(current.dsn);
  const detailRows = [
    [copy.quarantineDetailId, trace.trace_id || current.trace_id],
    [copy.quarantineDetailMessageReceived, formatLongTraceDateTime(current.received_at || latestHistoryEvent?.timestamp || quarantineDate(current))],
    [copy.quarantineDetailEnvelopeFrom, displayMailAddress(current.mail_from)],
    [copy.quarantineDetailFromAddress, traceHeaderValue(current, "From") || displayMailAddress(current.mail_from)],
    [copy.quarantineDetailRecipient, formatList((current.rcpt_to ?? []).map(displayMailAddress))],
    [copy.quarantineDetailSubject, current.subject],
    [copy.quarantineDetailClientAddress, displayClientAddress(current.peer || latestHistoryEvent?.peer)],
    [copy.quarantineDetailCountryOfOrigin, current.country_of_origin || current.geo_country || current.country],
    [copy.quarantineDetailMessageSize, formatNumber(traceMessageSize(current, latestHistoryEvent))],
    [copy.quarantineDetailContentClassification, traceContentClassification(current)],
    [copy.quarantineDetailVirusInfected, traceBooleanLabel(reasonText.includes("virus") || reasonText.includes("malware"))],
    [copy.quarantineDetailScore, formatDetailedScore(quarantineScoreValue(current))],
    [copy.quarantineDetailQuarantined, traceBooleanLabel(statusText.includes("quarantine") || String(current.queue || "").toLowerCase() === "quarantine")],
    [copy.quarantineDetailMessageId, current.internet_message_id || traceHeaderValue(current, "Message-ID")],
    [copy.quarantineDetailSmtpResponse, smtpResponse],
    [copy.quarantineDetailDeliveryStatus, humanizeStatus(current.status)],
    [copy.quarantineDetailLastStatusUpdate, formatLongTraceDateTime(latestHistoryEvent?.timestamp || current.received_at)],
    [copy.quarantineDetailBlockedSender, tracePolicyFlag(senderAddress, "block_senders") ? "Y" : "N"],
    [copy.quarantineDetailAllowedSender, tracePolicyFlag(senderAddress, "allow_senders") ? "Y" : "N"],
    [copy.quarantineDetailMailFlowDirection, humanizeStatus(current.direction)],
    [copy.quarantineDetailEncryption, current.encryption || current.tls_cipher || current.tls_protocol || "None"],
  ];

  return `<dl class="quarantine-detail-list">${quarantineDetailRows(detailRows)}</dl>`;
}

export function renderMessageView(current) {
  const copy = getCopy();
  const headersText = traceHeadersText(current);
  const attachments = traceAttachmentItems(current);
  const attachmentItems = attachments
    .map((attachment) => {
      const name = attachment.name || attachment.filename || attachment.file_name || copy.unset;
      const size = attachment.size ?? attachment.size_bytes ?? attachment.bytes;
      return `<li>${escapeHtml(name)} <span>${escapeHtml(formatBytes(size))}</span></li>`;
    })
    .join("");

  return `
    <section class="quarantine-message-view">
      <dl class="message-header-list">
        <div><dt>${escapeHtml(copy.historyColumnFrom)}:</dt><dd>${escapeHtml(traceHeaderValue(current, "From") || displayMailAddress(current.mail_from))}</dd></div>
        <div><dt>${escapeHtml(copy.historyColumnTo)}:</dt><dd>${escapeHtml(traceHeaderValue(current, "To") || formatList((current.rcpt_to ?? []).map(displayMailAddress)))}</dd></div>
        <div><dt>${escapeHtml(copy.historyColumnDate)}:</dt><dd>${escapeHtml(traceHeaderValue(current, "Date") || formatHistoryDateTime(current.received_at))}</dd></div>
        <div><dt>${escapeHtml(copy.quarantineColumnSubject)}:</dt><dd>${escapeHtml(current.subject || traceHeaderValue(current, "Subject") || copy.unset)}</dd></div>
        <div>
          <dt>${escapeHtml(copy.traceHeadersTitle)}:</dt>
          <dd>
            <details class="message-headers-toggle">
              <summary>${escapeHtml(copy.quarantineShowAllHeaders)}</summary>
              <pre>${escapeHtml(headersText || copy.unset)}</pre>
            </details>
          </dd>
        </div>
      </dl>
      <pre class="message-raw-content">${escapeHtml(current.body_content || current.raw_content || current.body_excerpt || copy.unset)}</pre>
      ${attachmentItems ? `<ul class="message-attachment-list">${attachmentItems}</ul>` : ""}
    </section>
  `;
}

export function renderQuarantineTraceDialog(trace, opener = document.activeElement) {
  const copy = getCopy();
  if (!trace) {
    renderDrawerContent(copy.quarantineDialogTitle, copy.noTraceLoaded, `<p class="record-copy">${escapeHtml(copy.noTraceLoaded)}</p>`, opener, null, "wide");
    return;
  }
  const current = trace.current ?? {};
  const retainedHistory = trace.history ?? [];
  const activeTab = state.quarantineDialogTab === "message" ? "message" : "details";
  const detailsHtml = renderQuarantineDetails(trace, current, retainedHistory);
  const messageHtml = renderMessageView(current);
  const panelHtml = activeTab === "message" ? messageHtml : detailsHtml;
  renderDrawerContent(
    current.subject || trace.trace_id || copy.quarantineDialogTitle,
    `${displayMailAddress(current.mail_from)} -> ${formatList((current.rcpt_to ?? []).map(displayMailAddress))}`,
    `
      <div class="quarantine-dialog">
        <div class="quarantine-dialog-tabs" role="tablist" aria-label="${escapeHtml(copy.quarantineDialogTitle)}">
          ${quarantineDialogTabButton("details", copy.quarantineDialogDetailsTab, activeTab)}
          ${quarantineDialogTabButton("message", copy.quarantineDialogViewMessageTab, activeTab)}
        </div>
        <section class="quarantine-dialog-panel">
          ${panelHtml}
        </section>
      </div>
    `,
    opener,
    null,
    "wide",
  );
}

export function renderTraceDrawer(trace, opener = document.activeElement) {
  const copy = getCopy();
  if (!trace) {
    renderDrawerContent(copy.traceSummaryTitle, copy.noTraceLoaded, `<p class="record-copy">${escapeHtml(copy.noTraceLoaded)}</p>`, opener);
    return;
  }
  const current = trace.current ?? {};
  const retainedHistory = trace.history ?? [];
  const latestHistoryEvent = retainedHistory.length ? retainedHistory[retainedHistory.length - 1] : null;
  const messageSizeBytes = current.message_size_bytes ?? historySizeBytes(latestHistoryEvent);
  const technicalStatus = current.technical_status ? escapeHtml(JSON.stringify(current.technical_status, null, 2)) : escapeHtml(copy.unset);
  const authSummary = current.auth_summary ? escapeHtml(JSON.stringify(current.auth_summary, null, 2)) : escapeHtml(copy.unset);
  const dsn = current.dsn ? escapeHtml(JSON.stringify(current.dsn, null, 2)) : "";
  const historyItems = retainedHistory
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
    `${displayMailAddress(current.mail_from)} -> ${formatList((current.rcpt_to ?? []).map(displayMailAddress))}`,
    `
      <div class="record-actions">
        ${current.trace_id || trace.trace_id ? `<button class="list-action" type="button" data-action="trace-retry" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRetry}</button>` : ""}
        ${current.queue === "quarantine" || current.queue === "held" ? `<button class="list-action" type="button" data-action="trace-release" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceRelease}</button>` : ""}
        ${traceQueueCanBeDeleted(current.queue) ? `<button class="list-action danger-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceDelete}</button>` : ""}
      </div>
      <section class="trace-section">
        <h4>${copy.traceSummaryTitle}</h4>
        <div class="summary-grid">
          <div><p>${copy.traceLabel}</p><span class="record-copy">${escapeHtml(trace.trace_id)}</span></div>
          <div><p>${copy.statusLabel}</p><span class="record-copy">${escapeHtml(current.status || copy.unset)}</span></div>
          <div><p>${copy.queueLabel}</p><span class="record-copy">${escapeHtml(current.queue || copy.unset)}</span></div>
          <div><p>${copy.routeLabel}</p><span class="record-copy">${escapeHtml(current.route?.relay_target || copy.unset)}</span></div>
          <div><p>${copy.historyColumnClientAddress}</p><span class="record-copy">${escapeHtml(displayClientAddress(current.peer || latestHistoryEvent?.peer))}</span></div>
          <div><p>${copy.historyColumnSize}</p><span class="record-copy">${escapeHtml(formatBytes(messageSizeBytes))}</span></div>
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
export async function openHostLog(category, logId, opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.logPreviewTitle, logId, buildLoadingRows(2), opener, null, "wide");
  try {
    const entry = await fetchJson(`/api/host-logs/${encodeURIComponent(category)}/${encodeURIComponent(logId)}`);
    renderDrawerContent(
      entry.name,
      `${formatBytes(entry.size_bytes)}${entry.truncated ? ` · ${copy.logPreviewTruncated}` : ""}`,
      `<section class="trace-section host-log-preview"><pre>${escapeHtml(entry.content || copy.logEmptyContent)}</pre></section>`,
      opener,
      null,
      "wide",
    );
  } catch (error) {
    renderDrawerContent(
      copy.logPreviewTitle,
      logId,
      `<p class="record-copy">${escapeHtml(error instanceof Error ? error.message : copy.unknownError)}</p>`,
      opener,
      null,
      "wide",
    );
  }
}

export async function downloadHostLog(category, logId) {
  const blob = await fetchBlob(`/api/host-logs/${encodeURIComponent(category)}/${encodeURIComponent(logId)}/download`);
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = logId;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

export async function deleteHostLog(category, logId) {
  await fetchJson(`/api/host-logs/${encodeURIComponent(category)}/${encodeURIComponent(logId)}`, { method: "DELETE" });
  const response = await fetchJson(`/api/host-logs/${encodeURIComponent(category)}`);
  state.hostLogs[category] = response?.items ?? [];
  renderLogTableById(category);
  showFeedback(getCopy().logDeleted);
}

export async function loadTrace(traceId, opener = document.activeElement) {
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

export async function loadQuarantineTrace(traceId, opener = document.activeElement) {
  const copy = getCopy();
  state.loading.trace = true;
  state.quarantineDialogTab = "details";
  renderDrawerContent(copy.quarantineDialogTitle, copy.loadingTrace, buildLoadingRows(2), opener, null, "wide");
  try {
    state.selectedTrace = await fetchJson(`/api/history/${traceId}`);
    renderQuarantineTraceDialog(state.selectedTrace, opener);
  } catch (error) {
    renderDrawerContent(copy.quarantineDialogTitle, copy.noTraceLoaded, `<p class="record-copy">${escapeHtml(error instanceof Error ? error.message : copy.unknownError)}</p>`, opener, null, "wide");
  } finally {
    state.loading.trace = false;
  }
}

export function setQuarantineDialogTab(tabId) {
  if (!["details", "message"].includes(tabId)) {
    return;
  }
  state.quarantineDialogTab = tabId;
  renderQuarantineTraceDialog(state.selectedTrace, elements.drawerClose);
}

export async function triggerTraceAction(traceId, action) {
  const copy = getCopy();
  showFeedback(translate(copy.traceActionRunning, { action, traceId }), "warning");
  await fetchJson(`/api/traces/${traceId}/${action}`, { method: "POST" });
  showFeedback(translate(copy.traceActionCompleted, { traceId }));
  await callbacks.loadOps({ silent: true });
  try {
    await loadTrace(traceId, elements.drawerClose);
  } catch {
    closeDrawer();
  }
}

export async function triggerSelectedTraceAction(action) {
  const copy = getCopy();
  const traceIds = selectedQuarantineItems().map(quarantineTraceId).filter(Boolean);
  if (!traceIds.length) {
    showFeedback(copy.quarantineSelectAtLeastOne, "warning");
    return;
  }
  showFeedback(translate(copy.quarantineBulkRunning, { action, count: traceIds.length }), "warning");
  for (const traceId of traceIds) {
    await fetchJson(`/api/traces/${traceId}/${action}`, { method: "POST" });
  }
  state.quarantineSelection.clear();
  showFeedback(translate(copy.quarantineBulkCompleted, { action, count: traceIds.length }));
  await callbacks.loadOps({ silent: true });
}

export async function updateSelectedSenderPolicy(action) {
  const copy = getCopy();
  const senders = [
    ...new Set(
      selectedQuarantineItems()
        .map((item) => displayMailAddress(item.mail_from))
        .filter((sender) => sender && sender !== copy.unset)
        .map((sender) => sender.toLowerCase()),
    ),
  ];
  if (!senders.length) {
    showFeedback(copy.quarantineSelectAtLeastOne, "warning");
    return;
  }
  const policies = currentPolicies();
  policies.address_policy = policies.address_policy ?? {};
  policies.address_policy.allow_senders = policies.address_policy.allow_senders ?? [];
  policies.address_policy.block_senders = policies.address_policy.block_senders ?? [];
  const targetKey = action === "allow" ? "allow_senders" : "block_senders";
  const oppositeKey = action === "allow" ? "block_senders" : "allow_senders";
  const target = new Set(policies.address_policy[targetKey].map((value) => String(value).toLowerCase()));
  senders.forEach((sender) => target.add(sender));
  policies.address_policy[targetKey] = [...target].sort();
  const selectedSenderSet = new Set(senders);
  policies.address_policy[oppositeKey] = policies.address_policy[oppositeKey].filter(
    (value) => !selectedSenderSet.has(String(value).toLowerCase()),
  );
  state.quarantineSelection.clear();
  await callbacks.savePolicies(policies);
  showFeedback(translate(copy.quarantineBulkPolicyCompleted, { action: labelForAction(action), count: senders.length }));
}

export async function runQuarantineBulkAction(action) {
  if (action === "release" || action === "delete") {
    await triggerSelectedTraceAction(action);
    return;
  }
  if (action === "allow" || action === "block") {
    await updateSelectedSenderPolicy(action);
  }
}

export async function openDigestReport(reportId, opener = document.activeElement) {
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

export function renderDiagnosticDrawer(report, opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(
    report?.title || copy.systemDiagnosticsTitle,
    report?.detail || copy.unset,
    `
      <section class="trace-section">
        <h4>${escapeHtml(copy.statusLabel)}</h4>
        <span class="${statusChipClass(report?.status)}">${escapeHtml(humanizeStatus(report?.status))}</span>
      </section>
      <section class="trace-section">
        <h4>${escapeHtml(copy.output)}</h4>
        ${renderDiagnosticOutput(report, copy)}
      </section>
    `,
    opener,
    null,
    "wide",
  );
}

export function renderDiagnosticOutput(report, copy = getCopy()) {
  if (report?.title === copy.systemMailQueue || report?.title === "Mail Queue") {
    const metrics = parseJsonObject(report.output);
    if (metrics) {
      return renderMailQueueOutput(metrics, copy);
    }
  }
  if (report?.title === copy.systemHealthCheck || report?.title === "System Health Check") {
    const readiness = parseJsonObject(report.output);
    if (readiness && Array.isArray(readiness.checks)) {
      return renderHealthCheckOutput(readiness, copy);
    }
  }
  return `<pre class="diagnostic-output">${escapeHtml(report?.output || copy.unset)}</pre>`;
}

export function parseJsonObject(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function renderMailQueueOutput(metrics, copy = getCopy()) {
  const queueRows = [
    [copy.mailQueueInbound, metrics.inbound_messages],
    [copy.mailQueueIncoming, metrics.incoming_messages],
    [copy.mailQueueActive, metrics.active_messages],
    [copy.mailQueueDeferred, metrics.deferred_messages],
    [copy.mailQueueQuarantined, metrics.quarantined_messages],
    [copy.mailQueueHeld, metrics.held_messages],
    [copy.mailQueueCorrupt, metrics.corrupt_messages],
    [copy.mailQueueAttemptsLastHour, metrics.delivery_attempts_last_hour],
  ];
  const reachable = Boolean(metrics.upstream_reachable);
  return `
    <div class="mail-queue-output">
      <div class="mail-queue-reachability">
        <span class="${statusChipClass(reachable ? "ok" : "failed")}">${escapeHtml(reachable ? copy.relayReachable : copy.relayUnreachable)}</span>
      </div>
      <div class="mail-queue-metric-grid">
        ${queueRows
          .map(
            ([label, value]) => `
              <div class="mail-queue-metric">
                <span>${escapeHtml(label)}</span>
                <strong>${escapeHtml(formatNumber(value ?? 0))}</strong>
              </div>
            `,
          )
          .join("")}
      </div>
    </div>
  `;
}

export function healthCheckSummaryValue(value, copy = getCopy()) {
  const normalized = value === undefined || value === null || value === "" ? copy.unset : value;
  return escapeHtml(String(normalized));
}

export function healthCheckMarkerClass(value) {
  const normalized = String(value || "unknown").toLowerCase();
  if (normalized === "ok" || normalized === "ready") {
    return "ok";
  }
  if (normalized === "warn" || normalized === "degraded") {
    return "warn";
  }
  if (normalized === "failed" || normalized === "danger" || normalized === "error") {
    return "failed";
  }
  return "unknown";
}

export function renderHealthCheckOutput(readiness, copy = getCopy()) {
  const checks = Array.isArray(readiness.checks) ? readiness.checks : [];
  const warningCount = Number(readiness.warnings);
  const summaryRows = [
    {
      label: copy.healthCheckStatus,
      html: `<span class="${statusChipClass(readiness.status)}">${escapeHtml(humanizeStatus(readiness.status))}</span>`,
    },
    {
      label: copy.healthCheckWarnings,
      html: `<strong>${escapeHtml(formatNumber(Number.isFinite(warningCount) ? warningCount : 0))}</strong>`,
    },
    {
      label: copy.healthCheckService,
      html: `<strong>${healthCheckSummaryValue(humanizeStatus(readiness.service), copy)}</strong>`,
    },
    {
      label: copy.healthCheckNode,
      html: `<strong>${healthCheckSummaryValue(readiness.node_name, copy)}</strong>`,
    },
    {
      label: copy.healthCheckRole,
      html: `<strong>${healthCheckSummaryValue(humanizeStatus(readiness.role), copy)}</strong>`,
    },
  ];

  return `
    <div class="health-check-output">
      <div class="health-check-summary-grid">
        ${summaryRows
          .map(
            (item) => `
              <div class="health-check-summary-card">
                <span class="health-check-summary-label">${escapeHtml(item.label)}</span>
                ${item.html}
              </div>
            `,
          )
          .join("")}
      </div>
      <div class="health-check-list" aria-label="${escapeHtml(copy.healthCheckChecks)}">
        ${checks.length
          ? checks
              .map((check) => {
                const status = String(check?.status || "unknown").toLowerCase();
                const critical = Boolean(check?.critical);
                return `
                  <article class="health-check-row">
                    <span class="health-check-marker ${healthCheckMarkerClass(status)}" aria-hidden="true"></span>
                    <div class="health-check-row-body">
                      <strong>${escapeHtml(humanizeStatus(check?.name))}</strong>
                      <p>${escapeHtml(check?.detail || copy.unset)}</p>
                    </div>
                    <div class="health-check-row-actions">
                      <span class="${statusChipClass(status)}">${escapeHtml(humanizeStatus(status))}</span>
                      <span class="health-check-impact ${critical ? "critical" : "advisory"}">${escapeHtml(critical ? copy.healthCheckCritical : copy.healthCheckAdvisory)}</span>
                    </div>
                  </article>
                `;
              })
              .join("")
          : `<p class="muted-copy">${escapeHtml(copy.unset)}</p>`}
      </div>
    </div>
  `;
}

export function diagnosticToolTitle(tool, copy = getCopy()) {
  if (tool === "ping") {
    return copy.systemToolPing;
  }
  if (tool === "traceroute") {
    return copy.systemToolTraceroute;
  }
  if (tool === "dig") {
    return copy.systemToolDig;
  }
  return copy.systemToolsTitle;
}

export function diagnosticTitle(kind, copy = getCopy()) {
  if (kind === "mail-queue") {
    return copy.systemMailQueue;
  }
  if (kind === "process-list") {
    return copy.systemProcessList;
  }
  if (kind === "network-connections") {
    return copy.systemNetworkConnections;
  }
  if (kind === "routing-table") {
    return copy.systemRoutingTable;
  }
  return copy.systemDiagnosticsTitle;
}

export function diagnosticSummary(kind, copy = getCopy()) {
  if (kind === "mail-queue") {
    return copy.systemMailQueueSummary;
  }
  if (kind === "process-list") {
    return copy.systemProcessListSummary;
  }
  if (kind === "network-connections") {
    return copy.systemNetworkConnectionsSummary;
  }
  if (kind === "routing-table") {
    return copy.systemRoutingTableSummary;
  }
  return copy.systemDiagnosticsTitle;
}

export function diagnosticToolSummary(tool, copy = getCopy()) {
  if (tool === "ping") {
    return copy.systemToolPingSummary;
  }
  if (tool === "traceroute") {
    return copy.systemToolTracerouteSummary;
  }
  if (tool === "dig") {
    return copy.systemToolDigSummary;
  }
  return copy.systemToolsTitle;
}

export function waitForNextFrame() {
  return new Promise((resolve) => requestAnimationFrame(resolve));
}

export function renderPendingDiagnosticDrawer(title, summary, opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(
    title,
    summary || copy.loadingRecords,
    `
      <section class="trace-section">
        <h4>${escapeHtml(copy.statusLabel)}</h4>
        <span class="status-chip warn">${escapeHtml(copy.diagnosticRunning)}</span>
      </section>
      <section class="trace-section">
        <h4>${escapeHtml(copy.output)}</h4>
        <div class="diagnostic-output diagnostic-output-pending" role="status" aria-live="polite">
          <span class="diagnostic-hourglass" aria-hidden="true"></span>
          <span>${escapeHtml(copy.diagnosticWaitingOutput)}</span>
        </div>
      </section>
    `,
    opener,
    null,
    "wide",
  );
}

export async function openDiagnostic(kind, opener = document.activeElement) {
  const copy = getCopy();
  renderPendingDiagnosticDrawer(diagnosticTitle(kind, copy), diagnosticSummary(kind, copy), opener);
  await waitForNextFrame();
  const report = await fetchJson(`/api/system-diagnostics/${encodeURIComponent(kind)}`);
  renderDiagnosticDrawer(report, opener);
}

export async function runHealthCheck(opener = document.activeElement) {
  const copy = getCopy();
  renderPendingDiagnosticDrawer(copy.systemHealthCheck, copy.systemHealthCheckSummary, opener);
  await waitForNextFrame();
  const report = await postJson("/api/system-diagnostics/health-check");
  renderDiagnosticDrawer(report, opener);
}

export async function connectSupport(opener = document.activeElement) {
  const copy = getCopy();
  renderPendingDiagnosticDrawer(copy.systemSupportConnect, copy.systemSupportConnectSummary, opener);
  await waitForNextFrame();
  const report = await postJson("/api/system-diagnostics/support-connect");
  renderDiagnosticDrawer(report, opener);
}

export async function flushMailQueue(opener = document.activeElement) {
  const copy = getCopy();
  renderPendingDiagnosticDrawer(copy.systemFlushMailQueue, copy.systemFlushMailQueueSummary, opener);
  await waitForNextFrame();
  const report = await postJson("/api/system-diagnostics/flush-mail-queue");
  renderDiagnosticDrawer(report, opener);
  await callbacks.loadOps({ silent: true });
}

export async function runDiagnosticTool(tool, opener = document.activeElement) {
  const input = document.getElementById(`diagnostic-tool-${tool}`);
  const target = input?.value?.trim() ?? "";
  if (!target) {
    showFeedback(getCopy().targetRequired, "error");
    input?.focus();
    return;
  }
  const copy = getCopy();
  renderPendingDiagnosticDrawer(diagnosticToolTitle(tool, copy), diagnosticToolSummary(tool, copy), opener);
  await waitForNextFrame();
  const report = await postJson("/api/system-diagnostics/tools", { tool, target });
  renderDiagnosticDrawer(report, opener);
}

export async function runSpamTest(opener = document.activeElement) {
  const input = document.getElementById("spam-test-file");
  const file = input?.files?.[0];
  if (!file) {
    showFeedback(getCopy().fileRequired, "error");
    input?.focus();
    return;
  }
  const copy = getCopy();
  renderPendingDiagnosticDrawer(copy.systemSpamTest, copy.systemSpamTestSummary, opener);
  await waitForNextFrame();
  const contentBase64 = await fileToBase64(file);
  const report = await postJson("/api/system-diagnostics/spam-test", {
    filename: file.name,
    content_base64: contentBase64,
  });
  renderDiagnosticDrawer(report, opener);
}

export async function fileToBase64(file) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return window.btoa(binary);
}

export async function runServiceAction(serviceId, serviceAction) {
  await postJson(`/api/system-diagnostics/services/${encodeURIComponent(serviceId)}/${encodeURIComponent(serviceAction)}`);
  state.systemServices = (await fetchJson("/api/system-diagnostics/services"))?.items ?? [];
  renderSystemInformation();
  showFeedback(getCopy().recordSaved);
}
