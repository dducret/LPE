import { getCopy, i18n } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { DEFAULT_HISTORY_COLUMN_WIDTHS, DEFAULT_LOG_COLUMN_WIDTHS, DEFAULT_QUARANTINE_COLUMN_WIDTHS, LAST_ADMIN_EMAIL_KEY, MIN_HISTORY_COLUMN_WIDTH, state } from "./context.js?v=20260502-outbound-ehlo";

function buildEmptyState(title, description, actionHtml = "") {
  return `
    <article class="empty-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(description)}</p>
      ${actionHtml}
    </article>
  `;
}

export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function formatList(values) {
  return (values ?? []).filter(Boolean).join(", ") || getCopy().unset;
}

export function isValidHostname(value) {
  const normalized = String(value ?? "").trim().replace(/\.$/, "").toLowerCase();
  if (!normalized || normalized.length > 253 || !normalized.includes(".")) {
    return false;
  }
  return normalized.split(".").every((label) => (
    label.length > 0
      && label.length <= 63
      && /^[a-z0-9-]+$/.test(label)
      && !label.startsWith("-")
      && !label.endsWith("-")
  ));
}

export function parseProviderChain(value) {
  return dedupeList(
    String(value ?? "")
      .split(/[\n,]+/)
      .map((provider) => provider.trim().toLowerCase())
      .filter(Boolean),
  );
}

export function antivirusProviderChain(policies = state.dashboard?.policies) {
  return Array.isArray(policies?.antivirus_provider_chain)
    ? policies.antivirus_provider_chain.map((provider) => String(provider ?? "").trim().toLowerCase()).filter(Boolean)
    : [];
}

export function labelForAntivirusProvider(provider) {
  return String(provider ?? "").toLowerCase() === "takeri" ? getCopy().virusProviderTakeri : provider;
}

export function formatAntivirusProviders(providers) {
  return formatList(providers.map(labelForAntivirusProvider));
}

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return new Intl.NumberFormat(i18n.getLocale()).format(Number(value));
}

export function formatScore(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return Number(value).toFixed(1);
}

export function formatDetailedScore(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return Number(value).toFixed(3);
}

export function formatDateTime(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return getCopy().unset;
  }
  return new Intl.DateTimeFormat(i18n.getLocale(), {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function parseHistoryTimestamp(value) {
  if (!value) {
    return null;
  }
  const text = String(value);
  const unixValue = text.startsWith("unix:") ? Number(text.slice(5)) : Number.NaN;
  const date = Number.isFinite(unixValue) ? new Date(unixValue * 1000) : new Date(text);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function formatHistoryDateTime(value) {
  const date = parseHistoryTimestamp(value);
  if (!date) {
    return getCopy().unset;
  }
  const pad = (part) => String(part).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
  ].join("-") + ` ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function displayTraceId(value) {
  return String(value || getCopy().unset).replace(/^lpe-ct-(?:in|out)-/i, "");
}

export function displayClientAddress(value) {
  const text = String(value || "").trim();
  if (!text) {
    return getCopy().unset;
  }
  const bracketedIpv6 = text.match(/^\[([^\]]+)\](?::\d+)?$/);
  if (bracketedIpv6) {
    return bracketedIpv6[1];
  }
  const hostPort = text.match(/^([^:\s]+):\d+$/);
  if (hostPort) {
    return hostPort[1];
  }
  return text;
}

export function displayMailAddress(value) {
  const text = String(value || "").trim();
  if (!text) {
    return getCopy().unset;
  }
  const pathMatch = text.match(/<([^>]*)>/);
  if (pathMatch) {
    return pathMatch[1] || getCopy().unset;
  }
  const emailMatch = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (emailMatch) {
    return emailMatch[0];
  }
  return text.split(/\s+/)[0].replace(/[<>]/g, "");
}

export function historySizeBytes(item) {
  const stored = Number(item?.message_size_bytes);
  if (Number.isFinite(stored) && stored >= 0) {
    return stored;
  }
  const sizeMatch = String(item?.mail_from || "").match(/\bSIZE=(\d+)\b/i);
  return sizeMatch ? Number(sizeMatch[1]) : null;
}

export function formatLongTraceDateTime(value) {
  const date = parseHistoryTimestamp(value);
  if (!date) {
    return getCopy().unset;
  }
  return new Intl.DateTimeFormat(i18n.getLocale(), {
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  })
    .format(date)
    .replace(/\s+at\s+/i, ", ");
}

export function traceHeaderValue(current, name) {
  const target = String(name).toLowerCase();
  for (const header of current?.headers ?? []) {
    const headerName = Array.isArray(header) ? header[0] : header?.name;
    const headerValue = Array.isArray(header) ? header[1] : header?.value;
    if (String(headerName || "").toLowerCase() === target) {
      return String(headerValue || "").trim();
    }
  }
  return "";
}

export function traceHeadersText(current) {
  return (current?.headers ?? [])
    .map((header) => {
      const name = Array.isArray(header) ? header[0] : header?.name;
      const value = Array.isArray(header) ? header[1] : header?.value;
      return name ? `${name}: ${value ?? ""}` : "";
    })
    .filter(Boolean)
    .join("\n");
}

export function traceTextValue(value) {
  const text = String(value ?? "").trim();
  return text || getCopy().unset;
}

export function traceObjectValue(value) {
  if (!value) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value, null, 2);
}

export function traceContentClassification(current) {
  const reason = String(current?.reason || current?.status || "").toLowerCase();
  if (reason.includes("virus") || reason.includes("malware") || reason.includes("infected")) {
    return "Virus";
  }
  if (reason.includes("spam") || Number(current?.spam_score) >= 5) {
    return "Spam";
  }
  return humanizeStatus(current?.status || current?.queue);
}

export function traceBooleanLabel(value) {
  const copy = getCopy();
  return value ? copy.yes : copy.no;
}

export function tracePolicyFlag(address, policyKey) {
  const policy = state.dashboard?.policies?.address_policy ?? {};
  const values = Array.isArray(policy[policyKey]) ? policy[policyKey] : [];
  const normalized = displayMailAddress(address).toLowerCase();
  return normalized !== getCopy().unset.toLowerCase() && values.map((item) => String(item).toLowerCase()).includes(normalized);
}

export function traceMessageSize(current, fallbackEvent) {
  return current?.message_size_bytes ?? historySizeBytes(fallbackEvent);
}

export function traceAttachmentItems(current) {
  if (Array.isArray(current?.attachments)) {
    return current.attachments;
  }
  if (Array.isArray(current?.magika_summary?.attachments)) {
    return current.magika_summary.attachments;
  }
  return [];
}

export function formatShortDate(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return getCopy().unset;
  }
  return new Intl.DateTimeFormat(i18n.getLocale(), {
    month: "short",
    day: "numeric",
  }).format(date);
}

export function formatMetric(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "-";
  }
  return new Intl.NumberFormat(i18n.getLocale(), { notation: value >= 10000 ? "compact" : "standard" }).format(Number(value));
}

export function formatPercent(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return `${new Intl.NumberFormat(i18n.getLocale(), { maximumFractionDigits: 1 }).format(Number(value))}%`;
}

export function formatBytes(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let size = Math.max(0, Number(value));
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${new Intl.NumberFormat(i18n.getLocale(), { maximumFractionDigits: size >= 10 ? 0 : 1 }).format(size)} ${units[unitIndex]}`;
}

export function formatCompactBytes(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let size = Math.max(0, Number(value));
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${new Intl.NumberFormat(i18n.getLocale(), { maximumFractionDigits: size >= 10 ? 0 : 1 }).format(size)}${units[unitIndex]}`;
}

export function firstRecipient(item) {
  const recipient = (item?.rcpt_to ?? []).find(Boolean);
  return recipient ? displayMailAddress(recipient) : getCopy().unset;
}

export function humanizeStatus(value) {
  return String(value || getCopy().unset)
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function formatHistoryType(item) {
  const status = String(item?.status ?? "").toLowerCase();
  const queue = String(item?.queue ?? "").toLowerCase();
  const cleanStatuses = new Set(["accepted", "clean", "delivered", "incoming", "relayed", "sent"]);
  const label = cleanStatuses.has(status) || cleanStatuses.has(queue) ? "Clean" : humanizeStatus(item?.status);
  const score = Number(item?.spam_score);
  return Number.isNaN(score) ? label : `${label} (${score.toFixed(2)})`;
}

export function historyColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (item) => formatHistoryDateTime(item.latest_event_at), sortValue: (item) => parseHistoryTimestamp(item.latest_event_at)?.getTime() ?? 0 },
    { key: "lpeId", label: copy.historyColumnLpeId, value: (item) => displayTraceId(item.trace_id), sortValue: (item) => displayTraceId(item.trace_id).toLowerCase() },
    { key: "client", label: copy.historyColumnClientAddress, value: (item) => displayClientAddress(item.peer), sortValue: (item) => displayClientAddress(item.peer).toLowerCase() },
    { key: "type", label: copy.historyColumnType, value: (item) => formatHistoryType(item), sortValue: (item) => formatHistoryType(item).toLowerCase() },
    { key: "from", label: copy.historyColumnFrom, value: (item) => displayMailAddress(item.mail_from), sortValue: (item) => displayMailAddress(item.mail_from).toLowerCase() },
    { key: "to", label: copy.historyColumnTo, value: firstRecipient, sortValue: (item) => firstRecipient(item).toLowerCase() },
    { key: "size", label: copy.historyColumnSize, value: (item) => formatBytes(historySizeBytes(item)), sortValue: (item) => historySizeBytes(item) ?? -1 },
  ];
}

export function quarantineTraceId(item) {
  return String(item?.trace_id ?? "").trim();
}

export function quarantineDate(item) {
  return item?.received_at ?? item?.latest_event_at;
}

export function quarantineScoreValue(item) {
  const spamScore = Number(item?.spam_score);
  if (Number.isFinite(spamScore)) {
    return spamScore;
  }
  const securityScore = Number(item?.security_score);
  return Number.isFinite(securityScore) ? securityScore : null;
}

export function traceQueueCanBeDeleted(queue) {
  return ["incoming", "outbound", "deferred", "held", "quarantine", "bounces"].includes(String(queue || "").toLowerCase());
}

export function quarantineColumns(copy) {
  return [
    {
      key: "selected",
      label: copy.quarantineColumnSelection,
      value: (item) => (state.quarantineSelection.has(quarantineTraceId(item)) ? copy.yes : copy.no),
      sortValue: (item) => (state.quarantineSelection.has(quarantineTraceId(item)) ? 1 : 0),
    },
    { key: "from", label: copy.historyColumnFrom, value: (item) => displayMailAddress(item.mail_from), sortValue: (item) => displayMailAddress(item.mail_from).toLowerCase() },
    { key: "to", label: copy.historyColumnTo, value: firstRecipient, sortValue: (item) => firstRecipient(item).toLowerCase() },
    { key: "subject", label: copy.quarantineColumnSubject, value: (item) => item.subject || quarantineTraceId(item) || copy.unset, sortValue: (item) => String(item.subject || quarantineTraceId(item) || "").toLowerCase() },
    { key: "date", label: copy.historyColumnDate, value: (item) => formatHistoryDateTime(quarantineDate(item)), sortValue: (item) => parseHistoryTimestamp(quarantineDate(item))?.getTime() ?? 0 },
    { key: "score", label: copy.quarantineColumnScore, value: (item) => formatScore(quarantineScoreValue(item)), sortValue: (item) => quarantineScoreValue(item) ?? -1 },
  ];
}

export function quarantineGridTemplate() {
  return DEFAULT_QUARANTINE_COLUMN_WIDTHS.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

export function historyGridTemplate() {
  return state.historyColumnWidths.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

export function sortQuarantineItems(items, columns) {
  const column = columns.find((candidate) => candidate.key === state.quarantineSort.key) ?? columns[0];
  const direction = state.quarantineSort.direction === "asc" ? 1 : -1;
  return [...items].sort((left, right) => {
    const leftValue = column.sortValue(left);
    const rightValue = column.sortValue(right);
    if (typeof leftValue === "number" && typeof rightValue === "number") {
      return (leftValue - rightValue) * direction;
    }
    return String(leftValue).localeCompare(String(rightValue), i18n.getLocale(), { numeric: true, sensitivity: "base" }) * direction;
  });
}

export function sortHistoryItems(items, columns) {
  const column = columns.find((candidate) => candidate.key === state.historySort.key) ?? columns[0];
  const direction = state.historySort.direction === "asc" ? 1 : -1;
  return [...items].sort((left, right) => {
    const leftValue = column.sortValue(left);
    const rightValue = column.sortValue(right);
    if (typeof leftValue === "number" && typeof rightValue === "number") {
      return (leftValue - rightValue) * direction;
    }
    return String(leftValue).localeCompare(String(rightValue), i18n.getLocale(), { numeric: true, sensitivity: "base" }) * direction;
  });
}

export function quarantineSortIndicator(key) {
  if (state.quarantineSort.key !== key) {
    return "";
  }
  return state.quarantineSort.direction === "asc" ? " ▲" : " ▼";
}

export function sortIndicator(key) {
  if (state.historySort.key !== key) {
    return "";
  }
  return state.historySort.direction === "asc" ? " ▲" : " ▼";
}

export function setQuarantineSort(key) {
  state.quarantineSort = {
    key,
    direction: state.quarantineSort.key === key && state.quarantineSort.direction === "asc" ? "desc" : "asc",
  };
  renderQuarantine();
}

export function setHistorySort(key) {
  state.historySort = {
    key,
    direction: state.historySort.key === key && state.historySort.direction === "asc" ? "desc" : "asc",
  };
  renderHistory();
}

export function logTableState(tableId) {
  return state.logTables[tableId] ?? state.logTables.interface;
}

export function logGridTemplate(tableId) {
  const table = logTableState(tableId);
  return table.columnWidths.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

export function sortLogItems(tableId, items, columns) {
  const table = logTableState(tableId);
  const column = columns.find((candidate) => candidate.key === table.sort.key) ?? columns[0];
  const direction = table.sort.direction === "asc" ? 1 : -1;
  return [...items].sort((left, right) => {
    const leftValue = column.sortValue(left);
    const rightValue = column.sortValue(right);
    if (typeof leftValue === "number" && typeof rightValue === "number") {
      return (leftValue - rightValue) * direction;
    }
    return String(leftValue).localeCompare(String(rightValue), i18n.getLocale(), { numeric: true, sensitivity: "base" }) * direction;
  });
}

export function logSortIndicator(tableId, key) {
  const table = logTableState(tableId);
  if (table.sort.key !== key) {
    return "";
  }
  return table.sort.direction === "asc" ? " ▲" : " ▼";
}

export function setLogSort(tableId, key) {
  const table = logTableState(tableId);
  table.sort = {
    key,
    direction: table.sort.key === key && table.sort.direction === "asc" ? "desc" : "asc",
  };
  renderLogTableById(tableId);
}

export function renderLogTable({ tableId, container, columns, rows, emptyTitle, emptyMessage }) {
  if (!container) {
    return;
  }
  const table = logTableState(tableId);
  const gridTemplate = logGridTemplate(tableId);
  const sortedRows = sortLogItems(tableId, rows, columns);
  container.innerHTML = `
    <div class="log-table-header" style="--log-grid-columns: ${escapeHtml(gridTemplate)}">
      ${columns
        .map(
          (column, index) => `
            <span class="log-column-heading">
              <button type="button" data-action="log-sort" data-log-table="${escapeHtml(tableId)}" data-sort-key="${escapeHtml(column.key)}" aria-sort="${table.sort.key === column.key ? table.sort.direction : "none"}">${escapeHtml(column.label)}${escapeHtml(logSortIndicator(tableId, column.key))}</button>
              <span class="log-column-resizer" role="separator" aria-orientation="vertical" data-log-resizer data-log-table="${escapeHtml(tableId)}" data-column-index="${index}"></span>
            </span>
          `,
        )
        .join("")}
    </div>
    ${
      sortedRows.length
        ? sortedRows
            .map(
              (row) => `
                <div class="log-message-row" style="--log-grid-columns: ${escapeHtml(gridTemplate)}">
                  ${columns.map((column) => `<span>${escapeHtml(column.value(row))}</span>`).join("")}
                </div>
              `,
            )
            .join("")
        : buildEmptyState(emptyTitle, emptyMessage)
    }
  `;
}

export function auditColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "actor", label: copy.logColumnActor, value: (entry) => entry.actor ?? copy.unset, sortValue: (entry) => String(entry.actor ?? "").toLowerCase() },
    { key: "action", label: copy.logColumnAction, value: (entry) => entry.action ?? copy.unset, sortValue: (entry) => String(entry.action ?? "").toLowerCase() },
    { key: "details", label: copy.logColumnDetails, value: (entry) => entry.details ?? copy.unset, sortValue: (entry) => String(entry.details ?? "").toLowerCase() },
  ];
}

export function messageLogColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "level", label: copy.logColumnLevel, value: (entry) => entry.level ?? copy.unset, sortValue: (entry) => String(entry.level ?? "").toLowerCase() },
    { key: "source", label: copy.logColumnSource, value: (entry) => entry.source ?? copy.unset, sortValue: (entry) => String(entry.source ?? "").toLowerCase() },
    { key: "message", label: copy.logColumnMessage, value: (entry) => entry.message ?? copy.unset, sortValue: (entry) => String(entry.message ?? "").toLowerCase() },
  ];
}

export function emailAlertLogColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "recipient", label: copy.logColumnRecipient, value: (entry) => entry.recipient ?? copy.unset, sortValue: (entry) => String(entry.recipient ?? "").toLowerCase() },
    { key: "status", label: copy.logColumnStatus, value: (entry) => entry.status ?? copy.unset, sortValue: (entry) => String(entry.status ?? "").toLowerCase() },
    { key: "message", label: copy.logColumnMessage, value: (entry) => entry.message ?? copy.unset, sortValue: (entry) => String(entry.message ?? "").toLowerCase() },
  ];
}

export function hostLogDate(item) {
  return item.modified_at_unix_seconds
    ? formatHistoryDateTime(new Date(item.modified_at_unix_seconds * 1000).toISOString())
    : getCopy().unset;
}

export function hostLogColumns(copy) {
  return [
    { key: "select", label: copy.logColumnSelect },
    { key: "name", label: copy.logColumnName },
    { key: "date", label: copy.historyColumnDate },
    { key: "size", label: copy.historyColumnSize },
    { key: "actions", label: copy.logColumnActions },
  ];
}

export function hostLogActionButton({ action, category, item, label, iconClass, disabled = false, danger = false }) {
  return `
    <button class="icon-button table-icon-button${danger ? " danger-icon-button" : ""}" type="button" data-action="${escapeHtml(action)}" data-log-category="${escapeHtml(category)}" data-log-id="${escapeHtml(item.id)}" aria-label="${escapeHtml(`${label}: ${item.name}`)}" title="${escapeHtml(label)}"${disabled ? " disabled" : ""}>
      <span class="${escapeHtml(iconClass)}" aria-hidden="true"></span>
    </button>
  `;
}

export function renderHostLogTable({ tableId, container, rows, emptyTitle, emptyMessage }) {
  if (!container) {
    return;
  }
  const copy = getCopy();
  const columns = hostLogColumns(copy);
  const gridTemplate = logGridTemplate(tableId);
  container.innerHTML = `
    <div class="log-table-header host-log-table-header" style="--log-grid-columns: ${escapeHtml(gridTemplate)}">
      ${columns.map((column) => `<span class="log-column-heading">${escapeHtml(column.label)}</span>`).join("")}
    </div>
    ${
      rows.length
        ? rows
            .map(
              (item) => `
                <div class="log-message-row host-log-row" style="--log-grid-columns: ${escapeHtml(gridTemplate)}">
                  <span><input class="quarantine-select-checkbox" type="checkbox" aria-label="${escapeHtml(`${copy.logColumnSelect}: ${item.name}`)}" /></span>
                  <span title="${escapeHtml(item.name)}">${escapeHtml(item.name)}${item.exists ? "" : ` <small class="muted-inline">${escapeHtml(copy.logMissingFile)}</small>`}</span>
                  <span>${escapeHtml(hostLogDate(item))}</span>
                  <span>${escapeHtml(formatBytes(item.size_bytes))}</span>
                  <span class="table-action-icons">
                    ${hostLogActionButton({ action: "host-log-view", category: tableId, item, label: copy.logViewAction, iconClass: "action-icon action-icon-view", disabled: !item.previewable })}
                    ${hostLogActionButton({ action: "host-log-download", category: tableId, item, label: copy.logDownloadAction, iconClass: "action-icon action-icon-download", disabled: !item.downloadable })}
                    ${hostLogActionButton({ action: "host-log-delete", category: tableId, item, label: copy.logDeleteAction, iconClass: "close-icon", disabled: !item.deletable, danger: true })}
                  </span>
                </div>
              `,
            )
            .join("")
        : buildEmptyState(emptyTitle, emptyMessage)
    }
  `;
}

export function formatDurationMinutes(seconds) {
  if (seconds === undefined || seconds === null || Number.isNaN(Number(seconds))) {
    return getCopy().unset;
  }
  const minutes = Math.max(1, Math.round(Number(seconds) / 60));
  return `${formatNumber(minutes)} min`;
}

export function formatUptime(seconds) {
  if (seconds === undefined || seconds === null || Number.isNaN(Number(seconds))) {
    return getCopy().unset;
  }
  const totalSeconds = Math.max(0, Math.floor(Number(seconds)));
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (days > 0) {
    return `${formatNumber(days)}d ${formatNumber(hours)}h`;
  }
  if (hours > 0) {
    return `${formatNumber(hours)}h ${formatNumber(minutes)}m`;
  }
  return `${formatNumber(Math.max(1, minutes))}m`;
}

export function formatReportingUptime(seconds) {
  if (seconds === undefined || seconds === null || Number.isNaN(Number(seconds))) {
    return getCopy().unset;
  }
  const totalSeconds = Math.max(0, Math.floor(Number(seconds)));
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const time = `${String(hours).padStart(1, "0")}:${String(minutes).padStart(2, "0")}`;
  return days > 0 ? `${formatNumber(days)} days, ${time}` : time;
}

export function formatBooleanLabel(value) {
  return value ? getCopy().enabled : getCopy().disabled;
}

export function healthPosture(dashboard) {
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

export function getOperatorEmail() {
  return window.localStorage.getItem(LAST_ADMIN_EMAIL_KEY) || getCopy().unset;
}

export function getDigestSettings() {
  return state.reporting?.settings ?? state.dashboard?.reporting ?? null;
}

export function getTrafficRecords() {
  const records = [...(state.history ?? []), ...(state.quarantine ?? [])];
  const seen = new Set();
  return records.filter((item, index) => {
    const key = item?.trace_id || item?.current?.trace_id || `record-${index}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

export function getRelayOrPeer(item) {
  return item?.peer || item?.route_target || item?.current?.peer || item?.current?.route?.relay_target || getCopy().unset;
}

export function getPolicySignals() {
  return {
    verification: state.policyStatus?.recipient_verification ?? null,
    dkim: state.policyStatus?.dkim ?? null,
    reporting: getDigestSettings(),
  };
}


export function dedupeList(values) {
  return Array.from(new Set(values));
}

export function currentPolicies() {
  return structuredClone(state.dashboard?.policies ?? {});
}

export function currentReporting() {
  return structuredClone(state.reporting?.settings ?? state.dashboard?.reporting ?? {});
}

export function statusChipClass(value) {
  const normalized = typeof value === "string" ? value.toLowerCase() : value;
  if (normalized === true || normalized === "present" || normalized === "active" || normalized === "enabled" || normalized === "running" || normalized === "ok" || normalized === "ready") {
    return "status-chip ok";
  }
  if (normalized === false || normalized === "missing" || normalized === "disabled" || normalized === "misconfigured" || normalized === "failed" || normalized === "not-started" || normalized === "error") {
    return "status-chip danger";
  }
  if (normalized === "unreadable" || normalized === "invalid-path" || normalized === "degraded" || normalized === "unknown" || normalized === "not-configured" || normalized === "warn") {
    return "status-chip warn";
  }
  return "status-chip muted";
}

export function labelForAddressRole(role) {
  return role === "sender" ? getCopy().policyRoleSender : getCopy().policyRoleRecipient;
}

export function labelForAction(action) {
  return action === "allow" ? getCopy().policyActionAllow : getCopy().policyActionBlock;
}

export function labelForAttachmentScope(scope) {
  const copy = getCopy();
  if (scope === "extension") return copy.attachmentScopeExtension;
  if (scope === "mime") return copy.attachmentScopeMime;
  return copy.attachmentScopeDetected;
}

export function labelForVerificationBackend(backend) {
  return backend === "private-postgres" ? getCopy().cacheBackendPostgres : getCopy().cacheBackendMemory;
}

export function labelForKeyStatus(status) {
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
