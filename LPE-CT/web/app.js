import { i18n, getCopy, translate } from './modules/i18n/index.js';
import { DEFAULT_PAGE_ID, activatePageView, pageIdFromHash, renderPageModules } from "./modules/pages/index.js";

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
  pageViews: Array.from(document.querySelectorAll("[data-page-view]")),
  pageTabButtons: Array.from(document.querySelectorAll("[data-action='page-tab']")),
  pageTabPanels: Array.from(document.querySelectorAll("[data-page-tab-panel]")),
  refresh: document.getElementById("refresh"),
  refreshToolbar: document.getElementById("refresh-toolbar"),
  runDigests: document.getElementById("run-digests"),
  loginForm: document.getElementById("login-form"),
  quarantineSearchForm: document.getElementById("quarantine-search-form"),
  historySearchForm: document.getElementById("history-search-form"),
  createAddressRule: document.getElementById("create-address-rule"),
  createAttachmentRule: document.getElementById("create-attachment-rule"),
  editFilteringPolicy: document.getElementById("edit-filtering-policy"),
  editRecipientVerification: document.getElementById("edit-recipient-verification"),
  editDkimSettings: document.getElementById("edit-dkim-settings"),
  createDkimDomain: document.getElementById("create-dkim-domain"),
  editDigestSettings: document.getElementById("edit-digest-settings"),
  createDigestDefault: document.getElementById("create-digest-default"),
  createDigestOverride: document.getElementById("create-digest-override"),
  nodeName: document.getElementById("node-name"),
  heroSummary: document.getElementById("hero-summary"),
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
  systemOverviewList: document.getElementById("system-overview-list"),
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
  filteringPolicy: document.getElementById("filtering-policy-status"),
  addressRules: document.getElementById("address-rules-list"),
  attachmentRules: document.getElementById("attachment-rules-list"),
  recipientVerification: document.getElementById("recipient-verification-status"),
  dkimDomains: document.getElementById("dkim-domain-list"),
  digestSettings: document.getElementById("digest-settings-list"),
  digestDefaults: document.getElementById("digest-defaults-list"),
  digestOverrides: document.getElementById("digest-overrides-list"),
  digestReports: document.getElementById("digest-report-list"),
  systemInformation: document.getElementById("reporting-system-information"),
  platform: document.getElementById("platform-list"),
  mailLog: document.getElementById("mail-log"),
  audit: document.getElementById("audit-log"),
  messageLog: document.getElementById("message-log"),
  emailAlertLog: document.getElementById("email-alert-log"),
};

const AUTH_TOKEN_KEY = 'lpeCtAdminToken';
const LAST_ADMIN_EMAIL_KEY = 'lpeCtAdminLastEmail';
const DASHBOARD_REFRESH_INTERVAL_MS = 60_000;
const DEFAULT_QUARANTINE_COLUMN_WIDTHS = [112, 220, 220, 280, 172, 112];
const DEFAULT_HISTORY_COLUMN_WIDTHS = [172, 176, 144, 132, 220, 220, 112];
const MIN_HISTORY_COLUMN_WIDTH = 88;
const DEFAULT_LOG_COLUMN_WIDTHS = {
  mail: [56, 280, 172, 112, 132],
  interface: [56, 280, 172, 112, 132],
  messages: [56, 280, 172, 112, 132],
  emailAlerts: [172, 180, 132, 360],
};

// Application State
const state = {
  dashboard: null,
  quarantine: [],
  history: [],
  routeDiagnostics: null,
  reporting: null,
  digestReports: [],
  systemServices: [],
  policyStatus: null,
  hostLogs: {
    mail: [],
    interface: [],
    messages: [],
  },
  selectedTrace: null,
  pageTabs: {
    filtering: "content-filtering",
    "anti-spam": "settings",
    quarantine: "search",
    reporting: "system-information",
    logs: "interface",
  },
  systemSetup: {
    primaryTab: "network",
    nestedTabs: {
      network: "ip",
      mailRelay: "general",
      mailAuthentication: "spf",
    },
  },
  hostClockLoadedAt: null,
  loading: {
    dashboard: false,
    ops: false,
    auth: false,
    trace: false,
    runDigests: false,
  },
  activePage: DEFAULT_PAGE_ID,
  historySort: {
    key: "date",
    direction: "desc",
  },
  quarantineSort: {
    key: "date",
    direction: "desc",
  },
  quarantineSelection: new Set(),
  quarantineDialogTab: "details",
  historyColumnWidths: [...DEFAULT_HISTORY_COLUMN_WIDTHS],
  logTables: Object.fromEntries(
    Object.entries(DEFAULT_LOG_COLUMN_WIDTHS).map(([key, widths]) => [
      key,
      {
        sort: { key: "date", direction: "desc" },
        columnWidths: [...widths],
      },
    ]),
  ),
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

function formatDetailedScore(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return Number(value).toFixed(3);
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

function parseHistoryTimestamp(value) {
  if (!value) {
    return null;
  }
  const text = String(value);
  const unixValue = text.startsWith("unix:") ? Number(text.slice(5)) : Number.NaN;
  const date = Number.isFinite(unixValue) ? new Date(unixValue * 1000) : new Date(text);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatHistoryDateTime(value) {
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

function displayTraceId(value) {
  return String(value || getCopy().unset).replace(/^lpe-ct-(?:in|out)-/i, "");
}

function displayClientAddress(value) {
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

function displayMailAddress(value) {
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

function historySizeBytes(item) {
  const stored = Number(item?.message_size_bytes);
  if (Number.isFinite(stored) && stored >= 0) {
    return stored;
  }
  const sizeMatch = String(item?.mail_from || "").match(/\bSIZE=(\d+)\b/i);
  return sizeMatch ? Number(sizeMatch[1]) : null;
}

function formatLongTraceDateTime(value) {
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

function traceHeaderValue(current, name) {
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

function traceHeadersText(current) {
  return (current?.headers ?? [])
    .map((header) => {
      const name = Array.isArray(header) ? header[0] : header?.name;
      const value = Array.isArray(header) ? header[1] : header?.value;
      return name ? `${name}: ${value ?? ""}` : "";
    })
    .filter(Boolean)
    .join("\n");
}

function traceTextValue(value) {
  const text = String(value ?? "").trim();
  return text || getCopy().unset;
}

function traceObjectValue(value) {
  if (!value) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value, null, 2);
}

function traceContentClassification(current) {
  const reason = String(current?.reason || current?.status || "").toLowerCase();
  if (reason.includes("virus") || reason.includes("malware") || reason.includes("infected")) {
    return "Virus";
  }
  if (reason.includes("spam") || Number(current?.spam_score) >= 5) {
    return "Spam";
  }
  return humanizeStatus(current?.status || current?.queue);
}

function traceBooleanLabel(value) {
  const copy = getCopy();
  return value ? copy.yes : copy.no;
}

function tracePolicyFlag(address, policyKey) {
  const policy = state.dashboard?.policies?.address_policy ?? {};
  const values = Array.isArray(policy[policyKey]) ? policy[policyKey] : [];
  const normalized = displayMailAddress(address).toLowerCase();
  return normalized !== getCopy().unset.toLowerCase() && values.map((item) => String(item).toLowerCase()).includes(normalized);
}

function traceMessageSize(current, fallbackEvent) {
  return current?.message_size_bytes ?? historySizeBytes(fallbackEvent);
}

function traceAttachmentItems(current) {
  if (Array.isArray(current?.attachments)) {
    return current.attachments;
  }
  if (Array.isArray(current?.magika_summary?.attachments)) {
    return current.magika_summary.attachments;
  }
  return [];
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

function formatPercent(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  return `${new Intl.NumberFormat(i18n.getLocale(), { maximumFractionDigits: 1 }).format(Number(value))}%`;
}

function formatBytes(value) {
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

function formatCompactBytes(value) {
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

function firstRecipient(item) {
  const recipient = (item?.rcpt_to ?? []).find(Boolean);
  return recipient ? displayMailAddress(recipient) : getCopy().unset;
}

function humanizeStatus(value) {
  return String(value || getCopy().unset)
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatHistoryType(item) {
  const status = String(item?.status ?? "").toLowerCase();
  const queue = String(item?.queue ?? "").toLowerCase();
  const cleanStatuses = new Set(["accepted", "clean", "delivered", "incoming", "relayed", "sent"]);
  const label = cleanStatuses.has(status) || cleanStatuses.has(queue) ? "Clean" : humanizeStatus(item?.status);
  const score = Number(item?.spam_score);
  return Number.isNaN(score) ? label : `${label} (${score.toFixed(2)})`;
}

function historyColumns(copy) {
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

function quarantineTraceId(item) {
  return String(item?.trace_id ?? "").trim();
}

function quarantineDate(item) {
  return item?.received_at ?? item?.latest_event_at;
}

function quarantineScoreValue(item) {
  const spamScore = Number(item?.spam_score);
  if (Number.isFinite(spamScore)) {
    return spamScore;
  }
  const securityScore = Number(item?.security_score);
  return Number.isFinite(securityScore) ? securityScore : null;
}

function quarantineColumns(copy) {
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

function quarantineGridTemplate() {
  return DEFAULT_QUARANTINE_COLUMN_WIDTHS.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

function historyGridTemplate() {
  return state.historyColumnWidths.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

function sortQuarantineItems(items, columns) {
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

function sortHistoryItems(items, columns) {
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

function quarantineSortIndicator(key) {
  if (state.quarantineSort.key !== key) {
    return "";
  }
  return state.quarantineSort.direction === "asc" ? " ▲" : " ▼";
}

function sortIndicator(key) {
  if (state.historySort.key !== key) {
    return "";
  }
  return state.historySort.direction === "asc" ? " ▲" : " ▼";
}

function setQuarantineSort(key) {
  state.quarantineSort = {
    key,
    direction: state.quarantineSort.key === key && state.quarantineSort.direction === "asc" ? "desc" : "asc",
  };
  renderQuarantine();
}

function setHistorySort(key) {
  state.historySort = {
    key,
    direction: state.historySort.key === key && state.historySort.direction === "asc" ? "desc" : "asc",
  };
  renderHistory();
}

function logTableState(tableId) {
  return state.logTables[tableId] ?? state.logTables.interface;
}

function logGridTemplate(tableId) {
  const table = logTableState(tableId);
  return table.columnWidths.map((width) => `${Math.max(MIN_HISTORY_COLUMN_WIDTH, Number(width) || MIN_HISTORY_COLUMN_WIDTH)}px`).join(" ");
}

function sortLogItems(tableId, items, columns) {
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

function logSortIndicator(tableId, key) {
  const table = logTableState(tableId);
  if (table.sort.key !== key) {
    return "";
  }
  return table.sort.direction === "asc" ? " ▲" : " ▼";
}

function setLogSort(tableId, key) {
  const table = logTableState(tableId);
  table.sort = {
    key,
    direction: table.sort.key === key && table.sort.direction === "asc" ? "desc" : "asc",
  };
  renderLogTableById(tableId);
}

function renderLogTable({ tableId, container, columns, rows, emptyTitle, emptyMessage }) {
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

function auditColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "actor", label: copy.logColumnActor, value: (entry) => entry.actor ?? copy.unset, sortValue: (entry) => String(entry.actor ?? "").toLowerCase() },
    { key: "action", label: copy.logColumnAction, value: (entry) => entry.action ?? copy.unset, sortValue: (entry) => String(entry.action ?? "").toLowerCase() },
    { key: "details", label: copy.logColumnDetails, value: (entry) => entry.details ?? copy.unset, sortValue: (entry) => String(entry.details ?? "").toLowerCase() },
  ];
}

function messageLogColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "level", label: copy.logColumnLevel, value: (entry) => entry.level ?? copy.unset, sortValue: (entry) => String(entry.level ?? "").toLowerCase() },
    { key: "source", label: copy.logColumnSource, value: (entry) => entry.source ?? copy.unset, sortValue: (entry) => String(entry.source ?? "").toLowerCase() },
    { key: "message", label: copy.logColumnMessage, value: (entry) => entry.message ?? copy.unset, sortValue: (entry) => String(entry.message ?? "").toLowerCase() },
  ];
}

function emailAlertLogColumns(copy) {
  return [
    { key: "date", label: copy.historyColumnDate, value: (entry) => formatHistoryDateTime(entry.timestamp), sortValue: (entry) => parseHistoryTimestamp(entry.timestamp)?.getTime() ?? 0 },
    { key: "recipient", label: copy.logColumnRecipient, value: (entry) => entry.recipient ?? copy.unset, sortValue: (entry) => String(entry.recipient ?? "").toLowerCase() },
    { key: "status", label: copy.logColumnStatus, value: (entry) => entry.status ?? copy.unset, sortValue: (entry) => String(entry.status ?? "").toLowerCase() },
    { key: "message", label: copy.logColumnMessage, value: (entry) => entry.message ?? copy.unset, sortValue: (entry) => String(entry.message ?? "").toLowerCase() },
  ];
}

function hostLogDate(item) {
  return item.modified_at_unix_seconds
    ? formatHistoryDateTime(new Date(item.modified_at_unix_seconds * 1000).toISOString())
    : getCopy().unset;
}

function hostLogColumns(copy) {
  return [
    { key: "select", label: copy.logColumnSelect },
    { key: "name", label: copy.logColumnName },
    { key: "date", label: copy.historyColumnDate },
    { key: "size", label: copy.historyColumnSize },
    { key: "actions", label: copy.logColumnActions },
  ];
}

function hostLogActionButton({ action, category, item, label, iconClass, disabled = false, danger = false }) {
  return `
    <button class="icon-button table-icon-button${danger ? " danger-icon-button" : ""}" type="button" data-action="${escapeHtml(action)}" data-log-category="${escapeHtml(category)}" data-log-id="${escapeHtml(item.id)}" aria-label="${escapeHtml(`${label}: ${item.name}`)}" title="${escapeHtml(label)}"${disabled ? " disabled" : ""}>
      <span class="${escapeHtml(iconClass)}" aria-hidden="true"></span>
    </button>
  `;
}

function renderHostLogTable({ tableId, container, rows, emptyTitle, emptyMessage }) {
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

function formatDurationMinutes(seconds) {
  if (seconds === undefined || seconds === null || Number.isNaN(Number(seconds))) {
    return getCopy().unset;
  }
  const minutes = Math.max(1, Math.round(Number(seconds) / 60));
  return `${formatNumber(minutes)} min`;
}

function formatUptime(seconds) {
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

function formatReportingUptime(seconds) {
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

async function fetchBlob(path, init = {}) {
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
  return response.blob();
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

async function postJson(path, payload = null) {
  return fetchJson(path, {
    method: "POST",
    headers: payload ? { "Content-Type": "application/json" } : {},
    body: payload ? JSON.stringify(payload) : undefined,
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
  if (value === true || value === "present" || value === "active" || value === "enabled" || value === "running" || value === "ok") {
    return "status-chip ok";
  }
  if (value === false || value === "missing" || value === "disabled" || value === "misconfigured" || value === "failed" || value === "not-started") {
    return "status-chip danger";
  }
  if (value === "unreadable" || value === "invalid-path" || value === "degraded" || value === "unknown" || value === "not-configured") {
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

function renderDrawerContent(title, summary, content, opener = document.activeElement, onClose = null, variant = "") {
  state.drawer.previousFocus = opener instanceof HTMLElement ? opener : null;
  state.drawer.onClose = onClose;
  state.drawer.open = true;
  elements.drawer.classList.toggle("drawer-wide", variant === "wide");
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
  elements.drawer.classList.remove("drawer-wide");
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
  if (element) {
    element.textContent = formatMetric(value);
  }
}

function setText(element, value) {
  if (element) {
    element.textContent = value;
  }
}

function setClassName(element, value) {
  if (element) {
    element.className = value;
  }
}

function setAuthenticated(authenticated) {
  elements.consoleShell.classList.toggle("hidden", !authenticated);
  elements.loginShell.classList.toggle("hidden", authenticated);
}

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

function selectedQuarantineItems() {
  return state.quarantine.filter((item) => state.quarantineSelection.has(quarantineTraceId(item)));
}

function pruneQuarantineSelection() {
  const currentIds = new Set(state.quarantine.map(quarantineTraceId).filter(Boolean));
  state.quarantineSelection.forEach((traceId) => {
    if (!currentIds.has(traceId)) {
      state.quarantineSelection.delete(traceId);
    }
  });
}

// Renderers
function renderQuarantine() {
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

function renderHistory() {
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

function renderFilteringPolicy() {
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

function formatSystemResource(percentValue, totalBytes) {
  const percent = formatPercent(percentValue);
  const total = formatCompactBytes(totalBytes);
  if (percent === getCopy().unset && total === getCopy().unset) {
    return getCopy().unset;
  }
  if (total === getCopy().unset) {
    return percent;
  }
  if (percent === getCopy().unset) {
    return total;
  }
  return `${percent} ${percent} of ${total}`;
}

function renderSystemInformation() {
  const copy = getCopy();
  const container = containers.systemInformation;
  if (!container) {
    return;
  }
  const dashboard = state.dashboard;
  if (!dashboard) {
    container.innerHTML = buildLoadingRows(4);
    return;
  }
  const system = getRuntimeSystem(dashboard);
  const loadAverages = system.load_averages || system.loadAverages || [];
  const systemRows = [
    { label: copy.systemProcessorType, value: system.processor_type || system.processor_model || copy.unset },
    {
      label: copy.systemMemory,
      value: formatSystemResource(system.memory_used_percent ?? system.memory?.used_percent, system.memory_total_bytes ?? system.memory?.total_bytes),
    },
    {
      label: copy.systemMailLogDiskSpace,
      value: formatSystemResource(system.disk_used_percent ?? system.disk?.used_percent, system.disk_total_bytes ?? system.disk?.total_bytes),
    },
    { label: copy.systemUptime, value: formatReportingUptime(system.uptime_seconds) },
    {
      label: copy.systemLoadAverages,
      value: loadAverages.length >= 3 ? loadAverages.slice(0, 3).map((value) => Number(value).toFixed(2)).join(", ") : copy.unset,
    },
  ];
  const services = state.systemServices.length
    ? state.systemServices
    : [
        { id: "antivirus", name: copy.serviceAntivirus, status: "unknown", action: "start" },
        { id: "lpe-ct", name: copy.serviceLpeCt, status: "unknown", action: "start" },
      ];
  const diagnostics = [
    { kind: "spam-test", label: copy.systemSpamTest, upload: true },
    { kind: "mail-queue", label: copy.systemMailQueue },
    { kind: "process-list", label: copy.systemProcessList },
    { kind: "network-connections", label: copy.systemNetworkConnections },
    { kind: "routing-table", label: copy.systemRoutingTable },
  ];
  const tools = [
    { tool: "ping", label: copy.systemToolPing, placeholder: "mail.example.test" },
    { tool: "traceroute", label: copy.systemToolTraceroute, placeholder: "mail.example.test" },
    { tool: "dig", label: copy.systemToolDig, placeholder: "example.test" },
  ];

  container.innerHTML = `
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.reportingTabSystemInformation)}</h4>
      </div>
      ${renderDashboardDetailTable(systemRows)}
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemServicesTitle)}</h4>
      </div>
      <div class="management-list-stack">
        ${services.map((service) => renderServiceRow(service, copy)).join("")}
      </div>
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemDiagnosticsTitle)}</h4>
      </div>
      <div class="management-list-stack">
        ${diagnostics.map((diagnostic) => renderDiagnosticRow(diagnostic, copy)).join("")}
        ${renderSimpleActionRow(copy.systemSupportConnect, copy.connect, "support-connect")}
        ${renderSimpleActionRow(copy.systemHealthCheck, copy.run, "health-check-run")}
      </div>
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemToolsTitle)}</h4>
      </div>
      <div class="management-list-stack">
        ${tools.map((tool) => renderToolRow(tool, copy)).join("")}
        ${renderSimpleActionRow(copy.systemFlushMailQueue, copy.flush, "flush-mail-queue")}
      </div>
    </section>
  `;
}

function renderServiceRow(service, copy) {
  const status = humanizeStatus(service.status);
  const action = service.action === "stop" ? "stop" : "start";
  const actionLabel = action === "stop" ? copy.stop : copy.start;
  return `
    <article class="record-row system-report-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${escapeHtml(service.name)}</h4>
          <div class="record-copy">${escapeHtml(service.unit || copy.unset)}</div>
        </div>
        <span class="${statusChipClass(service.status)}">${escapeHtml(status)}</span>
      </div>
      <div class="record-actions">
        <button class="secondary-button compact-button" type="button" data-action="system-service-action" data-service-id="${escapeHtml(service.id)}" data-service-action="${escapeHtml(action)}">${escapeHtml(actionLabel)}</button>
      </div>
    </article>
  `;
}

function renderDiagnosticRow(diagnostic, copy) {
  return `
    <article class="record-row system-report-row">
      <div class="record-head">
        <div>
          <h4 class="record-title">${escapeHtml(diagnostic.label)}</h4>
          ${diagnostic.upload ? `<input id="spam-test-file" class="system-file-input" type="file" />` : ""}
        </div>
      </div>
      <div class="record-actions">
        <button class="secondary-button compact-button" type="button" data-action="${diagnostic.upload ? "spam-test-show" : "diagnostic-show"}" data-diagnostic-kind="${escapeHtml(diagnostic.kind)}">${escapeHtml(copy.show)}</button>
      </div>
    </article>
  `;
}

function renderSimpleActionRow(label, buttonLabel, action) {
  return `
    <article class="record-row system-report-row">
      <div class="record-head">
        <h4 class="record-title">${escapeHtml(label)}</h4>
      </div>
      <div class="record-actions">
        <button class="secondary-button compact-button" type="button" data-action="${escapeHtml(action)}">${escapeHtml(buttonLabel)}</button>
      </div>
    </article>
  `;
}

function renderToolRow(tool, copy) {
  return `
    <article class="record-row system-tool-row">
      <label>
        <span>${escapeHtml(tool.label)}</span>
        <input id="diagnostic-tool-${escapeHtml(tool.tool)}" type="text" autocomplete="off" placeholder="${escapeHtml(tool.placeholder)}" />
      </label>
      <div class="record-actions">
        <button class="secondary-button compact-button" type="button" data-action="diagnostic-tool-run" data-diagnostic-tool="${escapeHtml(tool.tool)}">${escapeHtml(copy.run)}</button>
      </div>
    </article>
  `;
}

function systemSetupEmptyState(title, summary) {
  return `
    <article class="empty-state compact-empty-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(summary)}</p>
    </article>
  `;
}

function renderSystemSetupTabs(tabs, activeTab, level = "primary") {
  return `
    <div class="tab-strip ${level === "secondary" ? "tab-strip-secondary" : ""}" role="tablist">
      ${tabs
        .map((tab) => {
          const isActive = tab.id === activeTab;
          return `
            <button
              class="tab-button${isActive ? " tab-button-active" : ""}"
              type="button"
              role="tab"
              aria-selected="${String(isActive)}"
              data-action="system-setup-tab"
              data-tab-level="${level}"
              data-tab-id="${escapeHtml(tab.id)}"
            >${escapeHtml(tab.label)}</button>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderSystemSetupPanel(title, summary, body, actions = "") {
  return `
    <article class="record-row setup-panel">
      <div class="record-head">
        <div>
          <h4 class="record-title">${escapeHtml(title)}</h4>
          <div class="record-copy">${escapeHtml(summary)}</div>
        </div>
        ${actions ? `<div class="inline-actions">${actions}</div>` : ""}
      </div>
      ${body}
    </article>
  `;
}

function renderSystemSetupSummary(items) {
  return `
    <div class="record-grid">
      ${items
        .map(
          (item) => `
            <div class="summary-card">
              <p>${escapeHtml(item.label)}</p>
              <strong>${escapeHtml(item.value ?? getCopy().unset)}</strong>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function systemNetworkInterfaces(dashboard) {
  const candidates = [
    dashboard.system?.network_interfaces,
    dashboard.system?.interfaces,
    dashboard.network?.interfaces,
  ];
  return candidates.find((candidate) => Array.isArray(candidate)) ?? [];
}

function renderNetworkInterfaces(dashboard, copy) {
  const interfaces = systemNetworkInterfaces(dashboard);
  if (!interfaces.length) {
    return systemSetupEmptyState(copy.networkInterfacesTitle, copy.networkInterfacesUnavailable);
  }

  return `
    <section class="network-interface-panel">
      <h5>${escapeHtml(copy.networkInterfacesTitle)}</h5>
      <div class="data-table-wrap network-interface-table-wrap">
        <table class="data-table network-interface-table">
          <thead>
            <tr>
              <th scope="col">${escapeHtml(copy.networkInterfaceNameLabel)}</th>
              <th scope="col">${escapeHtml(copy.networkInterfaceAddressLabel)}</th>
              <th scope="col">${escapeHtml(copy.networkInterfaceNetmaskLabel)}</th>
              <th scope="col">${escapeHtml(copy.networkInterfaceGatewayLabel)}</th>
            </tr>
          </thead>
          <tbody>
            ${interfaces
              .map((item) => {
                const gateway =
                  item.default_gateway ?? item.gateway ?? item.route_gateway ?? copy.unset;
                return `
                  <tr>
                    <th scope="row">${escapeHtml(item.name ?? item.interface ?? item.iface ?? copy.unset)}</th>
                    <td>${escapeHtml(item.address ?? item.ip_address ?? item.ip ?? copy.unset)}</td>
                    <td>${escapeHtml(item.netmask ?? item.subnet_mask ?? item.prefix ?? copy.unset)}</td>
                    <td>${escapeHtml(gateway)}</td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderNetworkSetup(activeTab, dashboard, copy) {
  const tabs = [
    { id: "ip", label: copy.systemSetupNetworkIp },
    { id: "dns", label: copy.systemSetupNetworkDns },
    { id: "static-routes", label: copy.systemSetupNetworkStaticRoutes },
    { id: "ipv6", label: copy.systemSetupNetworkIpv6 },
  ];
  const editNetwork = `<button class="list-action" type="button" data-action="platform-edit" data-target="network">${copy.edit}</button>`;
  const editSite = `<button class="list-action" type="button" data-action="platform-edit" data-target="site">${copy.edit}</button>`;
  const bodies = {
    ip: renderSystemSetupPanel(
      copy.systemSetupNetworkIp,
      copy.systemSetupNetworkIpSummary,
      `${renderSystemSetupSummary([
        { label: copy.sitePublicSmtpLabel, value: dashboard.site.public_smtp_bind },
        { label: copy.siteManagementBindLabel, value: dashboard.site.management_bind },
        { label: copy.networkPublicListenerLabel, value: dashboard.network.public_listener_enabled ? copy.enabled : copy.disabled },
        { label: copy.networkSubmissionListenerLabel, value: dashboard.network.submission_listener_enabled ? copy.enabled : copy.disabled },
        { label: copy.networkConcurrentLabel, value: formatNumber(dashboard.network.max_concurrent_sessions) },
        { label: copy.networkProxyProtocolLabel, value: dashboard.network.proxy_protocol_enabled ? copy.enabled : copy.disabled },
      ])}${renderNetworkInterfaces(dashboard, copy)}`,
      editNetwork,
    ),
    dns: renderSystemSetupPanel(
      copy.systemSetupNetworkDns,
      copy.systemSetupNetworkDnsSummary,
      renderSystemSetupSummary([
        { label: copy.sitePublishedMxLabel, value: dashboard.site.published_mx },
        { label: copy.siteManagementFqdnLabel, value: dashboard.site.management_fqdn },
        { label: copy.siteDmzZoneLabel, value: dashboard.site.dmz_zone },
      ]),
      editSite,
    ),
    "static-routes": renderSystemSetupPanel(
      copy.systemSetupNetworkStaticRoutes,
      copy.systemSetupNetworkStaticRoutesSummary,
      systemSetupEmptyState(copy.systemSetupNetworkStaticRoutes, copy.systemSetupNoStaticRoutes),
    ),
    ipv6: renderSystemSetupPanel(
      copy.systemSetupNetworkIpv6,
      copy.systemSetupNetworkIpv6Summary,
      systemSetupEmptyState(copy.systemSetupNetworkIpv6, copy.systemSetupNoIpv6),
    ),
  };
  return renderSystemSetupTabs(tabs, activeTab, "secondary") + bodies[activeTab];
}

function formatVerificationType(value) {
  const copy = getCopy();
  switch (String(value ?? "").toLowerCase()) {
    case "dynamic":
      return copy.acceptedDomainVerificationDynamic;
    case "ldap":
      return copy.acceptedDomainVerificationLdap;
    case "allowed":
      return copy.acceptedDomainVerificationAllowed;
    default:
      return copy.acceptedDomainVerificationNone;
  }
}

function renderBooleanCell(value) {
  const copy = getCopy();
  return `<span class="${statusChipClass(Boolean(value))}">${escapeHtml(value ? copy.yes : copy.no)}</span>`;
}

function publicTlsSettings(dashboard = state.dashboard) {
  return dashboard?.network?.public_tls ?? { active_profile_id: null, profiles: [] };
}

function publicTlsActiveProfile(settings = publicTlsSettings()) {
  return (settings.profiles ?? []).find((profile) => profile.id === settings.active_profile_id) ?? null;
}

function publicTlsStatusLabel(settings = publicTlsSettings()) {
  const copy = getCopy();
  return publicTlsActiveProfile(settings) ? copy.enabled : copy.disabled;
}

function renderPublicTlsProfiles(dashboard, copy) {
  const settings = publicTlsSettings(dashboard);
  const profiles = settings.profiles ?? [];
  const rows = profiles.length
    ? `
      <div class="data-table-wrap public-tls-table-wrap">
        <table class="data-table public-tls-table">
          <thead>
            <tr>
              <th scope="col">${escapeHtml(copy.publicTlsProfileNameLabel)}</th>
              <th scope="col">${escapeHtml(copy.publicTlsCertPathLabel)}</th>
              <th scope="col">${escapeHtml(copy.publicTlsKeyPathLabel)}</th>
              <th scope="col">${escapeHtml(copy.statusLabel)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnOptions)}</th>
            </tr>
          </thead>
          <tbody>
            ${profiles
              .map((profile) => {
                const active = profile.id === settings.active_profile_id;
                return `
                  <tr>
                    <th scope="row">${escapeHtml(profile.name)}</th>
                    <td><span class="record-copy">${escapeHtml(profile.cert_path)}</span></td>
                    <td><span class="record-copy">${escapeHtml(profile.key_path)}</span></td>
                    <td><span class="${statusChipClass(active)}">${escapeHtml(active ? copy.active : copy.inactive)}</span></td>
                    <td>
                      <div class="table-action-icons">
                        <button class="icon-button table-icon-button" type="button" data-action="public-tls-select" data-profile-id="${escapeHtml(profile.id)}" aria-label="${escapeHtml(copy.publicTlsSelectAction)}" title="${escapeHtml(copy.publicTlsSelectAction)}"><span class="action-icon action-icon-test" aria-hidden="true"></span></button>
                        <button class="icon-button table-icon-button danger-icon-button" type="button" data-action="public-tls-delete" data-profile-id="${escapeHtml(profile.id)}" aria-label="${escapeHtml(copy.remove)}" title="${escapeHtml(copy.remove)}"><span class="action-icon action-icon-delete" aria-hidden="true"></span></button>
                      </div>
                    </td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    `
    : systemSetupEmptyState(copy.publicTlsProfilesTitle, copy.publicTlsNoProfiles);
  return `
    ${rows}
    <div class="list-footer-actions">
      ${settings.active_profile_id ? `<button class="secondary-button compact-button" type="button" data-action="public-tls-disable">${copy.publicTlsDisableAction}</button>` : ""}
      <button class="primary-button compact-button" type="button" data-action="public-tls-upload">${copy.publicTlsUploadAction}</button>
    </div>
  `;
}

function renderAcceptedDomainsTable(dashboard, copy) {
  const domains = dashboard.accepted_domains ?? [];
  const table = domains.length
    ? `
      <div class="data-table-wrap accepted-domain-table-wrap">
        <table class="data-table accepted-domain-table">
          <thead>
            <tr>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnDomain)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnDestination)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnVerification)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnRbl)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnSpf)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnGreylisting)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnNullReversePath)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnVerified)}</th>
              <th scope="col">${escapeHtml(copy.acceptedDomainColumnOptions)}</th>
            </tr>
          </thead>
          <tbody>
            ${domains
              .map(
                (domain) => `
                  <tr>
                    <th scope="row">${escapeHtml(domain.domain)}</th>
                    <td>${escapeHtml(domain.destination_server || copy.unset)}</td>
                    <td>${escapeHtml(formatVerificationType(domain.verification_type))}</td>
                    <td>${renderBooleanCell(domain.rbl_checks)}</td>
                    <td>${renderBooleanCell(domain.spf_checks)}</td>
                    <td>${renderBooleanCell(domain.greylisting)}</td>
                    <td>${renderBooleanCell(domain.accept_null_reverse_path ?? true)}</td>
                    <td>${renderBooleanCell(domain.verified)}</td>
                    <td>
                      <div class="table-action-icons">
                        <button class="icon-button table-icon-button" type="button" data-action="accepted-domain-edit" data-domain-id="${escapeHtml(domain.id)}" aria-label="${escapeHtml(copy.edit)}" title="${escapeHtml(copy.edit)}"><span class="action-icon action-icon-edit" aria-hidden="true"></span></button>
                        <button class="icon-button table-icon-button" type="button" data-action="accepted-domain-test" data-domain-id="${escapeHtml(domain.id)}" aria-label="${escapeHtml(copy.test)}" title="${escapeHtml(copy.test)}"><span class="action-icon action-icon-test" aria-hidden="true"></span></button>
                        <button class="icon-button table-icon-button danger-icon-button" type="button" data-action="accepted-domain-delete" data-domain-id="${escapeHtml(domain.id)}" aria-label="${escapeHtml(copy.remove)}" title="${escapeHtml(copy.remove)}"><span class="action-icon action-icon-delete" aria-hidden="true"></span></button>
                      </div>
                    </td>
                  </tr>
                `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `
    : systemSetupEmptyState(copy.systemSetupRelayDomains, copy.noAcceptedDomains);
  return `
    ${table}
    <div class="list-footer-actions">
      <button class="secondary-button compact-button" type="button" data-action="accepted-domain-import">${copy.import}</button>
      <button class="primary-button compact-button" type="button" data-action="accepted-domain-create">${copy.add}</button>
    </div>
  `;
}

function renderMailRelaySetup(activeTab, dashboard, copy) {
  const tabs = [
    { id: "general", label: copy.systemSetupRelayGeneral },
    { id: "domains", label: copy.systemSetupRelayDomains },
    { id: "ip-controls", label: copy.systemSetupRelayIpControls },
    { id: "sender-controls", label: copy.systemSetupRelaySenderControls },
    { id: "outbound", label: copy.systemSetupRelayOutbound },
    { id: "smtp-settings", label: copy.systemSetupRelaySmtpSettings },
    { id: "esmtp-settings", label: copy.systemSetupRelayEsmtpSettings },
    { id: "greylisting", label: copy.systemSetupRelayGreylisting },
    { id: "dkim-signing", label: copy.systemSetupRelayDkimSigning },
  ];
  const editRelay = `<button class="list-action" type="button" data-action="platform-edit" data-target="relay">${copy.edit}</button>`;
  const editNetwork = `<button class="list-action" type="button" data-action="platform-edit" data-target="network">${copy.edit}</button>`;
  const goFiltering = `<button class="list-action" type="button" data-page-target="filtering">${copy.navFiltering}</button>`;
  const dkimDomains = dashboard.policies?.dkim?.domains ?? [];
  const bodies = {
    general: renderSystemSetupPanel(
      copy.systemSetupRelayGeneral,
      copy.systemSetupRelayGeneralSummary,
      renderSystemSetupSummary([
        { label: copy.relayHaLabel, value: dashboard.relay.ha_enabled ? copy.enabled : copy.disabled },
        { label: copy.relayPrimaryLabel, value: dashboard.relay.primary_upstream },
        { label: copy.relaySecondaryLabel, value: dashboard.relay.secondary_upstream },
        { label: copy.relaySyncLabel, value: formatNumber(dashboard.relay.sync_interval_seconds) },
      ]),
      editRelay,
    ),
    domains: renderSystemSetupPanel(
      copy.systemSetupRelayDomains,
      copy.systemSetupRelayDomainsSummary,
      renderAcceptedDomainsTable(dashboard, copy),
    ),
    "ip-controls": renderSystemSetupPanel(
      copy.systemSetupRelayIpControls,
      copy.systemSetupRelayIpControlsSummary,
      renderSystemSetupSummary([
        { label: copy.networkManagementCidrsLabel, value: formatList(dashboard.network.allowed_management_cidrs) },
        { label: copy.networkUpstreamCidrsLabel, value: formatList(dashboard.network.allowed_upstream_cidrs) },
      ]),
      editNetwork,
    ),
    "sender-controls": renderSystemSetupPanel(
      copy.systemSetupRelaySenderControls,
      copy.systemSetupRelaySenderControlsSummary,
      systemSetupEmptyState(copy.systemSetupRelaySenderControls, copy.systemSetupSenderControlsNote),
      goFiltering,
    ),
    outbound: renderSystemSetupPanel(
      copy.systemSetupRelayOutbound,
      copy.systemSetupRelayOutboundSummary,
      renderSystemSetupSummary([
        { label: copy.networkSmartHostsLabel, value: formatList(dashboard.network.outbound_smart_hosts) },
        { label: copy.relayCoreDeliveryLabel, value: dashboard.relay.core_delivery_base_url },
        { label: copy.relayFallbackLabel, value: dashboard.relay.fallback_to_hold_queue ? copy.enabled : copy.disabled },
      ]),
      `${editRelay}${editNetwork}`,
    ),
    "smtp-settings": renderSystemSetupPanel(
      copy.systemSetupRelaySmtpSettings,
      copy.systemSetupRelaySmtpSettingsSummary,
      `${renderSystemSetupSummary([
        { label: copy.networkPublicListenerLabel, value: dashboard.network.public_listener_enabled ? copy.enabled : copy.disabled },
        { label: copy.publicTlsStarttlsLabel, value: publicTlsStatusLabel(publicTlsSettings(dashboard)) },
        { label: copy.networkSubmissionListenerLabel, value: dashboard.network.submission_listener_enabled ? copy.enabled : copy.disabled },
        { label: copy.networkConcurrentLabel, value: formatNumber(dashboard.network.max_concurrent_sessions) },
      ])}${renderPublicTlsProfiles(dashboard, copy)}`,
      editNetwork,
    ),
    "esmtp-settings": renderSystemSetupPanel(
      copy.systemSetupRelayEsmtpSettings,
      copy.systemSetupRelayEsmtpSettingsSummary,
      renderSystemSetupSummary([
        { label: copy.networkProxyProtocolLabel, value: dashboard.network.proxy_protocol_enabled ? copy.enabled : copy.disabled },
        { label: copy.relayMutualTlsLabel, value: dashboard.relay.mutual_tls_required ? copy.enabled : copy.disabled },
      ]),
      `${editNetwork}${editRelay}`,
    ),
    greylisting: renderSystemSetupPanel(
      copy.systemSetupRelayGreylisting,
      copy.systemSetupRelayGreylistingSummary,
      systemSetupEmptyState(copy.systemSetupRelayGreylisting, copy.systemSetupNoGreylisting),
    ),
    "dkim-signing": renderSystemSetupPanel(
      copy.systemSetupRelayDkimSigning,
      copy.systemSetupRelayDkimSigningSummary,
      renderSystemSetupSummary([
        { label: copy.dkimSigningEnabledLabel, value: dashboard.policies?.dkim?.enabled ? copy.enabled : copy.disabled },
        { label: copy.dkimHeaders, value: formatList(dashboard.policies?.dkim?.headers) },
        { label: copy.dkimDomainLabel, value: dkimDomains.length ? formatNumber(dkimDomains.length) : copy.noDkimDomains },
      ]),
      goFiltering,
    ),
  };
  return renderSystemSetupTabs(tabs, activeTab, "secondary") + bodies[activeTab];
}

function renderMailAuthenticationSetup(activeTab, dashboard, copy) {
  const tabs = [
    { id: "spf", label: copy.systemSetupAuthSpf },
    { id: "dkim", label: copy.systemSetupAuthDkim },
    { id: "dmarc", label: copy.systemSetupAuthDmarc },
    { id: "arc", label: copy.systemSetupAuthArc },
  ];
  const goFiltering = `<button class="list-action" type="button" data-page-target="filtering">${copy.navFiltering}</button>`;
  const bodies = {
    spf: renderSystemSetupPanel(
      copy.systemSetupAuthSpf,
      copy.systemSetupAuthSpfSummary,
      systemSetupEmptyState(copy.systemSetupAuthSpf, copy.systemSetupNoSpf),
    ),
    dkim: renderSystemSetupPanel(
      copy.systemSetupAuthDkim,
      copy.systemSetupAuthDkimSummary,
      renderSystemSetupSummary([
        { label: copy.dkimSigningEnabledLabel, value: dashboard.policies?.dkim?.enabled ? copy.enabled : copy.disabled },
        { label: copy.dkimHeaders, value: formatList(dashboard.policies?.dkim?.headers) },
        { label: copy.dkimExpiration, value: dashboard.policies?.dkim?.expiration_seconds ? formatNumber(dashboard.policies.dkim.expiration_seconds) : copy.unset },
      ]),
      goFiltering,
    ),
    dmarc: renderSystemSetupPanel(
      copy.systemSetupAuthDmarc,
      copy.systemSetupAuthDmarcSummary,
      systemSetupEmptyState(copy.systemSetupAuthDmarc, copy.systemSetupNoDmarc),
    ),
    arc: renderSystemSetupPanel(
      copy.systemSetupAuthArc,
      copy.systemSetupAuthArcSummary,
      systemSetupEmptyState(copy.systemSetupAuthArc, copy.systemSetupNoArc),
    ),
  };
  return renderSystemSetupTabs(tabs, activeTab, "secondary") + bodies[activeTab];
}

function renderPlatform() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    containers.platform.innerHTML = buildLoadingRows(2);
    return;
  }
  const primaryTabs = [
    { id: "network", label: copy.systemSetupNetwork },
    { id: "time", label: copy.systemSetupTime },
    { id: "mailRelay", label: copy.systemSetupMailRelay },
    { id: "mailAuthentication", label: copy.systemSetupMailAuthentication },
    { id: "systemUpdates", label: copy.systemSetupSystemUpdates },
    { id: "shutdownRestart", label: copy.systemSetupShutdownRestart },
  ];
  const activePrimary = state.systemSetup.primaryTab;
  const nested = state.systemSetup.nestedTabs;
  const panels = {
    network: renderNetworkSetup(nested.network, dashboard, copy),
    time: renderSystemSetupPanel(
      copy.systemSetupTime,
      copy.systemSetupTimeSummary,
      renderSystemSetupSummary([
        { label: copy.sessionTimeLabel, value: formatDateTime(getHostClockDate()) },
        { label: copy.systemHostname, value: dashboard.system?.hostname || dashboard.site?.node_name || copy.unset },
        { label: copy.systemUptime, value: formatUptime(dashboard.system?.uptime_seconds) },
      ]),
    ),
    mailRelay: renderMailRelaySetup(nested.mailRelay, dashboard, copy),
    mailAuthentication: renderMailAuthenticationSetup(nested.mailAuthentication, dashboard, copy),
    systemUpdates: renderSystemSetupPanel(
      copy.systemSetupSystemUpdates,
      copy.systemSetupSystemUpdatesSummary,
      renderSystemSetupSummary([
        { label: copy.updatesChannelLabel, value: dashboard.updates.channel },
        { label: copy.updatesAutoDownloadLabel, value: dashboard.updates.auto_download ? copy.enabled : copy.disabled },
        { label: copy.updatesWindowLabel, value: dashboard.updates.maintenance_window },
        { label: copy.updatesLastReleaseLabel, value: dashboard.updates.last_applied_release },
        { label: copy.updatesSourceLabel, value: dashboard.updates.update_source },
      ]),
      `<button class="list-action" type="button" data-action="platform-edit" data-target="updates">${copy.edit}</button>`,
    ),
    shutdownRestart: renderSystemSetupPanel(
      copy.systemSetupShutdownRestart,
      copy.systemSetupShutdownRestartSummary,
      systemSetupEmptyState(copy.systemSetupShutdownRestart, copy.systemSetupNoShutdown),
    ),
  };
  containers.platform.innerHTML = `
    <div class="setup-tabs">
      ${renderSystemSetupTabs(primaryTabs, activePrimary)}
      <div class="setup-tab-panel" role="tabpanel">
        ${panels[activePrimary]}
      </div>
    </div>
  `;
}

function renderMailLog() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "mail",
    container: containers.mailLog,
    rows: state.hostLogs.mail,
    emptyTitle: copy.logsTabMail,
    emptyMessage: copy.logsMailUnavailable,
  });
}

function renderAudit() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "interface",
    container: containers.audit,
    rows: state.hostLogs.interface,
    emptyTitle: copy.logsTabInterface,
    emptyMessage: copy.logsInterfaceUnavailable,
  });
}

function renderMessageLog() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "messages",
    container: containers.messageLog,
    rows: state.hostLogs.messages,
    emptyTitle: copy.logsTabMessages,
    emptyMessage: copy.logsMessagesUnavailable,
  });
}

function renderEmailAlertLog() {
  const copy = getCopy();
  renderLogTable({
    tableId: "emailAlerts",
    container: containers.emailAlertLog,
    columns: emailAlertLogColumns(copy),
    rows: [],
    emptyTitle: copy.logsTabEmailAlerts,
    emptyMessage: copy.logsEmailAlertsUnavailable,
  });
}

function renderLogTableById(tableId) {
  const renderers = {
    mail: renderMailLog,
    interface: renderAudit,
    messages: renderMessageLog,
    emailAlerts: renderEmailAlertLog,
  };
  renderers[tableId]?.();
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

function getItemText(item) {
  return [
    item?.status,
    item?.queue,
    item?.reason,
    item?.latest_decision,
    item?.magika_decision,
    item?.magika_summary,
    item?.current?.status,
    item?.current?.queue,
    item?.current?.reason,
    ...(item?.policy_tags ?? item?.current?.policy_tags ?? []),
    ...(item?.dnsbl_hits ?? item?.current?.dnsbl_hits ?? []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function itemIsSpam(item) {
  return Number(item?.spam_score ?? item?.current?.spam_score ?? 0) > 0 || /\b(spam|bayes|reputation)\b/.test(getItemText(item));
}

function itemIsVirus(item) {
  return Number(item?.security_score ?? item?.current?.security_score ?? 0) > 0 || /\b(virus|malware|infect|payload|phish)\b/.test(getItemText(item));
}

function itemHasBannedAttachment(item) {
  return /\b(banned|blocked)\b.*\b(attachment|extension|mime|magika|file)\b|\b(attachment|extension|mime|magika|file)\b.*\b(banned|blocked)\b/.test(
    getItemText(item),
  );
}

function itemHasInvalidRecipient(item) {
  return /\b(invalid|unknown|rejected|no such|recipient verification|rcpt)\b.*\b(recipient|rcpt|user|mailbox)\b|\b550\b/.test(getItemText(item));
}

function itemHasRelayDenied(item) {
  return /\b(relay denied|relay access denied|not allowed to relay)\b/.test(getItemText(item));
}

function itemHasRblHit(item) {
  return (item?.dnsbl_hits ?? item?.current?.dnsbl_hits ?? []).length > 0 || /\b(rbl|dnsbl|blocklist)\b/.test(getItemText(item));
}

function itemIsRejected(item) {
  return /\b(reject|rejected|denied|blocked|quarantined|deferred)\b/.test(getItemText(item));
}

function classifyTrafficItem(item) {
  if (itemIsSpam(item)) return "spam";
  if (itemIsVirus(item)) return "viruses";
  if (itemHasBannedAttachment(item)) return "banned";
  if (itemHasInvalidRecipient(item)) return "invalidRecipients";
  if (itemHasRelayDenied(item)) return "relayDenied";
  if (itemHasRblHit(item)) return "rblHits";
  if (itemIsRejected(item)) return "otherRejects";
  return "clean";
}

function buildScanSummary(records) {
  const summary = {
    clean: 0,
    spam: 0,
    viruses: 0,
    banned: 0,
    invalidRecipients: 0,
    relayDenied: 0,
    rblHits: 0,
    other: 0,
    total: records.length,
  };
  records.forEach((item) => {
    const category = classifyTrafficItem(item);
    if (category === "otherRejects") {
      summary.other += 1;
      return;
    }
    summary[category] += 1;
  });
  return summary;
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
      spam: 0,
      clean: 0,
      invalidRecipients: 0,
      viruses: 0,
      relayDenied: 0,
      rblHits: 0,
      banned: 0,
      otherRejects: 0,
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
    const category = classifyTrafficItem(item);
    bucket.total += 1;
    bucket[category] += 1;
  });
  return days;
}

function getRuntimeSystem(dashboard) {
  return dashboard?.system ?? dashboard?.host ?? dashboard?.runtime ?? {};
}

function getHostClockDate() {
  const system = getRuntimeSystem(state.dashboard);
  const rawHostTime = system.host_time || system.host_datetime || system.current_time || state.dashboard?.generated_at;
  if (!rawHostTime) {
    return new Date();
  }
  const hostDate = new Date(rawHostTime);
  if (Number.isNaN(hostDate.getTime())) {
    return new Date();
  }
  const loadedAt = state.hostClockLoadedAt ?? Date.now();
  return new Date(hostDate.getTime() + (Date.now() - loadedAt));
}

function renderHostClock() {
  const value = formatDateTime(getHostClockDate());
  setText(elements.contextTime, value);
  setText(document.getElementById("host-clock"), value);
}

function formatResourceUsage(usedPercent, total) {
  const percent = formatPercent(usedPercent);
  const totalLabel = formatBytes(total);
  if (percent === getCopy().unset && totalLabel === getCopy().unset) {
    return getCopy().unset;
  }
  if (percent === getCopy().unset) {
    return totalLabel;
  }
  if (totalLabel === getCopy().unset) {
    return percent;
  }
  return `${percent} / ${totalLabel}`;
}

function renderDashboardDetailTable(rows) {
  return `
    <article class="dashboard-detail-table-card">
      <table class="dashboard-detail-table">
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <th scope="row">${escapeHtml(row.label)}</th>
                  <td>
                    <strong${row.valueAttributes ?? ""}>${escapeHtml(row.value)}</strong>
                    ${row.detail ? `<small>${escapeHtml(row.detail)}</small>` : ""}
                  </td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </article>
  `;
}

function renderSystemOverview(dashboard, copy) {
  const system = getRuntimeSystem(dashboard);
  const osArchitecture =
    system.os_architecture ||
    [system.os_name || system.operating_system, system.architecture || system.arch].filter(Boolean).join(" / ");
  const rows = [
    {
      label: copy.sessionTimeLabel,
      value: formatDateTime(getHostClockDate()),
      valueAttributes: ' id="host-clock"',
    },
    { label: copy.systemHostname, value: system.hostname || dashboard.site?.node_name || copy.unset },
    { label: copy.systemUptime, value: formatUptime(system.uptime_seconds) },
    { label: copy.systemCpuUtilization, value: formatPercent(system.cpu_utilization_percent ?? system.cpu_percent) },
    { label: copy.systemProcessorType, value: system.processor_type || system.processor_model || copy.unset },
    {
      label: copy.systemProcessorSpeed,
      value: system.processor_speed || (system.processor_speed_mhz ? `${formatNumber(system.processor_speed_mhz)} MHz` : copy.unset),
    },
    { label: copy.systemOsArchitecture, value: osArchitecture || copy.unset },
    {
      label: copy.systemMemory,
      value: formatResourceUsage(
        system.memory_used_percent ?? system.memory?.used_percent,
        system.memory_total_bytes ?? system.memory?.total_bytes,
      ),
    },
    {
      label: copy.systemDisk,
      value: formatResourceUsage(
        system.disk_used_percent ?? system.disk?.used_percent,
        system.disk_total_bytes ?? system.disk?.total_bytes,
      ),
    },
  ];

  elements.systemOverviewList.innerHTML = renderDashboardDetailTable(rows);
}

function renderSevenDayTable(series, copy) {
  const headers = [
    copy.historyTableDate,
    copy.historyTableTotal,
    copy.historyTableSpam,
    copy.historyTableClean,
    copy.historyTableInvalidRcpts,
    copy.historyTableViruses,
    copy.historyTableRelayDenied,
    copy.historyTableRblHits,
    copy.historyTableBanned,
    copy.historyTableOtherRejects,
  ];
  return `
    <table class="data-table">
      <thead>
        <tr>${headers.map((header) => `<th scope="col">${escapeHtml(header)}</th>`).join("")}</tr>
      </thead>
      <tbody>
        ${series
          .slice()
          .reverse()
          .map(
            (entry) => `
              <tr>
                <th scope="row">${escapeHtml(entry.label)}</th>
                <td>${escapeHtml(formatNumber(entry.total))}</td>
                <td>${escapeHtml(formatNumber(entry.spam))}</td>
                <td>${escapeHtml(formatNumber(entry.clean))}</td>
                <td>${escapeHtml(formatNumber(entry.invalidRecipients))}</td>
                <td>${escapeHtml(formatNumber(entry.viruses))}</td>
                <td>${escapeHtml(formatNumber(entry.relayDenied))}</td>
                <td>${escapeHtml(formatNumber(entry.rblHits))}</td>
                <td>${escapeHtml(formatNumber(entry.banned))}</td>
                <td>${escapeHtml(formatNumber(entry.otherRejects))}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function renderOverview() {
  const copy = getCopy();
  const dashboard = state.dashboard;
  if (!dashboard) {
    elements.systemOverviewList.innerHTML = buildLoadingRows(2);
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
  const scanSummary = buildScanSummary(trafficRecords);
  const trafficMax = Math.max(...trafficSeries.map((entry) => entry.total), 1);

  setText(elements.operatorEmail, operatorEmail);
  setText(elements.operatorRole, copy.operatorRole);
  setText(elements.contextOperator, operatorEmail);
  setText(elements.contextRole, copy.operatorRole);
  setText(elements.contextVersion, dashboard.updates?.last_applied_release || dashboard.updates?.channel || copy.unset);
  setText(elements.contextLicense, "Apache-2.0");
  setText(elements.contextBuild, dashboard.updates?.update_source || copy.unset);
  renderHostClock();
  setText(elements.heroPrimaryRelay, relay.primary_upstream || copy.unset);
  setText(elements.heroRouteSummary, `${formatNumber(routeRules.length)} ${copy.metricRoutingRules.toLowerCase()}`);
  setText(elements.heroReportingSummary, reporting
    ? `${formatBooleanLabel(reporting.digest_enabled)} · ${formatNumber(reporting.digest_interval_minutes)} min`
    : copy.unset);
  setText(elements.heroReportingCopy, reporting?.next_digest_run_at
    ? `${formatNumber(state.digestReports.length)} · ${formatDateTime(reporting.next_digest_run_at)}`
    : `${formatNumber(state.digestReports.length)} · ${copy.unset}`);

  setText(elements.metricSystemHealth, posture.label);
  renderMetric(elements.metricInbound, queues.inbound_messages);
  renderMetric(elements.metricDeferred, queues.deferred_messages);
  renderMetric(elements.metricQuarantine, queues.quarantined_messages);
  renderMetric(elements.metricAttempts, queues.delivery_attempts_last_hour);
  renderMetric(elements.metricHeld, queues.held_messages);
  renderMetric(elements.metricRoutingRules, routeRules.length);
  renderMetric(elements.metricDkimDomains, dkim?.domains?.length ?? dashboard.policies?.dkim?.domains?.length ?? 0);
  setText(elements.metricRecipientVerification, verification ? verification.operational_state || copy.unset : "-");

  renderSystemOverview(dashboard, copy);

  elements.queueStatusList.innerHTML = renderDashboardDetailTable([
    { label: copy.queueIncomingQueue, value: formatMetric(queues.incoming_messages ?? queues.inbound_messages) },
    { label: copy.queueActiveQueue, value: formatMetric(queues.active_messages) },
    { label: copy.queueDeferredQueue, value: formatMetric(queues.deferred_messages) },
    { label: copy.queueHoldQueue, value: formatMetric(queues.held_messages) },
    { label: copy.queueCorruptQueue, value: formatMetric(queues.corrupt_messages) },
  ]);

  elements.scannerStatusList.innerHTML = [
    buildStatusTile(copy.scannerRelayLink, queues.upstream_reachable ? copy.active : copy.inactive, queues.upstream_reachable ? "active" : "disabled", relay.primary_upstream || copy.unset),
    buildStatusTile(copy.scannerVerification, verification?.operational_state || copy.unset, "custom", verification ? `${formatNumber(verification.cache_ttl_seconds)}s` : copy.unset),
    buildStatusTile(copy.scannerDkimReadiness, dkim?.operational_state || copy.unset, "custom", `${formatNumber(dkim?.domains?.length ?? 0)} ${copy.metricDkimDomains.toLowerCase()}`),
    buildStatusTile(copy.scannerDigestSchedule, reporting?.digest_enabled ? copy.enabled : copy.disabled, reporting?.digest_enabled ? "enabled" : "disabled", reporting ? `${formatNumber(reporting.digest_interval_minutes)} min` : copy.unset),
  ].join("");

  elements.relayHealthList.innerHTML = renderDashboardDetailTable([
    { label: copy.relayHealthNodeRole, value: site.region || copy.unset, detail: site.role || copy.unset },
    { label: copy.relayHealthMx, value: site.dmz_zone || copy.unset, detail: site.published_mx || copy.unset },
    {
      label: copy.relayHealthPrimary,
      value: queues.upstream_reachable ? copy.relayReachable : copy.relayUnreachable,
      detail: relay.primary_upstream || copy.unset,
    },
    {
      label: copy.relayHealthSecondary,
      value: relay.ha_enabled ? copy.enabled : copy.disabled,
      detail: relay.secondary_upstream || copy.unset,
    },
    { label: copy.relayHealthManagement, value: site.management_bind || copy.unset, detail: site.management_fqdn || copy.unset },
    {
      label: copy.relayHealthSync,
      value: relay.core_delivery_base_url || copy.unset,
      detail: formatDurationMinutes(relay.sync_interval_seconds),
    },
  ]);

  elements.topSpamRelaysList.innerHTML = buildRankedRows(
    topSpamRelays.map((entry) => ({ ...entry, detail: copy.topSpamRelaysHeading })),
  );
  elements.topVirusRelaysList.innerHTML = buildRankedRows(
    topVirusRelays.map((entry) => ({ ...entry, detail: copy.topVirusRelaysHeading })),
  );
  elements.topVirusesList.innerHTML = buildRankedRows(
    topViruses.map((entry) => ({ ...entry, detail: copy.topVirusesHeading })),
  );

  elements.scanSummaryList.innerHTML = renderDashboardDetailTable([
    { label: copy.scanSummaryClean, value: formatMetric(scanSummary.clean) },
    { label: copy.scanSummarySpamMessages, value: formatMetric(scanSummary.spam) },
    { label: copy.scanSummaryVirusMessages, value: formatMetric(scanSummary.viruses) },
    { label: copy.scanSummaryBannedAttachments, value: formatMetric(scanSummary.banned) },
    { label: copy.scanSummaryInvalidRecipients, value: formatMetric(scanSummary.invalidRecipients) },
    { label: copy.scanSummaryRelayDenied, value: formatMetric(scanSummary.relayDenied) },
    { label: copy.scanSummaryRblReject, value: formatMetric(scanSummary.rblHits) },
    { label: copy.scanSummaryOther, value: formatMetric(scanSummary.other) },
    { label: copy.scanSummaryTotalProcessed, value: formatMetric(scanSummary.total) },
  ]);

  elements.trafficChart.innerHTML = trafficSeries
    .map((entry) => {
      const totalHeight = Math.max(8, Math.round((entry.total / trafficMax) * 164));
      const spamHeight = entry.total ? Math.max(8, Math.round((entry.spam / trafficMax) * 164)) : 8;
      const securityHeight = entry.total ? Math.max(8, Math.round((entry.viruses / trafficMax) * 164)) : 8;
      return `
        <article class="traffic-bar">
          <div class="traffic-bar-total">${escapeHtml(formatNumber(entry.total))}</div>
          <div class="traffic-bar-stack">
            <span class="traffic-bar-segment total" style="height:${totalHeight}px"></span>
            <span class="traffic-bar-segment quarantine" style="height:${entry.spam ? spamHeight : 8}px;opacity:${entry.spam ? "1" : "0.22"}"></span>
            <span class="traffic-bar-segment security" style="height:${entry.viruses ? securityHeight : 8}px;opacity:${entry.viruses ? "1" : "0.22"}"></span>
          </div>
          <div class="traffic-bar-label">${escapeHtml(entry.label)}</div>
        </article>
      `;
    })
    .join("");
  elements.trafficTable.innerHTML = trafficSeries.some((entry) => entry.total > 0)
    ? renderSevenDayTable(trafficSeries, copy)
    : `<article class="traffic-row"><strong>${escapeHtml(copy.noTrafficHistory)}</strong></article>`;
}

const PAGE_RENDERERS = {
  overview: renderOverview,
  quarantine: renderQuarantine,
  history: renderHistory,
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

function openFilteringPolicyDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const policies = currentPolicies();
  const numberValue = (value) => (value === null || value === undefined ? "" : escapeHtml(value));
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.filteringPolicySummary,
    formId: "filtering-policy-form",
    opener,
    content: `
      <div class="field-grid">
        <label class="toggle-field">
          <span>${copy.requireSpfLabel}</span>
          <input name="require_spf" type="checkbox"${policies.require_spf ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.requireDmarcLabel}</span>
          <input name="require_dmarc_enforcement" type="checkbox"${policies.require_dmarc_enforcement ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.requireDkimAlignmentLabel}</span>
          <input name="require_dkim_alignment" type="checkbox"${policies.require_dkim_alignment ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.deferOnAuthTempfailLabel}</span>
          <input name="defer_on_auth_tempfail" type="checkbox"${policies.defer_on_auth_tempfail ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.bayespamEnabledLabel}</span>
          <input name="bayespam_enabled" type="checkbox"${policies.bayespam_enabled ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.reputationEnabledLabel}</span>
          <input name="reputation_enabled" type="checkbox"${policies.reputation_enabled ? " checked" : ""} />
        </label>
        <label>
          <span>${copy.spamQuarantineThresholdLabel}</span>
          <input name="spam_quarantine_threshold" type="number" min="0" step="0.1" value="${numberValue(policies.spam_quarantine_threshold)}" />
        </label>
        <label>
          <span>${copy.spamRejectThresholdLabel}</span>
          <input name="spam_reject_threshold" type="number" min="0" step="0.1" value="${numberValue(policies.spam_reject_threshold)}" />
        </label>
        <label>
          <span>${copy.reputationQuarantineThresholdLabel}</span>
          <input name="reputation_quarantine_threshold" type="number" step="1" value="${numberValue(policies.reputation_quarantine_threshold)}" />
        </label>
        <label>
          <span>${copy.reputationRejectThresholdLabel}</span>
          <input name="reputation_reject_threshold" type="number" step="1" value="${numberValue(policies.reputation_reject_threshold)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const spamQuarantineThreshold = Number(form.elements.namedItem("spam_quarantine_threshold").value);
      const spamRejectThreshold = Number(form.elements.namedItem("spam_reject_threshold").value);
      const reputationQuarantineThreshold = Number(form.elements.namedItem("reputation_quarantine_threshold").value);
      const reputationRejectThreshold = Number(form.elements.namedItem("reputation_reject_threshold").value);
      const errors = [];
      if (!Number.isFinite(spamQuarantineThreshold) || spamQuarantineThreshold < 0) {
        errors.push({ field: "spam_quarantine_threshold", message: copy.validationNonNegativeNumber });
      }
      if (!Number.isFinite(spamRejectThreshold) || spamRejectThreshold < 0) {
        errors.push({ field: "spam_reject_threshold", message: copy.validationNonNegativeNumber });
      }
      if (Number.isFinite(spamQuarantineThreshold) && Number.isFinite(spamRejectThreshold) && spamQuarantineThreshold > spamRejectThreshold) {
        errors.push({ field: "spam_reject_threshold", message: copy.validationSpamThresholdOrder });
      }
      if (!Number.isInteger(reputationQuarantineThreshold)) {
        errors.push({ field: "reputation_quarantine_threshold", message: copy.validationInteger });
      }
      if (!Number.isInteger(reputationRejectThreshold)) {
        errors.push({ field: "reputation_reject_threshold", message: copy.validationInteger });
      }
      if (
        Number.isInteger(reputationQuarantineThreshold) &&
        Number.isInteger(reputationRejectThreshold) &&
        reputationRejectThreshold > reputationQuarantineThreshold
      ) {
        errors.push({ field: "reputation_reject_threshold", message: copy.validationReputationThresholdOrder });
      }
      if (errors.length) {
        context.fail(errors);
      }
      policies.require_spf = form.elements.namedItem("require_spf").checked;
      policies.require_dmarc_enforcement = form.elements.namedItem("require_dmarc_enforcement").checked;
      policies.require_dkim_alignment = form.elements.namedItem("require_dkim_alignment").checked;
      policies.defer_on_auth_tempfail = form.elements.namedItem("defer_on_auth_tempfail").checked;
      policies.bayespam_enabled = form.elements.namedItem("bayespam_enabled").checked;
      policies.reputation_enabled = form.elements.namedItem("reputation_enabled").checked;
      policies.spam_quarantine_threshold = spamQuarantineThreshold;
      policies.spam_reject_threshold = spamRejectThreshold;
      policies.reputation_quarantine_threshold = reputationQuarantineThreshold;
      policies.reputation_reject_threshold = reputationRejectThreshold;
      await savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
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
        public_tls: publicTlsSettings(),
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

function quarantineDialogTabButton(tabId, label, activeTab) {
  return `
    <button class="tab-button${activeTab === tabId ? " tab-button-active" : ""}" type="button" data-action="quarantine-dialog-tab" data-tab-id="${escapeHtml(tabId)}" aria-selected="${activeTab === tabId ? "true" : "false"}">
      ${escapeHtml(label)}
    </button>
  `;
}

function quarantineDetailRows(rows) {
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

function renderQuarantineDetails(trace, current, retainedHistory) {
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

function renderMessageView(current) {
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

function renderQuarantineTraceDialog(trace, opener = document.activeElement) {
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

function renderTraceDrawer(trace, opener = document.activeElement) {
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
        ${current.queue === "quarantine" ? `<button class="list-action" type="button" data-action="trace-delete" data-trace-id="${escapeHtml(trace.trace_id)}">${copy.traceDelete}</button>` : ""}
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
async function openHostLog(category, logId, opener = document.activeElement) {
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

async function downloadHostLog(category, logId) {
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

async function deleteHostLog(category, logId) {
  await fetchJson(`/api/host-logs/${encodeURIComponent(category)}/${encodeURIComponent(logId)}`, { method: "DELETE" });
  const response = await fetchJson(`/api/host-logs/${encodeURIComponent(category)}`);
  state.hostLogs[category] = response?.items ?? [];
  renderLogTableById(category);
  showFeedback(getCopy().logDeleted);
}

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

async function loadQuarantineTrace(traceId, opener = document.activeElement) {
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

function setQuarantineDialogTab(tabId) {
  if (!["details", "message"].includes(tabId)) {
    return;
  }
  state.quarantineDialogTab = tabId;
  renderQuarantineTraceDialog(state.selectedTrace, elements.drawerClose);
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

async function triggerSelectedTraceAction(action) {
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
  await loadOps({ silent: true });
}

async function updateSelectedSenderPolicy(action) {
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
  await savePolicies(policies);
  showFeedback(translate(copy.quarantineBulkPolicyCompleted, { action: labelForAction(action), count: senders.length }));
}

async function runQuarantineBulkAction(action) {
  if (action === "release" || action === "delete") {
    await triggerSelectedTraceAction(action);
    return;
  }
  if (action === "allow" || action === "block") {
    await updateSelectedSenderPolicy(action);
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

function renderDiagnosticDrawer(report, opener = document.activeElement) {
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
        <pre class="diagnostic-output">${escapeHtml(report?.output || copy.unset)}</pre>
      </section>
    `,
    opener,
    null,
    "wide",
  );
}

async function openDiagnostic(kind, opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.systemDiagnosticsTitle, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const report = await fetchJson(`/api/system-diagnostics/${encodeURIComponent(kind)}`);
  renderDiagnosticDrawer(report, opener);
}

async function runHealthCheck(opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.systemHealthCheck, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const report = await postJson("/api/system-diagnostics/health-check");
  renderDiagnosticDrawer(report, opener);
}

async function connectSupport(opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.systemSupportConnect, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const report = await postJson("/api/system-diagnostics/support-connect");
  renderDiagnosticDrawer(report, opener);
}

async function flushMailQueue(opener = document.activeElement) {
  const copy = getCopy();
  renderDrawerContent(copy.systemFlushMailQueue, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const report = await postJson("/api/system-diagnostics/flush-mail-queue");
  renderDiagnosticDrawer(report, opener);
  await loadOps({ silent: true });
}

async function runDiagnosticTool(tool, opener = document.activeElement) {
  const input = document.getElementById(`diagnostic-tool-${tool}`);
  const target = input?.value?.trim() ?? "";
  if (!target) {
    showFeedback(getCopy().targetRequired, "error");
    input?.focus();
    return;
  }
  const copy = getCopy();
  renderDrawerContent(copy.systemToolsTitle, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const report = await postJson("/api/system-diagnostics/tools", { tool, target });
  renderDiagnosticDrawer(report, opener);
}

async function runSpamTest(opener = document.activeElement) {
  const input = document.getElementById("spam-test-file");
  const file = input?.files?.[0];
  if (!file) {
    showFeedback(getCopy().fileRequired, "error");
    input?.focus();
    return;
  }
  const copy = getCopy();
  renderDrawerContent(copy.systemSpamTest, copy.loadingRecords, buildLoadingRows(1), opener, null, "wide");
  const contentBase64 = await fileToBase64(file);
  const report = await postJson("/api/system-diagnostics/spam-test", {
    filename: file.name,
    content_base64: contentBase64,
  });
  renderDiagnosticDrawer(report, opener);
}

async function fileToBase64(file) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return window.btoa(binary);
}

async function runServiceAction(serviceId, serviceAction) {
  await postJson(`/api/system-diagnostics/services/${encodeURIComponent(serviceId)}/${encodeURIComponent(serviceAction)}`);
  state.systemServices = (await fetchJson("/api/system-diagnostics/services"))?.items ?? [];
  renderSystemInformation();
  showFeedback(getCopy().recordSaved);
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
      fetchJson("/api/system-diagnostics/services"),
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
  const { traceId, ruleId, index, reportId, target, domainId, profileId, sortKey, logTable, logCategory, logId, bulkAction, tabId, diagnosticKind, diagnosticTool, serviceId, serviceAction } = actionTarget.dataset;
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


