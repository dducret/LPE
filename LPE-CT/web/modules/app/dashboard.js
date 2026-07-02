import { getCopy } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { elements, state } from "./context.js?v=20260502-outbound-ehlo";
import { buildLoadingRows, renderMetric, setText } from "./ui.js?v=20260502-outbound-ehlo";
import { getRuntimeSystem, getHostClockDate, renderHostClock } from "./system.js?v=20260502-outbound-ehlo";
import { escapeHtml, formatNumber, formatMetric, formatPercent, formatBytes, formatCompactBytes, formatDateTime, formatDurationMinutes, formatUptime, formatBooleanLabel, formatAntivirusProviders, antivirusProviderChain, firstRecipient, formatShortDate, getOperatorEmail, getDigestSettings, getTrafficRecords, getRelayOrPeer, getPolicySignals, healthPosture, statusChipClass } from "./format.js?v=20260502-outbound-ehlo";

export function buildMiniStat(label, value, detail = "") {
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

export function buildStatusTile(title, stateLabel, tone = "muted", detail = "") {
  return `
    <article class="status-tile">
      <p>${escapeHtml(title)}</p>
      <span class="${statusChipClass(tone === "custom" ? stateLabel : tone)}">${escapeHtml(stateLabel)}</span>
      ${detail ? `<small>${escapeHtml(detail)}</small>` : ""}
    </article>
  `;
}

export function buildRankedRows(items) {
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

export function countRankedItems(items, resolveLabel, predicate = () => true, limit = 5) {
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

export function itemIsSecurityFlagged(item) {
  const reason = String(item?.reason ?? item?.current?.reason ?? "").toLowerCase();
  return Number(item?.security_score ?? item?.current?.security_score ?? 0) > 0 || /(virus|malware|payload|phish|suspicious|infect)/.test(reason);
}

export function extractThreatLabel(item) {
  const reason = String(item?.reason ?? item?.current?.reason ?? "").trim();
  if (reason) {
    return reason;
  }
  const tag = (item?.policy_tags ?? item?.current?.policy_tags ?? []).find(Boolean);
  return tag || "";
}

export function getItemText(item) {
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

export function itemIsSpam(item) {
  return Number(item?.spam_score ?? item?.current?.spam_score ?? 0) > 0 || /\b(spam|bayes|reputation)\b/.test(getItemText(item));
}

export function itemIsVirus(item) {
  return Number(item?.security_score ?? item?.current?.security_score ?? 0) > 0 || /\b(virus|malware|infect|payload|phish)\b/.test(getItemText(item));
}

export function itemHasBannedAttachment(item) {
  return /\b(banned|blocked)\b.*\b(attachment|extension|mime|magika|file)\b|\b(attachment|extension|mime|magika|file)\b.*\b(banned|blocked)\b/.test(
    getItemText(item),
  );
}

export function itemHasInvalidRecipient(item) {
  return /\b(invalid|unknown|rejected|no such|recipient verification|rcpt)\b.*\b(recipient|rcpt|user|mailbox)\b|\b550\b/.test(getItemText(item));
}

export function itemHasRelayDenied(item) {
  return /\b(relay denied|relay access denied|not allowed to relay)\b/.test(getItemText(item));
}

export function itemHasRblHit(item) {
  return (item?.dnsbl_hits ?? item?.current?.dnsbl_hits ?? []).length > 0 || /\b(rbl|dnsbl|blocklist)\b/.test(getItemText(item));
}

export function itemIsRejected(item) {
  return /\b(reject|rejected|denied|blocked|quarantined|deferred)\b/.test(getItemText(item));
}

export function classifyTrafficItem(item) {
  if (itemIsSpam(item)) return "spam";
  if (itemIsVirus(item)) return "viruses";
  if (itemHasBannedAttachment(item)) return "banned";
  if (itemHasInvalidRecipient(item)) return "invalidRecipients";
  if (itemHasRelayDenied(item)) return "relayDenied";
  if (itemHasRblHit(item)) return "rblHits";
  if (itemIsRejected(item)) return "otherRejects";
  return "clean";
}

export function buildScanSummary(records) {
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

export function buildTrafficSeries(records) {
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

export function formatResourceUsage(usedPercent, total) {
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

export function renderDashboardDetailTable(rows) {
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

export function renderSystemOverview(dashboard, copy) {
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

export function renderSevenDayTable(series, copy) {
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

export function renderOverview() {
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
      value: relay.secondary_upstream ? copy.enabled : copy.unset,
      detail: relay.secondary_upstream || copy.unset,
    },
    {
      label: copy.relayOutboundEhloLabel,
      value: relay.outbound_ehlo_name || copy.unset,
      detail: copy.systemSetupRelayOutbound,
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

