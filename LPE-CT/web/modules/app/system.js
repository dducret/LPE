import { i18n, getCopy } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { containers, elements, state } from "./context.js?v=20260502-outbound-ehlo";
import { buildLoadingRows, setText } from "./ui.js?v=20260502-outbound-ehlo";
import { escapeHtml, formatList, formatNumber, formatDateTime, formatPercent, formatReportingUptime, formatUptime, humanizeStatus, renderHostLogTable, renderLogTable, emailAlertLogColumns, statusChipClass } from "./format.js?v=20260502-outbound-ehlo";

export function formatGigabytes(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return getCopy().unset;
  }
  const gib = Math.max(0, Number(value)) / 1024 / 1024 / 1024;
  return `${new Intl.NumberFormat(i18n.getLocale(), { maximumFractionDigits: gib >= 10 ? 0 : 1 }).format(gib)} GB`;
}

export function renderResourceGauge(percentValue, totalBytes) {
  const percentNumber = Number(percentValue);
  const hasPercent = !Number.isNaN(percentNumber);
  const boundedPercent = hasPercent ? Math.max(0, Math.min(100, percentNumber)) : 0;
  const percent = hasPercent ? formatPercent(percentNumber) : getCopy().unset;
  const total = formatGigabytes(totalBytes);
  return `
    <div class="resource-meter" aria-label="${escapeHtml(`${percent} used of ${total}`)}">
      <span class="resource-meter-track" aria-hidden="true">
        <span class="resource-meter-fill" style="width: ${escapeHtml(String(boundedPercent))}%"></span>
      </span>
      <span class="resource-meter-meta">
        <strong>${escapeHtml(percent)}</strong>
        <small>${escapeHtml(total)}</small>
      </span>
    </div>
  `;
}

export function renderSystemTable(rows, { actionColumn = false } = {}) {
  return `
    <article class="dashboard-detail-table-card">
      <table class="dashboard-detail-table system-report-table${actionColumn ? " system-report-table-actions" : ""}">
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <th scope="row">${escapeHtml(row.label)}</th>
                  <td>${row.html ?? `<strong>${escapeHtml(row.value)}</strong>`}</td>
                  ${actionColumn ? `<td class="system-report-action-cell">${row.action ?? ""}</td>` : ""}
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </article>
  `;
}

export function renderSystemInformation() {
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
      html: renderResourceGauge(system.memory_used_percent ?? system.memory?.used_percent, system.memory_total_bytes ?? system.memory?.total_bytes),
    },
    {
      label: copy.systemMailLogDiskSpace,
      html: renderResourceGauge(system.disk_used_percent ?? system.disk?.used_percent, system.disk_total_bytes ?? system.disk?.total_bytes),
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
      ${renderSystemTable(systemRows)}
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemServicesTitle)}</h4>
      </div>
      ${renderSystemTable(services.map((service) => serviceTableRow(service, copy)), { actionColumn: true })}
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemDiagnosticsTitle)}</h4>
      </div>
      ${renderSystemTable([
        ...diagnostics.map((diagnostic) => diagnosticTableRow(diagnostic, copy)),
        actionTableRow(copy.systemSupportConnect, "", copy.connect, "support-connect"),
        actionTableRow(copy.systemHealthCheck, "", copy.run, "health-check-run"),
      ], { actionColumn: true })}
    </section>
    <section class="system-report-section">
      <div class="system-report-section-head">
        <h4>${escapeHtml(copy.systemToolsTitle)}</h4>
      </div>
      ${renderSystemTable([
        ...tools.map((tool) => toolTableRow(tool, copy)),
        actionTableRow(copy.systemFlushMailQueue, "", copy.flush, "flush-mail-queue"),
      ], { actionColumn: true })}
    </section>
  `;
}

export function serviceTableRow(service, copy) {
  const status = humanizeStatus(service.status);
  const action = service.action === "stop" ? "stop" : "start";
  const actionLabel = action === "stop" ? copy.stop : copy.start;
  const label = service.id === "antivirus" ? `${copy.serviceAntivirus} Takeri` : copy.serviceLpeCt;
  return {
    label,
    html: `<span class="${statusChipClass(service.status)}">${escapeHtml(status)}</span>`,
    action: `<button class="secondary-button compact-button" type="button" data-action="system-service-action" data-service-id="${escapeHtml(service.id)}" data-service-action="${escapeHtml(action)}">${escapeHtml(actionLabel)}</button>`,
  };
}

export function diagnosticTableRow(diagnostic, copy) {
  return {
    label: diagnostic.label,
    html: diagnostic.upload ? `<input id="spam-test-file" class="system-file-input" type="file" />` : `<span class="record-copy">${escapeHtml(copy.showDiagnosticStatus)}</span>`,
    action: `<button class="secondary-button compact-button" type="button" data-action="${diagnostic.upload ? "spam-test-show" : "diagnostic-show"}" data-diagnostic-kind="${escapeHtml(diagnostic.kind)}">${escapeHtml(copy.show)}</button>`,
  };
}

export function actionTableRow(label, value, buttonLabel, action) {
  return {
    label,
    html: value ? `<strong>${escapeHtml(value)}</strong>` : `<span class="record-copy">&nbsp;</span>`,
    action: `<button class="secondary-button compact-button" type="button" data-action="${escapeHtml(action)}">${escapeHtml(buttonLabel)}</button>`,
  };
}

export function toolTableRow(tool, copy) {
  return {
    label: tool.label,
    html: `<input id="diagnostic-tool-${escapeHtml(tool.tool)}" type="text" autocomplete="off" placeholder="${escapeHtml(tool.placeholder)}" />`,
    action: `<button class="secondary-button compact-button" type="button" data-action="diagnostic-tool-run" data-diagnostic-tool="${escapeHtml(tool.tool)}">${escapeHtml(copy.run)}</button>`,
  };
}

export function systemSetupEmptyState(title, summary) {
  return `
    <article class="empty-state compact-empty-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(summary)}</p>
    </article>
  `;
}

export function renderSystemSetupTabs(tabs, activeTab, level = "primary") {
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

export function renderSystemSetupPanel(title, summary, body, actions = "") {
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

export function renderSystemSetupSummary(items) {
  const copy = getCopy();
  const valueOrUnset = (value) => {
    if (typeof value === "string") {
      const trimmed = value.trim();
      return trimmed ? trimmed : copy.unset;
    }
    return value ?? copy.unset;
  };
  return `
    <div class="record-grid">
      ${items
        .map(
          (item) => `
            <div class="summary-card">
              <p>${escapeHtml(item.label)}</p>
              <strong>${escapeHtml(valueOrUnset(item.value))}</strong>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

export function systemNetworkInterfaces(dashboard) {
  const candidates = [
    dashboard.system?.network_interfaces,
    dashboard.system?.interfaces,
    dashboard.network?.interfaces,
  ];
  return candidates.find((candidate) => Array.isArray(candidate)) ?? [];
}

export function renderNetworkInterfaces(dashboard, copy) {
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

export function renderSimpleList(title, values, emptyMessage) {
  const rows = (values ?? []).filter(Boolean);
  if (!rows.length) {
    return systemSetupEmptyState(title, emptyMessage);
  }
  return `
    <section class="network-interface-panel">
      <h5>${escapeHtml(title)}</h5>
      <div class="system-value-list">
        ${rows.map((value) => `<code>${escapeHtml(value)}</code>`).join("")}
      </div>
    </section>
  `;
}

export function renderIpv6Addresses(dashboard, copy) {
  const addresses = dashboard.system?.ipv6_addresses ?? [];
  if (!addresses.length) {
    return systemSetupEmptyState(copy.systemSetupNetworkIpv6, copy.systemSetupNoIpv6);
  }
  return `
    <section class="network-interface-panel">
      <h5>${escapeHtml(copy.systemSetupNetworkIpv6)}</h5>
      <div class="data-table-wrap network-interface-table-wrap">
        <table class="data-table network-interface-table">
          <thead>
            <tr>
              <th scope="col">${escapeHtml(copy.networkInterfaceNameLabel)}</th>
              <th scope="col">${escapeHtml(copy.networkInterfaceAddressLabel)}</th>
              <th scope="col">${escapeHtml(copy.networkInterfacePrefixLabel)}</th>
            </tr>
          </thead>
          <tbody>
            ${addresses
              .map(
                (item) => `
                  <tr>
                    <th scope="row">${escapeHtml(item.interface ?? copy.unset)}</th>
                    <td>${escapeHtml(item.address ?? copy.unset)}</td>
                    <td>${escapeHtml(item.prefix ?? copy.unset)}</td>
                  </tr>
                `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

export function renderNetworkSetup(activeTab, dashboard, copy) {
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
      `${renderSystemSetupSummary([
        { label: copy.sitePublishedMxLabel, value: dashboard.site.published_mx },
        { label: copy.siteManagementFqdnLabel, value: dashboard.site.management_fqdn },
        { label: copy.siteDmzZoneLabel, value: dashboard.site.dmz_zone },
      ])}${renderSimpleList(copy.systemSetupDebianDnsServers, dashboard.system?.dns_servers, copy.systemSetupNoDnsServers)}`,
      editSite,
    ),
    "static-routes": renderSystemSetupPanel(
      copy.systemSetupNetworkStaticRoutes,
      copy.systemSetupNetworkStaticRoutesSummary,
      renderSimpleList(copy.systemSetupNetworkStaticRoutes, dashboard.system?.ipv4_routes, copy.systemSetupNoStaticRoutes),
    ),
    ipv6: renderSystemSetupPanel(
      copy.systemSetupNetworkIpv6,
      copy.systemSetupNetworkIpv6Summary,
      `${renderIpv6Addresses(dashboard, copy)}${renderSimpleList(copy.systemSetupIpv6Routes, dashboard.system?.ipv6_routes, copy.systemSetupNoIpv6Routes)}`,
    ),
  };
  return renderSystemSetupTabs(tabs, activeTab, "secondary") + bodies[activeTab];
}

export function formatVerificationType(value) {
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

export function renderBooleanCell(value) {
  const copy = getCopy();
  return `<span class="${statusChipClass(Boolean(value))}">${escapeHtml(value ? copy.yes : copy.no)}</span>`;
}

export function publicTlsSettings(dashboard = state.dashboard) {
  return dashboard?.network?.public_tls ?? { active_profile_id: null, profiles: [] };
}

export function publicTlsActiveProfile(settings = publicTlsSettings()) {
  return (settings.profiles ?? []).find((profile) => profile.id === settings.active_profile_id) ?? null;
}

export function publicTlsStatusLabel(settings = publicTlsSettings()) {
  const copy = getCopy();
  return publicTlsActiveProfile(settings) ? copy.enabled : copy.disabled;
}

export function renderPublicTlsProfiles(dashboard, copy) {
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

export function renderAcceptedDomainsTable(dashboard, copy) {
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

export function renderMailRelaySetup(activeTab, dashboard, copy) {
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
        { label: copy.relayPrimaryLabel, value: dashboard.relay.primary_upstream },
        { label: copy.relaySecondaryLabel, value: dashboard.relay.secondary_upstream },
        { label: copy.relayOutboundEhloLabel, value: dashboard.relay.outbound_ehlo_name },
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

export function renderMailAuthenticationSetup(activeTab, dashboard, copy) {
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

export function renderPlatform() {
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
        { label: copy.ntpEnabledLabel, value: dashboard.system?.ntp?.enabled ? copy.enabled : copy.disabled },
        { label: copy.ntpSynchronizedLabel, value: dashboard.system?.ntp?.synchronized === true ? copy.yes : dashboard.system?.ntp?.synchronized === false ? copy.no : copy.unset },
        { label: copy.ntpServersLabel, value: formatList(dashboard.system?.ntp?.servers) },
      ]),
      `<button class="list-action" type="button" data-action="platform-edit" data-target="time">${copy.edit}</button><button class="list-action" type="button" data-action="ntp-sync">${copy.ntpSyncAction}</button>`,
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
      `<button class="list-action" type="button" data-action="platform-edit" data-target="updates">${copy.edit}</button><button class="list-action" type="button" data-action="apt-upgrade">${copy.aptUpgradeAction}</button>`,
    ),
    shutdownRestart: renderSystemSetupPanel(
      copy.systemSetupShutdownRestart,
      copy.systemSetupShutdownRestartSummary,
      renderSystemSetupSummary([{ label: copy.systemHostname, value: dashboard.system?.hostname || dashboard.site?.node_name || copy.unset }]),
      `<button class="list-action danger-action" type="button" data-action="power-action" data-power-action="restart">${copy.restartAction}</button><button class="list-action danger-action" type="button" data-action="power-action" data-power-action="shutdown">${copy.shutdownAction}</button>`,
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

export function renderMailLog() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "mail",
    container: containers.mailLog,
    rows: state.hostLogs.mail,
    emptyTitle: copy.logsTabMail,
    emptyMessage: copy.logsMailUnavailable,
  });
}

export function renderAudit() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "interface",
    container: containers.audit,
    rows: state.hostLogs.interface,
    emptyTitle: copy.logsTabInterface,
    emptyMessage: copy.logsInterfaceUnavailable,
  });
}

export function renderMessageLog() {
  const copy = getCopy();
  renderHostLogTable({
    tableId: "messages",
    container: containers.messageLog,
    rows: state.hostLogs.messages,
    emptyTitle: copy.logsTabMessages,
    emptyMessage: copy.logsMessagesUnavailable,
  });
}

export function renderEmailAlertLog() {
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

export function renderLogTableById(tableId) {
  const renderers = {
    mail: renderMailLog,
    interface: renderAudit,
    messages: renderMessageLog,
    emailAlerts: renderEmailAlertLog,
  };
  renderers[tableId]?.();
}


export function getRuntimeSystem(dashboard) {
  return dashboard?.system ?? dashboard?.host ?? dashboard?.runtime ?? {};
}

export function getHostClockDate() {
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

export function renderHostClock() {
  const value = formatDateTime(getHostClockDate());
  setText(elements.contextTime, value);
  setText(document.getElementById("host-clock"), value);
}

