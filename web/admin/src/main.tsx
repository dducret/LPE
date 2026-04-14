import React from "react";
import ReactDOM from "react-dom/client";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import "./styles.css";

type DashboardState = {
  health: { service: string; status: string };
  overview: {
    total_accounts: number;
    total_mailboxes: number;
    total_domains: number;
    total_aliases: number;
    pending_queue_items: number;
    local_ai_enabled: boolean;
  };
  protocols: { name: string; enabled: boolean; bind_address: string; state: string }[];
  accounts: {
    id: string;
    email: string;
    display_name: string;
    quota_mb: number;
    used_mb: number;
    status: string;
    mailboxes: { id: string; display_name: string; role: string; message_count: number; retention_days: number }[];
  }[];
  domains: {
    id: string;
    name: string;
    status: string;
    inbound_enabled: boolean;
    outbound_enabled: boolean;
    default_quota_mb: number;
  }[];
  aliases: { id: string; source: string; target: string; kind: string; status: string }[];
  server_settings: {
    primary_hostname: string;
    admin_bind_address: string;
    smtp_bind_address: string;
    imap_bind_address: string;
    jmap_bind_address: string;
    default_locale: string;
    max_message_size_mb: number;
    tls_mode: string;
  };
  security_settings: {
    password_login_enabled: boolean;
    mfa_required_for_admins: boolean;
    session_timeout_minutes: number;
    audit_retention_days: number;
  };
  local_ai_settings: {
    enabled: boolean;
    provider: string;
    model: string;
    offline_only: boolean;
    indexing_enabled: boolean;
  };
  storage: {
    primary_store: string;
    search_engine: string;
    attachment_formats: string[];
    replication_mode: string;
  };
  audit_log: { id: string; timestamp: string; actor: string; action: string; subject: string }[];
};

type AccountForm = { email: string; display_name: string; quota_mb: string };
type MailboxForm = { account_id: string; display_name: string; role: string; retention_days: string };
type DomainForm = { name: string; default_quota_mb: string; inbound_enabled: boolean; outbound_enabled: boolean };
type AliasForm = { source: string; target: string; kind: string };

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api/${path}`);
  if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`);
  return (await response.json()) as T;
}

async function sendJson<T>(path: string, method: "POST" | "PUT", payload: unknown): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`);
  return (await response.json()) as T;
}

function Field(props: { label: string; value: string; onChange: (value: string) => void; type?: "text" | "number"; placeholder?: string }) {
  return (
    <label className="field">
      <span>{props.label}</span>
      <input type={props.type ?? "text"} value={props.value} placeholder={props.placeholder} onChange={(event) => props.onChange(event.target.value)} />
    </label>
  );
}

function ToggleField(props: { label: string; checked: boolean; onChange: (checked: boolean) => void }) {
  return (
    <label className="toggle-field">
      <span>{props.label}</span>
      <input type="checkbox" checked={props.checked} onChange={(event) => props.onChange(event.target.checked)} />
    </label>
  );
}

function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const [state, setState] = React.useState<DashboardState | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [notice, setNotice] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);
  const [accountForm, setAccountForm] = React.useState<AccountForm>({ email: "", display_name: "", quota_mb: "4096" });
  const [mailboxForm, setMailboxForm] = React.useState<MailboxForm>({ account_id: "", display_name: "", role: "custom", retention_days: "365" });
  const [domainForm, setDomainForm] = React.useState<DomainForm>({ name: "", default_quota_mb: "4096", inbound_enabled: true, outbound_enabled: true });
  const [aliasForm, setAliasForm] = React.useState<AliasForm>({ source: "", target: "", kind: "forward" });
  const [serverForm, setServerForm] = React.useState<DashboardState["server_settings"] | null>(null);
  const [securityForm, setSecurityForm] = React.useState<DashboardState["security_settings"] | null>(null);
  const [localAiForm, setLocalAiForm] = React.useState<DashboardState["local_ai_settings"] | null>(null);
  const copy = messages[locale];

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

  const syncState = React.useCallback((dashboard: DashboardState) => {
    React.startTransition(() => {
      setState(dashboard);
      setServerForm(dashboard.server_settings);
      setSecurityForm(dashboard.security_settings);
      setLocalAiForm(dashboard.local_ai_settings);
    });
    setMailboxForm((current) => ({ ...current, account_id: current.account_id || dashboard.accounts[0]?.id || "" }));
  }, []);

  const load = React.useCallback(async () => {
    setBusy("load");
    try {
      const dashboard = await fetchJson<DashboardState>("console/dashboard");
      syncState(dashboard);
      setError(null);
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Unknown error");
    } finally {
      setBusy(null);
    }
  }, [syncState]);

  React.useEffect(() => {
    void load();
  }, [load]);

  async function mutate(action: string, path: string, method: "POST" | "PUT", payload: unknown, success: string, afterSuccess?: () => void) {
    setBusy(action);
    try {
      const dashboard = await sendJson<DashboardState>(path, method, payload);
      syncState(dashboard);
      setNotice(success);
      setError(null);
      afterSuccess?.();
    } catch (mutationError) {
      setError(mutationError instanceof Error ? mutationError.message : "Unknown error");
    } finally {
      setBusy(null);
    }
  }

  const storageUsed = state?.accounts.reduce((sum, account) => sum + account.used_mb, 0) ?? 0;

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <p className="eyebrow">{copy.eyebrow}</p>
          <h1>{copy.title}</h1>
          <p className="hero-body">{copy.subtitle}</p>
        </div>
        <div className="hero-actions">
          <label className="locale-picker">
            <span>{copy.languageLabel}</span>
            <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
              {supportedLocales.map((entry) => <option key={entry} value={entry}>{localeLabels[entry]}</option>)}
            </select>
          </label>
          <button className="secondary-button" type="button" onClick={() => void load()}>{copy.refresh}</button>
        </div>
      </section>

      <section className="banner">
        <div>
          <strong>{state?.health.status === "ok" ? copy.serviceHealthy : copy.serviceUnhealthy}</strong>
          <p>{copy.bannerBody}</p>
        </div>
        <a href="/api/console/dashboard" target="_blank" rel="noreferrer">{copy.openApiLabel}</a>
      </section>

      {error ? <p className="feedback error">{copy.failed} {error}</p> : null}
      {notice ? <p className="feedback notice">{notice}</p> : null}
      {!state && !error ? <p className="feedback muted">{copy.loading}</p> : null}

      {state ? <>
        <section className="stats-grid">
          <article className="stat-card"><p className="stat-label">{copy.accountsTitle}</p><strong className="stat-value">{state.overview.total_accounts}</strong><p className="stat-detail">{copy.accountsDetail.replace("{count}", String(state.overview.total_mailboxes))}</p></article>
          <article className="stat-card"><p className="stat-label">{copy.domainsTitle}</p><strong className="stat-value">{state.overview.total_domains}</strong><p className="stat-detail">{copy.aliasesDetail.replace("{count}", String(state.overview.total_aliases))}</p></article>
          <article className="stat-card"><p className="stat-label">{copy.storageTitle}</p><strong className="stat-value">{storageUsed} MB</strong><p className="stat-detail">{copy.storageDetail.replace("{count}", String(state.storage.attachment_formats.length))}</p></article>
          <article className="stat-card"><p className="stat-label">{copy.queueTitle}</p><strong className="stat-value">{state.overview.pending_queue_items}</strong><p className="stat-detail">{state.overview.local_ai_enabled ? copy.aiEnabled : copy.aiDisabled}</p></article>
        </section>

        <section className="workspace">
          <div className="column">
            <article className="panel">
              <div className="section-heading"><div><p className="section-kicker">{copy.accountsTitle}</p><h2>{copy.identitySectionTitle}</h2></div></div>
              <div className="form-grid">
                <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("account", "console/accounts", "POST", { email: accountForm.email, display_name: accountForm.display_name, quota_mb: Number(accountForm.quota_mb) }, copy.accountCreated, () => setAccountForm({ email: "", display_name: "", quota_mb: "4096" })); }}>
                  <h3>{copy.createAccount}</h3>
                  <Field label={copy.emailLabel} value={accountForm.email} onChange={(value) => setAccountForm((current) => ({ ...current, email: value }))} placeholder="user@example.test" />
                  <Field label={copy.nameLabel} value={accountForm.display_name} onChange={(value) => setAccountForm((current) => ({ ...current, display_name: value }))} placeholder="Jane Doe" />
                  <Field label={copy.quotaLabel} type="number" value={accountForm.quota_mb} onChange={(value) => setAccountForm((current) => ({ ...current, quota_mb: value }))} />
                  <button className="primary-button" disabled={busy === "account"} type="submit">{copy.create}</button>
                </form>
                <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("mailbox", "console/mailboxes", "POST", { account_id: mailboxForm.account_id, display_name: mailboxForm.display_name, role: mailboxForm.role, retention_days: Number(mailboxForm.retention_days) }, copy.mailboxCreated, () => setMailboxForm((current) => ({ ...current, display_name: "", role: "custom", retention_days: "365" }))); }}>
                  <h3>{copy.createMailbox}</h3>
                  <label className="field"><span>{copy.accountLabel}</span><select value={mailboxForm.account_id} onChange={(event) => setMailboxForm((current) => ({ ...current, account_id: event.target.value }))}>{state.accounts.map((account) => <option key={account.id} value={account.id}>{account.display_name} ({account.email})</option>)}</select></label>
                  <Field label={copy.mailboxLabel} value={mailboxForm.display_name} onChange={(value) => setMailboxForm((current) => ({ ...current, display_name: value }))} placeholder="Projects" />
                  <Field label={copy.roleLabel} value={mailboxForm.role} onChange={(value) => setMailboxForm((current) => ({ ...current, role: value }))} />
                  <Field label={copy.retentionLabel} type="number" value={mailboxForm.retention_days} onChange={(value) => setMailboxForm((current) => ({ ...current, retention_days: value }))} />
                  <button className="primary-button" disabled={busy === "mailbox"} type="submit">{copy.create}</button>
                </form>
              </div>
              <div className="stack">{state.accounts.map((account) => <article className="account-card" key={account.id}><div className="account-header"><div><h3>{account.display_name}</h3><p>{account.email}</p></div><div className="chip-row"><span className="chip">{account.status}</span><span className="chip">{account.used_mb} / {account.quota_mb} MB</span></div></div><div className="mailbox-grid">{account.mailboxes.map((mailbox) => <div className="mailbox-card" key={mailbox.id}><strong>{mailbox.display_name}</strong><span>{mailbox.role}</span><span>{mailbox.message_count} {copy.messagesLabel.toLowerCase()}</span><span>{mailbox.retention_days} {copy.daysLabel.toLowerCase()}</span></div>)}</div></article>)}</div>
            </article>

            <article className="panel">
              <div className="section-heading"><div><p className="section-kicker">{copy.domainsTitle}</p><h2>{copy.routingSectionTitle}</h2></div></div>
              <div className="form-grid">
                <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("domain", "console/domains", "POST", { name: domainForm.name, default_quota_mb: Number(domainForm.default_quota_mb), inbound_enabled: domainForm.inbound_enabled, outbound_enabled: domainForm.outbound_enabled }, copy.domainCreated, () => setDomainForm({ name: "", default_quota_mb: "4096", inbound_enabled: true, outbound_enabled: true })); }}>
                  <h3>{copy.createDomain}</h3>
                  <Field label={copy.domainLabel} value={domainForm.name} onChange={(value) => setDomainForm((current) => ({ ...current, name: value }))} placeholder="team.example.test" />
                  <Field label={copy.defaultQuotaLabel} type="number" value={domainForm.default_quota_mb} onChange={(value) => setDomainForm((current) => ({ ...current, default_quota_mb: value }))} />
                  <ToggleField label={copy.inboundLabel} checked={domainForm.inbound_enabled} onChange={(checked) => setDomainForm((current) => ({ ...current, inbound_enabled: checked }))} />
                  <ToggleField label={copy.outboundLabel} checked={domainForm.outbound_enabled} onChange={(checked) => setDomainForm((current) => ({ ...current, outbound_enabled: checked }))} />
                  <button className="primary-button" disabled={busy === "domain"} type="submit">{copy.create}</button>
                </form>
                <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("alias", "console/aliases", "POST", aliasForm, copy.aliasCreated, () => setAliasForm({ source: "", target: "", kind: "forward" })); }}>
                  <h3>{copy.createAlias}</h3>
                  <Field label={copy.sourceLabel} value={aliasForm.source} onChange={(value) => setAliasForm((current) => ({ ...current, source: value }))} placeholder="sales@example.test" />
                  <Field label={copy.targetLabel} value={aliasForm.target} onChange={(value) => setAliasForm((current) => ({ ...current, target: value }))} placeholder="team@example.test" />
                  <Field label={copy.kindLabel} value={aliasForm.kind} onChange={(value) => setAliasForm((current) => ({ ...current, kind: value }))} />
                  <button className="primary-button" disabled={busy === "alias"} type="submit">{copy.create}</button>
                </form>
              </div>
              <div className="table-card"><table><thead><tr><th>{copy.domainLabel}</th><th>{copy.statusLabel}</th><th>{copy.inboundLabel}</th><th>{copy.outboundLabel}</th><th>{copy.defaultQuotaLabel}</th></tr></thead><tbody>{state.domains.map((domain) => <tr key={domain.id}><td>{domain.name}</td><td>{domain.status}</td><td>{domain.inbound_enabled ? copy.enabledLabel : copy.disabledLabel}</td><td>{domain.outbound_enabled ? copy.enabledLabel : copy.disabledLabel}</td><td>{domain.default_quota_mb} MB</td></tr>)}</tbody></table></div>
              <div className="table-card"><table><thead><tr><th>{copy.sourceLabel}</th><th>{copy.targetLabel}</th><th>{copy.kindLabel}</th><th>{copy.statusLabel}</th></tr></thead><tbody>{state.aliases.map((alias) => <tr key={alias.id}><td>{alias.source}</td><td>{alias.target}</td><td>{alias.kind}</td><td>{alias.status}</td></tr>)}</tbody></table></div>
            </article>
          </div>

          <div className="column">
            <article className="panel">
              <div className="section-heading"><div><p className="section-kicker">{copy.settingsTitle}</p><h2>{copy.platformSectionTitle}</h2></div></div>
              {serverForm ? <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("server", "console/settings/server", "PUT", serverForm, copy.serverSaved); }}><h3>{copy.serverSettingsTitle}</h3><div className="mini-grid"><Field label={copy.hostnameLabel} value={serverForm.primary_hostname} onChange={(value) => setServerForm((current) => current ? { ...current, primary_hostname: value } : current)} /><Field label={copy.localeDefaultLabel} value={serverForm.default_locale} onChange={(value) => setServerForm((current) => current ? { ...current, default_locale: value } : current)} /><Field label={copy.adminBindLabel} value={serverForm.admin_bind_address} onChange={(value) => setServerForm((current) => current ? { ...current, admin_bind_address: value } : current)} /><Field label={copy.smtpBindLabel} value={serverForm.smtp_bind_address} onChange={(value) => setServerForm((current) => current ? { ...current, smtp_bind_address: value } : current)} /><Field label={copy.imapBindLabel} value={serverForm.imap_bind_address} onChange={(value) => setServerForm((current) => current ? { ...current, imap_bind_address: value } : current)} /><Field label={copy.jmapBindLabel} value={serverForm.jmap_bind_address} onChange={(value) => setServerForm((current) => current ? { ...current, jmap_bind_address: value } : current)} /><Field label={copy.maxMessageLabel} type="number" value={String(serverForm.max_message_size_mb)} onChange={(value) => setServerForm((current) => current ? { ...current, max_message_size_mb: Number(value) || 0 } : current)} /><Field label={copy.tlsModeLabel} value={serverForm.tls_mode} onChange={(value) => setServerForm((current) => current ? { ...current, tls_mode: value } : current)} /></div><button className="primary-button" disabled={busy === "server"} type="submit">{copy.save}</button></form> : null}
              {securityForm ? <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("security", "console/settings/security", "PUT", securityForm, copy.securitySaved); }}><h3>{copy.securityTitle}</h3><div className="mini-grid"><ToggleField label={copy.passwordLoginLabel} checked={securityForm.password_login_enabled} onChange={(checked) => setSecurityForm((current) => current ? { ...current, password_login_enabled: checked } : current)} /><ToggleField label={copy.mfaLabel} checked={securityForm.mfa_required_for_admins} onChange={(checked) => setSecurityForm((current) => current ? { ...current, mfa_required_for_admins: checked } : current)} /><Field label={copy.sessionTimeoutLabel} type="number" value={String(securityForm.session_timeout_minutes)} onChange={(value) => setSecurityForm((current) => current ? { ...current, session_timeout_minutes: Number(value) || 0 } : current)} /><Field label={copy.auditRetentionLabel} type="number" value={String(securityForm.audit_retention_days)} onChange={(value) => setSecurityForm((current) => current ? { ...current, audit_retention_days: Number(value) || 0 } : current)} /></div><button className="primary-button" disabled={busy === "security"} type="submit">{copy.save}</button></form> : null}
              {localAiForm ? <form className="card" onSubmit={(event) => { event.preventDefault(); void mutate("ai", "console/settings/local-ai", "PUT", localAiForm, copy.localAiSaved); }}><h3>{copy.localAiTitle}</h3><div className="mini-grid"><ToggleField label={copy.localAiEnabledLabel} checked={localAiForm.enabled} onChange={(checked) => setLocalAiForm((current) => current ? { ...current, enabled: checked } : current)} /><ToggleField label={copy.offlineOnlyLabel} checked={localAiForm.offline_only} onChange={(checked) => setLocalAiForm((current) => current ? { ...current, offline_only: checked } : current)} /><ToggleField label={copy.indexingLabel} checked={localAiForm.indexing_enabled} onChange={(checked) => setLocalAiForm((current) => current ? { ...current, indexing_enabled: checked } : current)} /><Field label={copy.providerLabel} value={localAiForm.provider} onChange={(value) => setLocalAiForm((current) => current ? { ...current, provider: value } : current)} /><Field label={copy.modelLabel} value={localAiForm.model} onChange={(value) => setLocalAiForm((current) => current ? { ...current, model: value } : current)} /></div><button className="primary-button" disabled={busy === "ai"} type="submit">{copy.save}</button></form> : null}
            </article>

            <article className="panel">
              <div className="section-heading"><div><p className="section-kicker">{copy.operationsTitle}</p><h2>{copy.operationsSectionTitle}</h2></div></div>
              <div className="protocol-list">{state.protocols.map((protocol) => <article className="protocol-card" key={protocol.name}><div className="protocol-header"><h3>{protocol.name}</h3><span className={`status-pill ${protocol.enabled ? "is-up" : "is-down"}`}>{protocol.enabled ? copy.enabledLabel : copy.disabledLabel}</span></div><p>{protocol.bind_address}</p><strong>{protocol.state}</strong></article>)}</div>
              <div className="storage-card"><h3>{copy.storageTitle}</h3><dl className="definition-list"><div><dt>{copy.primaryStoreLabel}</dt><dd>{state.storage.primary_store}</dd></div><div><dt>{copy.searchLabel}</dt><dd>{state.storage.search_engine}</dd></div><div><dt>{copy.replicationLabel}</dt><dd>{state.storage.replication_mode}</dd></div></dl><div className="chip-row">{state.storage.attachment_formats.map((format) => <span className="chip" key={format}>{format}</span>)}</div></div>
            </article>

            <article className="panel">
              <div className="section-heading"><div><p className="section-kicker">{copy.auditTitle}</p><h2>{copy.auditSectionTitle}</h2></div></div>
              <div className="audit-list">{state.audit_log.map((event) => <article className="audit-card" key={event.id}><div><strong>{event.action}</strong><p>{event.subject}</p></div><div className="audit-meta"><span>{event.actor}</span><span>{event.timestamp}</span></div></article>)}</div>
            </article>
          </div>
        </section>
      </> : null}
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(<React.StrictMode><App /></React.StrictMode>);
