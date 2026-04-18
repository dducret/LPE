import React from "react";
import ReactDOM from "react-dom/client";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import "./styles.css";

type DashboardState = {
  health: { service: string; status: string };
  overview: { total_accounts: number; total_mailboxes: number; total_domains: number; total_aliases: number; pending_queue_items: number; local_ai_enabled: boolean };
  protocols: { name: string; enabled: boolean; bind_address: string; state: string }[];
  accounts: { id: string; email: string; display_name: string; quota_mb: number; used_mb: number; status: string; mailboxes: { id: string; display_name: string; role: string; message_count: number; retention_days: number; pst_jobs: { id: string; direction: string; server_path: string; status: string; requested_by: string; created_at: string; completed_at: string | null; processed_messages: number; error_message: string | null }[] }[] }[];
  domains: { id: string; name: string; status: string; inbound_enabled: boolean; outbound_enabled: boolean; default_quota_mb: number }[];
  aliases: { id: string; source: string; target: string; kind: string; status: string }[];
  server_admins: { id: string; domain_id: string | null; domain_name: string; email: string; display_name: string; role: string; rights_summary: string }[];
  server_settings: { primary_hostname: string; admin_bind_address: string; smtp_bind_address: string; imap_bind_address: string; jmap_bind_address: string; default_locale: string; max_message_size_mb: number; tls_mode: string };
  security_settings: { password_login_enabled: boolean; mfa_required_for_admins: boolean; session_timeout_minutes: number; audit_retention_days: number };
  local_ai_settings: { enabled: boolean; provider: string; model: string; offline_only: boolean; indexing_enabled: boolean };
  antispam_settings: { content_filtering_enabled: boolean; spam_engine: string; quarantine_enabled: boolean; quarantine_retention_days: number };
  antispam_rules: { id: string; name: string; scope: string; action: string; status: string }[];
  quarantine_items: { id: string; message_ref: string; sender: string; recipient: string; reason: string; status: string; created_at: string }[];
  storage: { primary_store: string; search_engine: string; attachment_formats: string[]; replication_mode: string };
  audit_log: { id: string; timestamp: string; actor: string; action: string; subject: string }[];
};

type TraceResult = { message_id: string; internet_message_id: string | null; subject: string; sender: string; account_email: string; mailbox: string; received_at: string };
type PstFormState = { direction: "import" | "export"; server_path: string; requested_by: string };
type AccountRecord = DashboardState["accounts"][number];
type MailboxRecord = AccountRecord["mailboxes"][number];
type AccountPanelMode = "new" | "details" | "import" | "export";
type LoginResponse = { token: string; admin: { email: string; display_name: string; role: string } };
type PageKey = "server" | "domain" | "antispam" | "audit" | "operations";
type ServerTab = "status" | "server" | "security" | "ai" | "domains" | "admins";
type DomainTab = "overview" | "accounts" | "aliases" | "admins";
type AntispamTab = "content" | "engines" | "rules" | "quarantine";
type AuditTab = "journal" | "trace";
type OperationsTab = "protocols" | "storage";

function authHeaders(token: string | null): Record<string, string> { return token ? { Authorization: `Bearer ${token}` } : {}; }
async function fetchJson<T>(path: string, token: string | null): Promise<T> { const response = await fetch(`/api/${path}`, { headers: authHeaders(token) }); if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`); return (await response.json()) as T; }
async function sendJson<T>(path: string, method: "POST" | "PUT", payload: unknown, token: string | null): Promise<T> { const response = await fetch(`/api/${path}`, { method, headers: { "Content-Type": "application/json", ...authHeaders(token) }, body: JSON.stringify(payload) }); if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`); return (await response.json()) as T; }
async function sendFormData<T>(path: string, payload: FormData, token: string | null): Promise<T> { const response = await fetch(`/api/${path}`, { method: "POST", headers: authHeaders(token), body: payload }); if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`); return (await response.json()) as T; }
function Field(props: { label: string; value: string; onChange: (value: string) => void; type?: "text" | "number" | "password"; placeholder?: string }) { return <label className="field"><span>{props.label}</span><input type={props.type ?? "text"} value={props.value} placeholder={props.placeholder} onChange={(event) => props.onChange(event.target.value)} /></label>; }
function ToggleField(props: { label: string; checked: boolean; onChange: (checked: boolean) => void }) { return <label className="toggle-field"><span>{props.label}</span><input type="checkbox" checked={props.checked} onChange={(event) => props.onChange(event.target.checked)} /></label>; }
function TabButton(props: { active: boolean; onClick: () => void; label: string }) { return <button className={props.active ? "tab-button is-active" : "tab-button"} type="button" onClick={props.onClick}>{props.label}</button>; }

function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const [state, setState] = React.useState<DashboardState | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [notice, setNotice] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);
  const [token, setToken] = React.useState<string | null>(() => window.localStorage.getItem("lpe.admin.token"));
  const [loginForm, setLoginForm] = React.useState({ email: "admin@example.test", password: "" });
  const [adminIdentity, setAdminIdentity] = React.useState<LoginResponse["admin"] | null>(null);
  const [page, setPage] = React.useState<PageKey>("server");
  const [serverTab, setServerTab] = React.useState<ServerTab>("status");
  const [domainTab, setDomainTab] = React.useState<DomainTab>("overview");
  const [antispamTab, setAntispamTab] = React.useState<AntispamTab>("content");
  const [auditTab, setAuditTab] = React.useState<AuditTab>("journal");
  const [operationsTab, setOperationsTab] = React.useState<OperationsTab>("protocols");
  const [selectedDomainId, setSelectedDomainId] = React.useState<string>("");
  const [serverDomainPanelOpen, setServerDomainPanelOpen] = React.useState(false);
  const [selectedServerDomainId, setSelectedServerDomainId] = React.useState<string>("");
  const [serverAdminPanelOpen, setServerAdminPanelOpen] = React.useState(false);
  const [selectedServerAdminId, setSelectedServerAdminId] = React.useState<string>("");
  const [rulePanelOpen, setRulePanelOpen] = React.useState(false);
  const [selectedRuleId, setSelectedRuleId] = React.useState<string>("");
  const [selectedAccountId, setSelectedAccountId] = React.useState<string>("");
  const [accountPanelMode, setAccountPanelMode] = React.useState<AccountPanelMode>("details");
  const [accountPanelOpen, setAccountPanelOpen] = React.useState(false);
  const [traceQuery, setTraceQuery] = React.useState("");
  const [traceResults, setTraceResults] = React.useState<TraceResult[]>([]);
  const [pstForms, setPstForms] = React.useState<Record<string, PstFormState>>({});

  const [domainForm, setDomainForm] = React.useState({ name: "", default_quota_mb: "4096", inbound_enabled: true, outbound_enabled: true });
  const [accountEditForm, setAccountEditForm] = React.useState({ local_part: "", display_name: "", quota_mb: "4096", status: "active", password: "" });
  const [accountTransferPath, setAccountTransferPath] = React.useState("");
  const [accountImportFile, setAccountImportFile] = React.useState<File | null>(null);
  const [aliasForm, setAliasForm] = React.useState({ source: "", target: "", kind: "forward" });
  const [adminForm, setAdminForm] = React.useState({ email: "", display_name: "", role: "domain-admin", rights_summary: "accounts, aliases, policies", password: "" });
  const [ruleForm, setRuleForm] = React.useState({ name: "", scope: "domain", action: "quarantine", status: "enabled" });
  const [serverForm, setServerForm] = React.useState<DashboardState["server_settings"] | null>(null);
  const [securityForm, setSecurityForm] = React.useState<DashboardState["security_settings"] | null>(null);
  const [localAiForm, setLocalAiForm] = React.useState<DashboardState["local_ai_settings"] | null>(null);
  const [antispamForm, setAntispamForm] = React.useState<DashboardState["antispam_settings"] | null>(null);
  const copy = messages[locale];

  React.useEffect(() => { document.documentElement.lang = locale; window.localStorage.setItem("lpe.locale", locale); }, [locale]);
  React.useEffect(() => { token ? window.localStorage.setItem("lpe.admin.token", token) : window.localStorage.removeItem("lpe.admin.token"); }, [token]);

  const syncState = React.useCallback((dashboard: DashboardState) => {
    React.startTransition(() => {
      setState(dashboard); setServerForm(dashboard.server_settings); setSecurityForm(dashboard.security_settings); setLocalAiForm(dashboard.local_ai_settings); setAntispamForm(dashboard.antispam_settings);
    });
    setSelectedDomainId((current) => current || dashboard.domains[0]?.id || "");
  }, []);

  const load = React.useCallback(async () => {
    if (!token) return;
    setBusy("load");
    try { const dashboard = await fetchJson<DashboardState>("console/dashboard", token); syncState(dashboard); setError(null); }
    catch (e) { setError(e instanceof Error ? e.message : "Unknown error"); if (e instanceof Error && e.message.includes("401")) setToken(null); }
    finally { setBusy(null); }
  }, [syncState, token]);
  React.useEffect(() => { void load(); }, [load]);

  async function loginAdmin() {
    setBusy("login");
    try { const response = await sendJson<LoginResponse>("auth/login", "POST", loginForm, null); setToken(response.token); setAdminIdentity(response.admin); setError(null); setNotice(copy.saved); }
    catch (e) { setError(e instanceof Error ? e.message : "Unknown error"); }
    finally { setBusy(null); }
  }

  async function mutate(action: string, path: string, method: "POST" | "PUT", payload: unknown, success: string, afterSuccess?: () => void) {
    setBusy(action);
    try { const dashboard = await sendJson<DashboardState>(path, method, payload, token); syncState(dashboard); setNotice(success); setError(null); afterSuccess?.(); }
    catch (e) { setError(e instanceof Error ? e.message : "Unknown error"); if (e instanceof Error && e.message.includes("401")) setToken(null); }
    finally { setBusy(null); }
  }

  async function searchTrace() {
    setBusy("trace");
    try { setTraceResults(await sendJson<TraceResult[]>("console/audit/email-trace-search", "POST", { query: traceQuery }, token)); setError(null); }
    catch (e) { setError(e instanceof Error ? e.message : "Unknown error"); }
    finally { setBusy(null); }
  }

  async function runPstJobs() {
    setBusy("pst-run");
    try { await sendJson("console/mailboxes/pst-jobs/run-pending", "POST", {}, token); await load(); setNotice(copy.saved); setError(null); }
    catch (e) { setError(e instanceof Error ? e.message : "Unknown error"); if (e instanceof Error && e.message.includes("401")) setToken(null); }
    finally { setBusy(null); }
  }

  async function uploadPstImport(mailbox: MailboxRecord, requestedBy: string) {
    if (!accountImportFile) { setError(copy.pstFile); return; }
    const payload = new FormData();
    payload.append("requested_by", requestedBy);
    payload.append("file", accountImportFile);
    setBusy(`pst-${mailbox.id}`);
    try {
      const dashboard = await sendFormData<DashboardState>(`console/mailboxes/${mailbox.id}/pst-upload`, payload, token);
      syncState(dashboard);
      setAccountImportFile(null);
      setNotice(copy.saved);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
      if (e instanceof Error && e.message.includes("401")) setToken(null);
    } finally {
      setBusy(null);
    }
  }

  function pstFormFor(mailboxId: string): PstFormState {
    return pstForms[mailboxId] ?? { direction: "export", server_path: "", requested_by: "admin@example.test" };
  }

  function primaryMailbox(account: AccountRecord): MailboxRecord | null {
    return account.mailboxes.find((mailbox) => mailbox.role === "inbox") ?? account.mailboxes[0] ?? null;
  }

  function openAccountPanel(account: AccountRecord, mode: AccountPanelMode) {
    const mailbox = primaryMailbox(account);
    setSelectedAccountId(account.id);
    setAccountPanelMode(mode);
    setAccountPanelOpen(true);
    setAccountEditForm({ local_part: account.email.split("@")[0] ?? "", display_name: account.display_name, quota_mb: String(account.quota_mb), status: account.status, password: "" });
    setAccountTransferPath("");
    setAccountImportFile(null);
    if (mailbox) {
      setPstForms((current) => ({
        ...current,
        [mailbox.id]: {
          ...pstFormFor(mailbox.id),
          direction: mode === "import" ? "import" : "export",
          requested_by: adminIdentity?.email ?? pstFormFor(mailbox.id).requested_by
        }
      }));
    }
  }

  function openNewAccountPanel() {
    setSelectedAccountId("");
    setAccountPanelMode("new");
    setAccountPanelOpen(true);
    setAccountEditForm({ local_part: "", display_name: "", quota_mb: String(selectedDomain?.default_quota_mb ?? 4096), status: "active", password: "" });
    setAccountTransferPath("");
    setAccountImportFile(null);
  }

  function closeAccountPanel() {
    setAccountPanelOpen(false);
    setSelectedAccountId("");
    setAccountImportFile(null);
  }

  function openNewDomainPanel() {
    setSelectedServerDomainId("");
    setDomainForm({ name: "", default_quota_mb: "4096", inbound_enabled: true, outbound_enabled: true });
    setServerDomainPanelOpen(true);
  }

  function openDomainPanel(domain: DashboardState["domains"][number]) {
    setSelectedServerDomainId(domain.id);
    setServerDomainPanelOpen(true);
  }

  function closeDomainPanel() {
    setServerDomainPanelOpen(false);
    setSelectedServerDomainId("");
  }

  function openNewAdminPanel() {
    setSelectedServerAdminId("");
    setAdminForm({ email: "", display_name: "", role: "domain-admin", rights_summary: "accounts, aliases, policies", password: "" });
    setServerAdminPanelOpen(true);
  }

  function openAdminPanel(admin: DashboardState["server_admins"][number]) {
    setSelectedServerAdminId(admin.id);
    setServerAdminPanelOpen(true);
  }

  function closeAdminPanel() {
    setServerAdminPanelOpen(false);
    setSelectedServerAdminId("");
  }

  function openNewRulePanel() {
    setSelectedRuleId("");
    setRuleForm({ name: "", scope: "domain", action: "quarantine", status: "enabled" });
    setRulePanelOpen(true);
  }

  function openRulePanel(rule: DashboardState["antispam_rules"][number]) {
    setSelectedRuleId(rule.id);
    setRulePanelOpen(true);
  }

  function closeRulePanel() {
    setRulePanelOpen(false);
    setSelectedRuleId("");
  }

  const selectedDomain = state?.domains.find((domain) => domain.id === selectedDomainId) ?? state?.domains[0] ?? null;
  const selectedServerDomain = state?.domains.find((domain) => domain.id === selectedServerDomainId) ?? null;
  const selectedServerAdmin = state?.server_admins.find((admin) => admin.id === selectedServerAdminId) ?? null;
  const selectedRule = state?.antispam_rules.find((rule) => rule.id === selectedRuleId) ?? null;
  const domainAccounts = selectedDomain ? (state?.accounts.filter((account) => account.email.endsWith(`@${selectedDomain.name}`)) ?? []) : [];
  const selectedAccount = domainAccounts.find((account) => account.id === selectedAccountId) ?? null;
  const selectedMailbox = selectedAccount ? primaryMailbox(selectedAccount) : null;
  const domainAliases = selectedDomain ? (state?.aliases.filter((alias) => alias.source.endsWith(`@${selectedDomain.name}`) || alias.target.endsWith(`@${selectedDomain.name}`)) ?? []) : [];
  const domainAdmins = selectedDomain ? (state?.server_admins.filter((admin) => admin.domain_id === selectedDomain.id || admin.domain_name === "All domains") ?? []) : [];
  const sidebarPages: { key: PageKey; label: string; icon: string }[] = [
    { key: "server", label: copy.pageServer, icon: "01" },
    { key: "domain", label: copy.pageDomain, icon: "02" },
    { key: "antispam", label: copy.pageAntispam, icon: "03" },
    { key: "audit", label: copy.pageAudit, icon: "04" },
    { key: "operations", label: copy.pageOperations, icon: "05" }
  ];

  if (!token) {
    return <main className="console-shell login-shell">
      <section className="login-card card form-stack">
        <p className="eyebrow">{copy.eyebrow}</p>
        <h1>{copy.loginTitle}</h1>
        <p>{copy.loginHelp}</p>
        {error ? <p className="feedback error">{error}</p> : null}
        <form className="form-stack" onSubmit={(event)=>{event.preventDefault(); void loginAdmin();}}>
          <Field label={copy.adminEmail} value={loginForm.email} onChange={(value)=>setLoginForm((current)=>({...current,email:value}))} />
          <Field label={copy.password} type="password" value={loginForm.password} onChange={(value)=>setLoginForm((current)=>({...current,password:value}))} />
          <button className="primary-button" type="submit" disabled={busy==="login"}>{copy.login}</button>
        </form>
      </section>
    </main>;
  }

  return <main className="console-shell">
    <aside className="sidebar">
      <div><p className="eyebrow">{copy.eyebrow}</p><h1>{copy.title}</h1><p className="sidebar-text">{copy.subtitle}</p></div>
      <nav className="page-list">{sidebarPages.map((entry) => <button key={entry.key} type="button" className={page === entry.key ? "page-button is-active" : "page-button"} onClick={() => setPage(entry.key)}><span className="page-icon">{entry.icon}</span><span>{entry.label}</span></button>)}</nav>
      <label className="locale-picker"><span>{copy.languageLabel}</span><select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>{supportedLocales.map((entry) => <option key={entry} value={entry}>{localeLabels[entry]}</option>)}</select></label>
    </aside>
    <section className="workspace">
      <header className="topbar"><div><h2>{sidebarPages.find((entry) => entry.key === page)?.label}</h2><p>{adminIdentity ? `${copy.banner} · ${adminIdentity.email}` : copy.banner}</p></div><div className="inline-form"><button className="secondary-button" type="button" onClick={() => void load()}>{copy.refresh}</button><button className="secondary-button" type="button" onClick={() => { setToken(null); setState(null); }}>{copy.logout}</button></div></header>
      {error ? <p className="feedback error">{error}</p> : null}
      {notice ? <p className="feedback notice">{notice}</p> : null}
      {!state ? <p className="feedback muted">{busy === "load" ? copy.loading : copy.noData}</p> : null}
      {state ? <>
        {page === "server" ? <section className="page-card">
          <div className="tabs">{(["status","server","security","ai","domains","admins"] as ServerTab[]).map((tab) => <TabButton key={tab} active={serverTab===tab} onClick={() => setServerTab(tab)} label={copy.serverTabs[tab]} />)}</div>
          {serverTab === "status" ? <div className="grid two"><article className="card"><h3>{copy.serverOverview}</h3><div className="stats-grid small"><div className="metric"><span>{copy.domains}</span><strong>{state.overview.total_domains}</strong></div><div className="metric"><span>{copy.accounts}</span><strong>{state.overview.total_accounts}</strong></div><div className="metric"><span>{copy.aliases}</span><strong>{state.overview.total_aliases}</strong></div><div className="metric"><span>{copy.queue}</span><strong>{state.overview.pending_queue_items}</strong></div></div></article><article className="card"><h3>{copy.protocolStatus}</h3><div className="list">{state.protocols.map((protocol) => <div className="row" key={protocol.name}><strong>{protocol.name}</strong><span>{protocol.bind_address}</span><span className={protocol.enabled ? "pill ok" : "pill warn"}>{protocol.state}</span></div>)}</div></article></div> : null}
          {serverTab === "server" && serverForm ? <form className="card form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("server","console/settings/server","PUT",serverForm,copy.saved);}}><h3>{copy.serverPolicies}</h3><div className="grid two"><Field label={copy.hostname} value={serverForm.primary_hostname} onChange={(value)=>setServerForm((c)=>c?{...c,primary_hostname:value}:c)} /><Field label={copy.defaultLocale} value={serverForm.default_locale} onChange={(value)=>setServerForm((c)=>c?{...c,default_locale:value}:c)} /><Field label={copy.adminBind} value={serverForm.admin_bind_address} onChange={(value)=>setServerForm((c)=>c?{...c,admin_bind_address:value}:c)} /><Field label={copy.smtpBind} value={serverForm.smtp_bind_address} onChange={(value)=>setServerForm((c)=>c?{...c,smtp_bind_address:value}:c)} /><Field label={copy.imapBind} value={serverForm.imap_bind_address} onChange={(value)=>setServerForm((c)=>c?{...c,imap_bind_address:value}:c)} /><Field label={copy.jmapBind} value={serverForm.jmap_bind_address} onChange={(value)=>setServerForm((c)=>c?{...c,jmap_bind_address:value}:c)} /><Field label={copy.maxMessage} type="number" value={String(serverForm.max_message_size_mb)} onChange={(value)=>setServerForm((c)=>c?{...c,max_message_size_mb:Number(value)||0}:c)} /><Field label={copy.tlsMode} value={serverForm.tls_mode} onChange={(value)=>setServerForm((c)=>c?{...c,tls_mode:value}:c)} /></div><button className="primary-button" disabled={busy==="server"} type="submit">{copy.save}</button></form> : null}
          {serverTab === "security" && securityForm ? <form className="card form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("security","console/settings/security","PUT",securityForm,copy.saved);}}><h3>{copy.securityPolicies}</h3><div className="grid two"><ToggleField label={copy.passwordLogin} checked={securityForm.password_login_enabled} onChange={(checked)=>setSecurityForm((c)=>c?{...c,password_login_enabled:checked}:c)} /><ToggleField label={copy.mfa} checked={securityForm.mfa_required_for_admins} onChange={(checked)=>setSecurityForm((c)=>c?{...c,mfa_required_for_admins:checked}:c)} /><Field label={copy.sessionTimeout} type="number" value={String(securityForm.session_timeout_minutes)} onChange={(value)=>setSecurityForm((c)=>c?{...c,session_timeout_minutes:Number(value)||0}:c)} /><Field label={copy.auditRetention} type="number" value={String(securityForm.audit_retention_days)} onChange={(value)=>setSecurityForm((c)=>c?{...c,audit_retention_days:Number(value)||0}:c)} /></div><button className="primary-button" disabled={busy==="security"} type="submit">{copy.save}</button></form> : null}
          {serverTab === "ai" && localAiForm ? <form className="card form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("ai","console/settings/local-ai","PUT",localAiForm,copy.saved);}}><h3>{copy.localAiPolicies}</h3><div className="grid two"><ToggleField label={copy.aiEnabled} checked={localAiForm.enabled} onChange={(checked)=>setLocalAiForm((c)=>c?{...c,enabled:checked}:c)} /><ToggleField label={copy.offlineOnly} checked={localAiForm.offline_only} onChange={(checked)=>setLocalAiForm((c)=>c?{...c,offline_only:checked}:c)} /><ToggleField label={copy.indexing} checked={localAiForm.indexing_enabled} onChange={(checked)=>setLocalAiForm((c)=>c?{...c,indexing_enabled:checked}:c)} /><Field label={copy.provider} value={localAiForm.provider} onChange={(value)=>setLocalAiForm((c)=>c?{...c,provider:value}:c)} /><Field label={copy.model} value={localAiForm.model} onChange={(value)=>setLocalAiForm((c)=>c?{...c,model:value}:c)} /></div><button className="primary-button" disabled={busy==="ai"} type="submit">{copy.save}</button></form> : null}
          {serverTab === "domains" ? <div className="management-workbench"><article className="card management-list-card"><div className="section-title-row"><h3>{copy.domainList}</h3><div className="management-actions"><button className="primary-button" type="button" onClick={openNewDomainPanel}>{copy.newItem}</button></div></div><div className="management-list full-width">{state.domains.map((domain)=><button key={domain.id} type="button" className={selectedServerDomainId===domain.id&&serverDomainPanelOpen?"management-list-item is-active":"management-list-item"} onClick={()=>openDomainPanel(domain)} onDoubleClick={()=>{setSelectedDomainId(domain.id); setPage("domain"); closeDomainPanel();}}><span className="management-main"><strong>{domain.name}</strong><span>{domain.status}</span></span><span className="management-meta"><span>{domain.default_quota_mb} MB</span><span className="pill">{domain.inbound_enabled ? copy.inbound : copy.disabled} / {domain.outbound_enabled ? copy.outbound : copy.disabled}</span></span><span className="management-actions">{copy.open}</span></button>)}</div></article>{serverDomainPanelOpen ? <div className="management-modal-backdrop" role="presentation" onClick={closeDomainPanel}><aside className="management-modal card" role="dialog" aria-modal="true" aria-label={copy.manageDomains} onClick={(event)=>event.stopPropagation()}><div className="form-stack"><div className="side-panel-header"><div><h3>{selectedServerDomain ? copy.manageDomains : copy.create}</h3><p className="muted">{selectedServerDomain?.name ?? copy.domainName}</p></div><button className="icon-button" type="button" aria-label={copy.close} onClick={closeDomainPanel}>×</button></div>{selectedServerDomain ? <div className="list"><div className="row"><strong>{copy.domainName}</strong><span>{selectedServerDomain.name}</span></div><div className="row"><strong>{copy.status}</strong><span>{selectedServerDomain.status}</span></div><div className="row"><strong>{copy.defaultQuota}</strong><span>{selectedServerDomain.default_quota_mb} MB</span></div><div className="row"><strong>{copy.inbound}</strong><span>{selectedServerDomain.inbound_enabled ? copy.enabled : copy.disabled}</span></div><div className="row"><strong>{copy.outbound}</strong><span>{selectedServerDomain.outbound_enabled ? copy.enabled : copy.disabled}</span></div><button className="primary-button" type="button" onClick={()=>{setSelectedDomainId(selectedServerDomain.id); setPage("domain"); closeDomainPanel();}}>{copy.selectedDomain}</button></div> : <form className="form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("domain","console/domains","POST",{ name: domainForm.name, default_quota_mb: Number(domainForm.default_quota_mb), inbound_enabled: domainForm.inbound_enabled, outbound_enabled: domainForm.outbound_enabled },copy.saved,()=>{setDomainForm({ name:"", default_quota_mb:"4096", inbound_enabled:true, outbound_enabled:true }); closeDomainPanel();});}}><Field label={copy.domainName} value={domainForm.name} onChange={(value)=>setDomainForm((c)=>({...c,name:value}))} placeholder="example.com" /><Field label={copy.defaultQuota} type="number" value={domainForm.default_quota_mb} onChange={(value)=>setDomainForm((c)=>({...c,default_quota_mb:value}))} /><ToggleField label={copy.inbound} checked={domainForm.inbound_enabled} onChange={(checked)=>setDomainForm((c)=>({...c,inbound_enabled:checked}))} /><ToggleField label={copy.outbound} checked={domainForm.outbound_enabled} onChange={(checked)=>setDomainForm((c)=>({...c,outbound_enabled:checked}))} /><button className="primary-button" disabled={busy==="domain"} type="submit">{copy.create}</button></form>}</div></aside></div> : null}</div> : null}
          {serverTab === "admins" ? <div className="management-workbench"><article className="card management-list-card"><div className="section-title-row"><h3>{copy.adminMatrix}</h3><div className="management-actions"><button className="primary-button" type="button" onClick={openNewAdminPanel}>{copy.newItem}</button></div></div><div className="management-list full-width">{state.server_admins.map((admin)=><button type="button" className={selectedServerAdminId===admin.id&&serverAdminPanelOpen?"management-list-item is-active":"management-list-item"} key={admin.id} onClick={()=>openAdminPanel(admin)}><span className="management-main"><strong>{admin.display_name}</strong><span>{admin.email}</span></span><span className="management-meta"><span>{admin.domain_name}</span><span className="pill">{admin.role}</span></span><span className="management-actions">{copy.open}</span></button>)}</div></article>{serverAdminPanelOpen ? <div className="management-modal-backdrop" role="presentation" onClick={closeAdminPanel}><aside className="management-modal card" role="dialog" aria-modal="true" aria-label={copy.administrators} onClick={(event)=>event.stopPropagation()}><div className="form-stack"><div className="side-panel-header"><div><h3>{selectedServerAdmin ? copy.administrators : copy.create}</h3><p className="muted">{selectedServerAdmin?.email ?? selectedDomain?.name ?? "All domains"}</p></div><button className="icon-button" type="button" aria-label={copy.close} onClick={closeAdminPanel}>×</button></div>{selectedServerAdmin ? <div className="list"><div className="row"><strong>{copy.displayName}</strong><span>{selectedServerAdmin.display_name}</span></div><div className="row"><strong>{copy.adminEmail}</strong><span>{selectedServerAdmin.email}</span></div><div className="row"><strong>{copy.role}</strong><span>{selectedServerAdmin.role}</span></div><div className="row"><strong>{copy.selectedDomain}</strong><span>{selectedServerAdmin.domain_name}</span></div><div className="row"><strong>{copy.rights}</strong><span>{selectedServerAdmin.rights_summary}</span></div></div> : <form className="form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("admin","console/admins","POST",{ domain_id: selectedDomain?.id ?? null, ...adminForm },copy.saved,()=>{setAdminForm({ email:"", display_name:"", role:"domain-admin", rights_summary:"accounts, aliases, policies", password:"" }); closeAdminPanel();});}}><Field label={copy.adminEmail} value={adminForm.email} onChange={(value)=>setAdminForm((c)=>({...c,email:value}))} placeholder="admin@example.com" /><Field label={copy.displayName} value={adminForm.display_name} onChange={(value)=>setAdminForm((c)=>({...c,display_name:value}))} /><Field label={copy.role} value={adminForm.role} onChange={(value)=>setAdminForm((c)=>({...c,role:value}))} /><Field label={copy.rights} value={adminForm.rights_summary} onChange={(value)=>setAdminForm((c)=>({...c,rights_summary:value}))} /><Field label={copy.password} type="password" value={adminForm.password} onChange={(value)=>setAdminForm((c)=>({...c,password:value}))} /><button className="primary-button" disabled={busy==="admin"} type="submit">{copy.create}</button></form>}</div></aside></div> : null}</div> : null}
        </section> : null}

        {page === "domain" ? <section className="page-card">
          <div className="toolbar-row"><label className="field compact"><span>{copy.selectedDomain}</span><select value={selectedDomain?.id ?? ""} onChange={(event)=>setSelectedDomainId(event.target.value)}>{state.domains.map((domain)=><option key={domain.id} value={domain.id}>{domain.name}</option>)}</select></label><div className="tabs">{(["overview","accounts","aliases","admins"] as DomainTab[]).map((tab)=><TabButton key={tab} active={domainTab===tab} onClick={()=>setDomainTab(tab)} label={copy.domainTabs[tab]} />)}</div></div>
          {selectedDomain ? <>
            {domainTab === "overview" ? <div className="grid two"><article className="card"><h3>{selectedDomain.name}</h3><div className="list"><div className="row"><strong>{copy.status}</strong><span>{selectedDomain.status}</span></div><div className="row"><strong>{copy.accounts}</strong><span>{domainAccounts.length}</span></div><div className="row"><strong>{copy.aliases}</strong><span>{domainAliases.length}</span></div><div className="row"><strong>{copy.administrators}</strong><span>{domainAdmins.length}</span></div></div></article><article className="card"><h3>{copy.domainPolicySnapshot}</h3><div className="list"><div className="row"><strong>{copy.defaultQuota}</strong><span>{selectedDomain.default_quota_mb} MB</span></div><div className="row"><strong>{copy.inbound}</strong><span>{selectedDomain.inbound_enabled ? copy.enabled : copy.disabled}</span></div><div className="row"><strong>{copy.outbound}</strong><span>{selectedDomain.outbound_enabled ? copy.enabled : copy.disabled}</span></div></div></article></div> : null}
            {domainTab === "accounts" ? <div className="account-workbench"><article className="card account-list-card"><div className="section-title-row"><h3>{copy.domainAccounts}</h3><div className="account-actions"><button className="primary-button" type="button" onClick={openNewAccountPanel}>{copy.newAccount}</button><button className="secondary-button" type="button" disabled={busy==="pst-run"} onClick={()=>void runPstJobs()}>{copy.runPstJobs}</button></div></div><div className="account-list full-width">{domainAccounts.map((account)=><div className={selectedAccountId===account.id&&accountPanelOpen?"account-list-item is-active":"account-list-item"} key={account.id} role="button" tabIndex={0} onClick={()=>openAccountPanel(account,"details")} onDoubleClick={()=>openAccountPanel(account,"details")} onKeyDown={(event)=>{ if (event.key === "Enter") openAccountPanel(account,"details"); }}><span className="account-main"><strong>{account.display_name}</strong><span>{account.email}</span></span><span className="account-meta"><span>{account.used_mb}/{account.quota_mb} MB</span><span className="pill">{account.status}</span></span><span className="account-actions" onClick={(event)=>event.stopPropagation()}><button className="icon-button" type="button" title={copy.pstImport} onClick={()=>openAccountPanel(account,"import")}>⇩</button><button className="icon-button" type="button" title={copy.pstExport} onClick={()=>openAccountPanel(account,"export")}>⇧</button></span></div>)}</div></article>{accountPanelOpen ? <div className="account-modal-backdrop" role="presentation" onClick={closeAccountPanel}><aside className="account-modal card" role="dialog" aria-modal="true" aria-label={copy.accountDetails} onClick={(event)=>event.stopPropagation()}><div className="form-stack"><div className="side-panel-header"><div><h3>{accountPanelMode === "new" ? copy.newAccount : accountPanelMode === "details" ? copy.accountDetails : accountPanelMode === "import" ? copy.pstImport : copy.pstExport}</h3><p className="muted">{accountPanelMode === "new" ? selectedDomain.name : selectedAccount?.email}</p></div><div className="account-actions">{selectedAccount ? <><button className={accountPanelMode==="details"?"icon-button is-active":"icon-button"} type="button" onClick={()=>openAccountPanel(selectedAccount,"details")}>✎</button><button className={accountPanelMode==="import"?"icon-button is-active":"icon-button"} type="button" onClick={()=>openAccountPanel(selectedAccount,"import")}>⇩</button><button className={accountPanelMode==="export"?"icon-button is-active":"icon-button"} type="button" onClick={()=>openAccountPanel(selectedAccount,"export")}>⇧</button></> : null}<button className="icon-button" type="button" aria-label={copy.close} onClick={closeAccountPanel}>×</button></div></div>{accountPanelMode === "new" || accountPanelMode === "details" ? <form className="form-stack" onSubmit={(e)=>{e.preventDefault(); if (accountPanelMode === "new") { const localPart = accountEditForm.local_part.trim(); const email = `${localPart}@${selectedDomain.name}`; void mutate("account","console/accounts","POST",{ email, display_name: accountEditForm.display_name || localPart, quota_mb: Number(accountEditForm.quota_mb), password: accountEditForm.password },copy.saved,closeAccountPanel); return; } if (!selectedAccount) { setError(copy.noData); return; } void mutate(`account-${selectedAccount.id}`,`console/accounts/${selectedAccount.id}`,"PUT",{ display_name: accountEditForm.display_name, quota_mb: Number(accountEditForm.quota_mb), status: accountEditForm.status, password: accountEditForm.password || null },copy.saved,()=>setAccountEditForm((current)=>({...current,password:""})));}}>{accountPanelMode === "new" ? <Field label={copy.localPart} value={accountEditForm.local_part} onChange={(value)=>setAccountEditForm((current)=>({...current,local_part:value}))} placeholder="alice" /> : null}<Field label={copy.displayName} value={accountEditForm.display_name} onChange={(value)=>setAccountEditForm((current)=>({...current,display_name:value}))} /><Field label={copy.quota} type="number" value={accountEditForm.quota_mb} onChange={(value)=>setAccountEditForm((current)=>({...current,quota_mb:value}))} />{accountPanelMode === "details" ? <label className="field"><span>{copy.status}</span><select value={accountEditForm.status} onChange={(event)=>setAccountEditForm((current)=>({...current,status:event.target.value}))}><option value="active">active</option><option value="disabled">disabled</option><option value="suspended">suspended</option></select></label> : null}<Field label={accountPanelMode === "new" ? copy.password : copy.newPassword} type="password" value={accountEditForm.password} onChange={(value)=>setAccountEditForm((current)=>({...current,password:value}))} placeholder={accountPanelMode === "new" ? "" : "Optional"} /><button className="primary-button" type="submit" disabled={accountPanelMode === "new" ? busy==="account" : selectedAccount ? busy===`account-${selectedAccount.id}` : true}>{accountPanelMode === "new" ? copy.create : copy.save}</button></form> : null}{selectedAccount && accountPanelMode !== "new" && accountPanelMode !== "details" ? <form className="form-stack" onSubmit={(e)=>{e.preventDefault(); if (!selectedMailbox) { setError(copy.noData); return; } const pstForm = pstFormFor(selectedMailbox.id); if (accountPanelMode === "import") { void uploadPstImport(selectedMailbox, pstForm.requested_by); return; } if (!accountTransferPath.trim()) { setError(copy.pstPath); return; } void mutate(`pst-${selectedMailbox.id}`,"console/mailboxes/pst-jobs","POST",{ mailbox_id: selectedMailbox.id, direction: accountPanelMode, server_path: accountTransferPath, requested_by: pstForm.requested_by },copy.saved,()=>setAccountTransferPath(""));}}>{selectedMailbox ? <p className="feedback muted">{copy.primaryMailbox}: {selectedMailbox.display_name}</p> : <p className="feedback error">{copy.noData}</p>}<Field label={copy.requestedBy} value={selectedMailbox ? pstFormFor(selectedMailbox.id).requested_by : (adminIdentity?.email ?? "admin@example.test")} onChange={(value)=>selectedMailbox ? setPstForms((current)=>({ ...current, [selectedMailbox.id]: { ...pstFormFor(selectedMailbox.id), requested_by: value, direction: accountPanelMode } })) : undefined} />{accountPanelMode === "import" ? <label className="field"><span>{copy.pstFile}</span><input type="file" accept=".pst" onChange={(event)=>setAccountImportFile(event.target.files?.[0] ?? null)} /></label> : <Field label={copy.pstPath} value={accountTransferPath} onChange={setAccountTransferPath} placeholder="/var/lib/lpe/exports/account.pst" />}<button className="primary-button" disabled={selectedMailbox ? busy===`pst-${selectedMailbox.id}` : true} type="submit">{accountPanelMode === "import" ? copy.pstImport : copy.pstExport}</button></form> : null}{selectedAccount ? <div className="sublist mailbox-history">{selectedAccount.mailboxes.flatMap((mailbox)=>mailbox.pst_jobs.map((job)=><span className={job.status === "completed" ? "pill ok" : job.status === "failed" ? "pill warn" : "pill"} key={job.id}>{mailbox.display_name} · {job.direction} · {job.status} · {job.processed_messages} · {job.error_message ?? job.server_path}</span>))}</div> : null}</div></aside></div> : null}</div> : null}
            {domainTab === "aliases" ? <div className="grid two"><form className="card form-stack" onSubmit={(e)=>{e.preventDefault(); const source = aliasForm.source.includes("@") ? aliasForm.source : `${aliasForm.source}@${selectedDomain.name}`; void mutate("alias","console/aliases","POST",{ source, target: aliasForm.target, kind: aliasForm.kind },copy.saved,()=>setAliasForm({ source:"", target:"", kind:"forward" }));}}><h3>{copy.aliases}</h3><Field label={copy.source} value={aliasForm.source} onChange={(value)=>setAliasForm((c)=>({...c,source:value}))} placeholder="support" /><Field label={copy.target} value={aliasForm.target} onChange={(value)=>setAliasForm((c)=>({...c,target:value}))} placeholder="team@example.com" /><Field label={copy.kind} value={aliasForm.kind} onChange={(value)=>setAliasForm((c)=>({...c,kind:value}))} /><button className="primary-button" disabled={busy==="alias"} type="submit">{copy.create}</button></form><article className="card"><h3>{copy.domainAliases}</h3><div className="list">{domainAliases.map((alias)=><div className="row" key={alias.id}><strong>{alias.source}</strong><span>{alias.target}</span><span className="pill">{alias.kind}</span></div>)}</div></article></div> : null}
            {domainTab === "admins" ? <article className="card"><h3>{copy.domainAdmins}</h3><div className="list">{domainAdmins.map((admin)=><div className="row" key={admin.id}><strong>{admin.display_name}</strong><span>{admin.email}</span><span>{admin.rights_summary}</span></div>)}</div></article> : null}
          </> : null}
        </section> : null}

        {page === "antispam" ? <section className="page-card">
          <div className="tabs">{(["content","engines","rules","quarantine"] as AntispamTab[]).map((tab)=><TabButton key={tab} active={antispamTab===tab} onClick={()=>setAntispamTab(tab)} label={copy.antispamTabs[tab]} />)}</div>
          {(antispamTab === "content" || antispamTab === "engines") && antispamForm ? <form className="card form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("antispam","console/settings/antispam","PUT",antispamForm,copy.saved);}}><h3>{antispamTab === "content" ? copy.contentFiltering : copy.antispamEngines}</h3><div className="grid two"><ToggleField label={copy.contentFiltering} checked={antispamForm.content_filtering_enabled} onChange={(checked)=>setAntispamForm((c)=>c?{...c,content_filtering_enabled:checked}:c)} /><ToggleField label={copy.quarantine} checked={antispamForm.quarantine_enabled} onChange={(checked)=>setAntispamForm((c)=>c?{...c,quarantine_enabled:checked}:c)} /><Field label={copy.spamEngine} value={antispamForm.spam_engine} onChange={(value)=>setAntispamForm((c)=>c?{...c,spam_engine:value}:c)} /><Field label={copy.quarantineRetention} type="number" value={String(antispamForm.quarantine_retention_days)} onChange={(value)=>setAntispamForm((c)=>c?{...c,quarantine_retention_days:Number(value)||0}:c)} /></div><button className="primary-button" disabled={busy==="antispam"} type="submit">{copy.save}</button></form> : null}
          {antispamTab === "rules" ? <div className="management-workbench"><article className="card management-list-card"><div className="section-title-row"><h3>{copy.ruleList}</h3><div className="management-actions"><button className="primary-button" type="button" onClick={openNewRulePanel}>{copy.newItem}</button></div></div><div className="management-list full-width">{state.antispam_rules.map((rule)=><button type="button" className={selectedRuleId===rule.id&&rulePanelOpen?"management-list-item is-active":"management-list-item"} key={rule.id} onClick={()=>openRulePanel(rule)}><span className="management-main"><strong>{rule.name}</strong><span>{rule.scope}</span></span><span className="management-meta"><span>{rule.action}</span><span className="pill">{rule.status}</span></span><span className="management-actions">{copy.open}</span></button>)}</div></article>{rulePanelOpen ? <div className="management-modal-backdrop" role="presentation" onClick={closeRulePanel}><aside className="management-modal card" role="dialog" aria-modal="true" aria-label={copy.filterRules} onClick={(event)=>event.stopPropagation()}><div className="form-stack"><div className="side-panel-header"><div><h3>{selectedRule ? copy.filterRules : copy.create}</h3><p className="muted">{selectedRule?.name ?? copy.ruleName}</p></div><button className="icon-button" type="button" aria-label={copy.close} onClick={closeRulePanel}>×</button></div>{selectedRule ? <div className="list"><div className="row"><strong>{copy.ruleName}</strong><span>{selectedRule.name}</span></div><div className="row"><strong>{copy.scope}</strong><span>{selectedRule.scope}</span></div><div className="row"><strong>{copy.action}</strong><span>{selectedRule.action}</span></div><div className="row"><strong>{copy.status}</strong><span>{selectedRule.status}</span></div></div> : <form className="form-stack" onSubmit={(e)=>{e.preventDefault(); void mutate("rule","console/antispam/rules","POST",ruleForm,copy.saved,()=>{setRuleForm({ name:"", scope:"domain", action:"quarantine", status:"enabled" }); closeRulePanel();});}}><Field label={copy.ruleName} value={ruleForm.name} onChange={(value)=>setRuleForm((c)=>({...c,name:value}))} /><Field label={copy.scope} value={ruleForm.scope} onChange={(value)=>setRuleForm((c)=>({...c,scope:value}))} /><Field label={copy.action} value={ruleForm.action} onChange={(value)=>setRuleForm((c)=>({...c,action:value}))} /><Field label={copy.status} value={ruleForm.status} onChange={(value)=>setRuleForm((c)=>({...c,status:value}))} /><button className="primary-button" disabled={busy==="rule"} type="submit">{copy.create}</button></form>}</div></aside></div> : null}</div> : null}
          {antispamTab === "quarantine" ? <article className="card"><h3>{copy.quarantine}</h3><div className="list">{state.quarantine_items.map((item)=><div className="row multi" key={item.id}><strong>{item.sender}</strong><span>{item.recipient}</span><span>{item.reason}</span><span>{item.created_at}</span></div>)}</div></article> : null}
        </section> : null}

        {page === "audit" ? <section className="page-card">
          <div className="tabs">{(["journal","trace"] as AuditTab[]).map((tab)=><TabButton key={tab} active={auditTab===tab} onClick={()=>setAuditTab(tab)} label={copy.auditTabs[tab]} />)}</div>
          {auditTab === "journal" ? <article className="card"><h3>{copy.auditJournal}</h3><div className="list">{state.audit_log.map((event)=><div className="row multi" key={event.id}><strong>{event.action}</strong><span>{event.subject}</span><span>{event.actor}</span><span>{event.timestamp}</span></div>)}</div></article> : null}
          {auditTab === "trace" ? <div className="card form-stack"><h3>{copy.emailTrace}</h3><div className="inline-form"><Field label={copy.searchQuery} value={traceQuery} onChange={setTraceQuery} placeholder="message-id, sender, subject, account" /><button className="primary-button" type="button" disabled={busy==="trace"} onClick={() => void searchTrace()}>{copy.search}</button></div><div className="list">{traceResults.map((result)=><div className="row multi" key={result.message_id}><strong>{result.subject}</strong><span>{result.sender}</span><span>{result.account_email}</span><span>{result.mailbox}</span><span>{result.received_at}</span></div>)}</div></div> : null}
        </section> : null}

        {page === "operations" ? <section className="page-card"><div className="tabs">{(["protocols","storage"] as OperationsTab[]).map((tab)=><TabButton key={tab} active={operationsTab===tab} onClick={()=>setOperationsTab(tab)} label={copy.operationsTabs[tab]} />)}</div>{operationsTab === "protocols" ? <article className="card"><h3>{copy.protocolStatus}</h3><div className="list">{state.protocols.map((protocol)=><div className="row" key={protocol.name}><strong>{protocol.name}</strong><span>{protocol.bind_address}</span><span className={protocol.enabled ? "pill ok" : "pill warn"}>{protocol.state}</span></div>)}</div></article> : null}{operationsTab === "storage" ? <article className="card"><h3>{copy.storageOverview}</h3><div className="list"><div className="row"><strong>{copy.primaryStore}</strong><span>{state.storage.primary_store}</span></div><div className="row"><strong>{copy.searchEngine}</strong><span>{state.storage.search_engine}</span></div><div className="row"><strong>{copy.replication}</strong><span>{state.storage.replication_mode}</span></div></div><div className="sublist">{state.storage.attachment_formats.map((format)=><span className="pill" key={format}>{format}</span>)}</div></article> : null}</section> : null}
      </> : null}
    </section>
  </main>;
}

ReactDOM.createRoot(document.getElementById("root")!).render(<React.StrictMode><App /></React.StrictMode>);

