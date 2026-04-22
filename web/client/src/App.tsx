import React from "react";
import { getInitialLocale, localeLabels, messages, setStoredLocale, supportedLocales, type Locale } from "./i18n";
import { Sidebar } from "./components/Sidebar";
import { MasterPane } from "./components/MasterPane";
import { MailDetail } from "./components/MailDetail";
import { EventEditor } from "./components/EventEditor";
import { ContactEditor } from "./components/ContactEditor";
import { SettingsWorkspace } from "./components/SettingsWorkspace";
import { useClientWorkspace } from "./useClientWorkspace";
import type { ClientIdentity } from "./client-types";
import "./styles.css";

type ClientLoginResponse = {
  token: string;
  account: ClientIdentity;
};

type ClientOidcMetadataResponse = {
  enabled: boolean;
  provider_label: string;
};

async function apiJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {})
    },
    credentials: "same-origin"
  });
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return (await response.json()) as T;
}

export function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const copy = messages[locale];
  const [authToken, setAuthToken] = React.useState<string | null>(() => window.localStorage.getItem("lpe.client.token"));
  const [identity, setIdentity] = React.useState<ClientIdentity | null>(null);
  const workspace = useClientWorkspace(copy, authToken, identity);
  const [loginForm, setLoginForm] = React.useState({ email: "", password: "", totp_code: "" });
  const [loginError, setLoginError] = React.useState("");
  const [loginBusy, setLoginBusy] = React.useState(false);
  const [oidcMetadata, setOidcMetadata] = React.useState<ClientOidcMetadataResponse | null>(null);
  const [accountMenuOpen, setAccountMenuOpen] = React.useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = React.useState(false);
  const [sidebarMobileOpen, setSidebarMobileOpen] = React.useState(false);
  const accountMenuRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    document.documentElement.lang = locale;
    setStoredLocale(locale);
  }, [locale]);

  React.useEffect(() => {
    authToken ? window.localStorage.setItem("lpe.client.token", authToken) : window.localStorage.removeItem("lpe.client.token");
  }, [authToken]);

  React.useEffect(() => {
    const hash = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const clientToken = hash.get("client_token");
    if (!clientToken) return;
    setAuthToken(clientToken);
    window.history.replaceState(null, "", window.location.pathname + window.location.search);
  }, []);

  React.useEffect(() => {
    if (!authToken) {
      setIdentity(null);
      return;
    }

    let cancelled = false;
    apiJson<ClientIdentity>("mail/auth/me", { headers: { Authorization: `Bearer ${authToken}` } })
      .then((account) => {
        if (!cancelled) setIdentity(account);
      })
      .catch(() => {
        if (!cancelled) {
          setAuthToken(null);
          setIdentity(null);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [authToken]);

  React.useEffect(() => {
    if (!accountMenuOpen) return;
    function handlePointerDown(event: PointerEvent) {
      if (accountMenuRef.current && !accountMenuRef.current.contains(event.target as Node)) {
        setAccountMenuOpen(false);
      }
    }
    window.addEventListener("pointerdown", handlePointerDown);
    return () => window.removeEventListener("pointerdown", handlePointerDown);
  }, [accountMenuOpen]);

  React.useEffect(() => {
    setSidebarMobileOpen(false);
  }, [workspace.section, workspace.folder]);

  React.useEffect(() => {
    apiJson<ClientOidcMetadataResponse>("mail/auth/oidc/metadata")
      .then(setOidcMetadata)
      .catch(() => setOidcMetadata({ enabled: false, provider_label: "" }));
  }, []);

  async function loginClient(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoginBusy(true);
    setLoginError("");
    try {
      const response = await apiJson<ClientLoginResponse>("mail/auth/login", {
        method: "POST",
        body: JSON.stringify(loginForm)
      });
      setAuthToken(response.token);
      setIdentity(response.account);
      setLoginForm((current) => ({ ...current, password: "", totp_code: "" }));
    } catch {
      setAuthToken(null);
      setIdentity(null);
      setLoginError(copy.loginError);
    } finally {
      setLoginBusy(false);
    }
  }

  async function loginWithOidc() {
    setLoginBusy(true);
    setLoginError("");
    try {
      const response = await apiJson<{ authorization_url: string }>("mail/auth/oidc/start");
      window.location.assign(response.authorization_url);
    } catch {
      setLoginBusy(false);
      setLoginError(copy.loginError);
    }
  }

  async function logoutClient() {
    if (authToken) {
      await apiJson("mail/auth/logout", { method: "POST", headers: { Authorization: `Bearer ${authToken}` } }).catch(() => undefined);
    }
    setAuthToken(null);
    setIdentity(null);
    setAccountMenuOpen(false);
  }

  if (!identity) {
    return (
      <main className="client-login-shell">
        <section className="client-login-card">
          <div className="brand-lockup">
            <div className="brand-mark">LPE</div>
            <div>
              <h1>{copy.productTitle}</h1>
              <p className="brand-subtitle">{copy.productSubtitle}</p>
            </div>
          </div>
          <div>
            <p className="eyebrow">{copy.sections.mail}</p>
            <h2>{copy.loginTitle}</h2>
            <p>{copy.loginHelp}</p>
          </div>
          <form className="client-login-form" onSubmit={loginClient}>
            <label className="field">
              <span>{copy.loginEmail}</span>
              <input type="email" value={loginForm.email} autoComplete="username" required onChange={(event) => setLoginForm((current) => ({ ...current, email: event.target.value }))} />
            </label>
            <label className="field">
              <span>{copy.loginPassword}</span>
              <input type="password" value={loginForm.password} autoComplete="current-password" required onChange={(event) => setLoginForm((current) => ({ ...current, password: event.target.value }))} />
            </label>
            <label className="field">
              <span>{copy.loginTotp}</span>
              <input type="text" value={loginForm.totp_code} inputMode="numeric" autoComplete="one-time-code" onChange={(event) => setLoginForm((current) => ({ ...current, totp_code: event.target.value }))} />
            </label>
            {loginError ? <p className="login-error">{loginError}</p> : null}
            <button className="primary-button" type="submit" disabled={loginBusy}>{copy.loginSubmit}</button>
            {oidcMetadata?.enabled ? (
              <>
                <p className="feedback muted">{copy.loginOrDivider}</p>
                <button className="ghost-button" type="button" disabled={loginBusy} onClick={() => void loginWithOidc()}>
                  {copy.loginOidc}{oidcMetadata.provider_label ? ` · ${oidcMetadata.provider_label}` : ""}
                </button>
              </>
            ) : null}
          </form>
          <label className="locale-picker">
            <span>{copy.languageLabel}</span>
            <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
              {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
            </select>
          </label>
        </section>
      </main>
    );
  }

  const isMailWorkspace = workspace.section === "mail";
  const showMailPane = isMailWorkspace;
  const visibleCount = workspace.section === "mail"
    ? workspace.filtered.length
    : workspace.section === "calendar"
      ? workspace.filteredEvents.length
      : workspace.section === "contacts"
        ? workspace.filteredContacts.length
        : (workspace.collaboration?.outgoingContacts.length ?? 0)
          + (workspace.collaboration?.outgoingCalendars.length ?? 0)
          + (workspace.collaboration?.outgoingTaskLists.length ?? 0)
          + (workspace.mailboxDelegation?.outgoingMailboxes.length ?? 0)
          + (workspace.sieve?.scripts.length ?? 0);
  const attachmentCount = workspace.section === "mail"
    ? workspace.filtered.reduce((total, item) => total + item.attachments.length, 0)
    : 0;
  const unreadCount = workspace.section === "mail"
    ? workspace.filtered.filter((item) => item.unread).length
    : 0;
  const workspaceTitle = workspace.section === "mail"
    ? copy.folders[workspace.folder]
    : copy.altViews[workspace.section];
  const workspaceBody = workspace.section === "mail"
    ? copy.heroBody
    : workspace.section === "calendar"
      ? copy.calendarBody
      : workspace.section === "contacts"
        ? copy.contactsBody
        : "Delegation, booking, and filtering stay synchronized with the canonical server state.";

  return (
    <main className="app-shell">
      <header className="app-header">
        <div className="app-header-left">
          <button className="header-action shell-toggle" type="button" aria-label={sidebarMobileOpen ? copy.editorActions.cancel : copy.accountMenuLabel} aria-expanded={sidebarMobileOpen} onClick={() => setSidebarMobileOpen((value) => !value)}>☰</button>
          <span className="header-app-icon">▦</span>
          <div className="header-product">
            <strong>{copy.productTitle}</strong>
            <span>{copy.productSubtitle}</span>
          </div>
        </div>
        <div className="search-shell is-header">
          <span className="search-icon">⌕</span>
          <input type="search" value={workspace.query} onChange={(event) => workspace.setQuery(event.target.value)} placeholder={copy.searchPlaceholder} aria-label={copy.searchPlaceholder} />
        </div>
        <div className="app-header-right">
          <div className="account-menu-shell" ref={accountMenuRef}>
            <button className="account-menu-trigger" type="button" aria-haspopup="menu" aria-expanded={accountMenuOpen} aria-label={copy.accountMenuLabel} onClick={() => setAccountMenuOpen((value) => !value)}>
              <span className="header-account">{copy.signedInAs.replace("{email}", identity.email)}</span>
            </button>
            {accountMenuOpen ? (
              <div className="account-menu-popover" role="menu">
                <strong>{copy.accountMenuTitle}</strong>
                <span>{identity.email}</span>
                <button className="ghost-button" type="button" onClick={() => void logoutClient()}>{copy.logout}</button>
              </div>
            ) : null}
          </div>
        </div>
      </header>

      {sidebarMobileOpen ? <button className="shell-overlay" type="button" aria-label={copy.editorActions.cancel} onClick={() => setSidebarMobileOpen(false)} /> : null}
      <div className={sidebarCollapsed ? "shell-row is-sidebar-collapsed" : "shell-row"}>
        <Sidebar
          copy={copy}
          section={workspace.section}
          setSection={workspace.setSection}
          folder={workspace.folder}
          setFolder={workspace.setFolder}
          counts={workspace.counts}
          unreadCount={workspace.mail.filter((item) => item.unread).length}
          eventCount={workspace.events.length}
          draftCount={workspace.mail.filter((item) => item.folder === "drafts").length}
          mailboxOwner={identity.email}
          onCompose={() => workspace.openComposer("new")}
          onCloseComposer={workspace.closeComposer}
          collapsed={sidebarCollapsed}
          mobileOpen={sidebarMobileOpen}
          onToggleCollapse={() => setSidebarCollapsed((value) => !value)}
          onCloseMobile={() => setSidebarMobileOpen(false)}
        />

        <section className="workspace">
          <div className="workspace-toolbar">
            <div className="workspace-toolbar-actions">
              <button className="primary-button workspace-compose-button" type="button" onClick={() => workspace.openComposer("new")}>{copy.compose}</button>
            </div>
            <div className="workspace-toolbar-summary">
              {isMailWorkspace ? <span className="workspace-chip">{`Unread ${unreadCount}`}</span> : null}
              {isMailWorkspace ? <span className="workspace-chip">{`Attachments ${attachmentCount}`}</span> : null}
              <button className="ghost-button" type="button" onClick={() => void workspace.refreshWorkspace()}>{copy.topActions.sync}</button>
              <label className="locale-picker compact">
                <span>{copy.languageLabel}</span>
                <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
                  {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
                </select>
              </label>
            </div>
          </div>

          <section className="workspace-hero-card">
            <div>
              <p className="workspace-hero-eyebrow">{copy.sections[workspace.section]}</p>
              <h1>{workspaceTitle}</h1>
              <p>{workspaceBody}</p>
            </div>
            <div className="workspace-hero-meta">
              <span className="workspace-stat-pill">{`${visibleCount} visible`}</span>
              <span className="workspace-stat-pill is-soft">{copy.productSubtitle}</span>
            </div>
          </section>

          {workspace.notice ? <div className="notice-banner">{workspace.notice}</div> : null}

          <div className={showMailPane || workspace.section !== "mail" ? "content-grid has-detail" : "content-grid"}>
            {workspace.section !== "settings" ? (
              <MasterPane
                copy={copy}
                section={workspace.section}
                folder={workspace.folder}
                mode={workspace.mode}
                filteredMessages={workspace.filtered}
                events={workspace.filteredEvents}
                contacts={workspace.filteredContacts}
                messageId={workspace.messageId}
                eventId={workspace.eventId}
                contactId={workspace.contactId}
                onSelectMessage={workspace.setMessageId}
                onSelectEvent={workspace.setEventId}
                onSelectContact={workspace.setContactId}
              />
            ) : null}

            {showMailPane ? (
              <section className="detail-pane">
                <MailDetail
                  copy={copy}
                  current={workspace.current}
                  mode={workspace.mode}
                  draft={workspace.draft}
                  composerMailboxes={workspace.composerMailboxes}
                  setDraft={workspace.setDraft}
                  onReply={(message) => workspace.openComposer("reply", message)}
                  onForward={(message) => workspace.openComposer("forward", message)}
                  onCancel={workspace.closeComposer}
                  onSaveDraft={() => void workspace.saveMessage(true)}
                  onSend={() => void workspace.saveMessage(false)}
                  onDeleteDraft={() => void workspace.deleteDraft()}
                />
              </section>
            ) : null}

            {workspace.section === "calendar" ? (
            <section className="detail-pane">
              <EventEditor
                copy={copy}
                currentEvent={workspace.currentEvent}
                eventForm={workspace.eventForm}
                setEventForm={workspace.setEventForm}
                resources={workspace.resources}
                onNew={workspace.resetEventForm}
                onSave={() => void workspace.saveEvent()}
              />
            </section>
            ) : null}

            {workspace.section === "contacts" ? (
            <section className="detail-pane">
              <ContactEditor
                copy={copy}
                currentContact={workspace.currentContact}
                contactForm={workspace.contactForm}
                setContactForm={workspace.setContactForm}
                onNew={workspace.resetContactForm}
                onSave={() => void workspace.saveContact()}
              />
            </section>
            ) : null}

            {workspace.section === "settings" ? (
            <section className="detail-pane detail-pane-wide">
              <SettingsWorkspace
                copy={copy}
                collaboration={workspace.collaboration}
                taskLists={workspace.taskLists}
                mailboxDelegation={workspace.mailboxDelegation}
                sieve={workspace.sieve}
                shareForm={workspace.shareForm}
                setShareForm={workspace.setShareForm}
                mailboxForm={workspace.mailboxForm}
                setMailboxForm={workspace.setMailboxForm}
                sieveForm={workspace.sieveForm}
                setSieveForm={workspace.setSieveForm}
                onSaveShare={() => void workspace.saveShare()}
                onDeleteShare={(kind, granteeAccountId, taskListId) => void workspace.deleteShare(kind, granteeAccountId, taskListId)}
                onSaveMailboxDelegation={() => void workspace.saveMailboxDelegation()}
                onDeleteMailboxDelegation={(granteeAccountId) => void workspace.deleteMailboxDelegation(granteeAccountId)}
                onSaveSenderDelegation={() => void workspace.saveSenderDelegation()}
                onDeleteSenderDelegation={(senderRight, granteeAccountId) => void workspace.deleteSenderDelegation(senderRight, granteeAccountId)}
                onSaveSieve={() => void workspace.saveSieve()}
                onLoadSieve={(name) => void workspace.loadSieveScript(name)}
                onDeleteSieve={(name) => void workspace.deleteSieve(name)}
                onSetActiveSieve={(name) => void workspace.activateSieve(name)}
              />
            </section>
            ) : null}
          </div>
        </section>
      </div>
    </main>
  );
}
