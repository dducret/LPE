import React from "react";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import { Sidebar } from "./components/Sidebar";
import { MasterPane } from "./components/MasterPane";
import { MailDetail } from "./components/MailDetail";
import { EventEditor } from "./components/EventEditor";
import { ContactEditor } from "./components/ContactEditor";
import { useClientWorkspace } from "./useClientWorkspace";
import type { ClientIdentity } from "./client-types";
import "./styles.css";

type ClientLoginResponse = {
  token: string;
  account: ClientIdentity;
};

async function apiJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {})
    }
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
  const [loginForm, setLoginForm] = React.useState({ email: "", password: "" });
  const [loginError, setLoginError] = React.useState("");
  const [loginBusy, setLoginBusy] = React.useState(false);
  const [accountMenuOpen, setAccountMenuOpen] = React.useState(false);
  const accountMenuRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

  React.useEffect(() => {
    authToken ? window.localStorage.setItem("lpe.client.token", authToken) : window.localStorage.removeItem("lpe.client.token");
  }, [authToken]);

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
      setLoginForm((current) => ({ ...current, password: "" }));
    } catch {
      setAuthToken(null);
      setIdentity(null);
      setLoginError(copy.loginError);
    } finally {
      setLoginBusy(false);
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
            {loginError ? <p className="login-error">{loginError}</p> : null}
            <button className="primary-button" type="submit" disabled={loginBusy}>{copy.loginSubmit}</button>
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

  const showMailPane = workspace.section === "mail" && (workspace.mode !== "closed" || workspace.current !== null);
  const visibleCount = workspace.section === "mail"
    ? workspace.filtered.length
    : workspace.section === "calendar"
      ? workspace.filteredEvents.length
      : workspace.filteredContacts.length;

  return (
    <main className="app-shell">
      <header className="app-header">
        <div className="app-header-left">
          <span className="header-app-icon">▦</span>
          <strong>{copy.productTitle}</strong>
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
          <button className="header-action" type="button" onClick={workspace.notifyFeaturePending} aria-label={copy.topActions.rules}>◫</button>
          <button className="header-action" type="button" onClick={workspace.notifyFeaturePending} aria-label={copy.topActions.schedule}>✉</button>
          <button className="header-action" type="button" onClick={workspace.notifyFeaturePending} aria-label={copy.accountMenuLabel}>⚙</button>
        </div>
      </header>

      <div className="shell-row">
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
          onAuxAction={workspace.notifyFeaturePending}
        />

        <section className="workspace">
          <div className="command-strip">
            <div className="command-tabs">
              {copy.shellTabs.map((tab, index) => <button key={tab} className={index === 1 ? "command-tab is-active" : "command-tab"} type="button">{tab}</button>)}
            </div>
            <div className="strip-meta">{copy.rightPaneTitle}</div>
          </div>

          <div className="ribbon">
            <button className="primary-button ribbon-compose" type="button" onClick={() => workspace.openComposer("new")}>{copy.compose}</button>
            {copy.ribbonActions.map((action) => <button key={action} className="ribbon-button" type="button" onClick={workspace.notifyFeaturePending}>{action}</button>)}
            <div className="ribbon-separator" />
            {copy.ribbonSecondary.map((action) => <button key={action} className="ribbon-button" type="button" onClick={workspace.notifyFeaturePending}>{action}</button>)}
          </div>

          <header className="workspace-meta">
            <div className="workspace-meta-left">
              <span className="status-pill">{copy.sections[workspace.section]}</span>
              <span className="status-pill is-muted">{visibleCount} visible</span>
              <span className="workspace-caption">{copy.productSubtitle}</span>
            </div>
            <div className="workspace-meta-right">
            <button className="ghost-button" type="button" onClick={() => void workspace.refreshWorkspace()}>{copy.topActions.sync}</button>
            <button className="ghost-button" type="button" onClick={workspace.notifyFeaturePending}>{copy.topActions.rules}</button>
            <button className="ghost-button" type="button" onClick={workspace.notifyFeaturePending}>{copy.topActions.schedule}</button>
            <label className="locale-picker">
              <span>{copy.languageLabel}</span>
              <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
                {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
              </select>
            </label>
            </div>
          </header>

          {workspace.notice ? <div className="notice-banner">{workspace.notice}</div> : null}

          <div className={showMailPane || workspace.section !== "mail" ? "content-grid has-detail" : "content-grid"}>
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
              onToolbarAction={workspace.notifyFeaturePending}
            />

            {showMailPane ? (
            <section className="detail-pane">
              <MailDetail
                copy={copy}
                current={workspace.current}
                mode={workspace.mode}
                draft={workspace.draft}
                setDraft={workspace.setDraft}
                onReply={(message) => workspace.openComposer("reply", message)}
                onForward={(message) => workspace.openComposer("forward", message)}
                onCancel={workspace.closeComposer}
                onSaveDraft={() => void workspace.saveMessage(true)}
                onSend={() => void workspace.saveMessage(false)}
                onDeleteDraft={() => void workspace.deleteDraft()}
                onArchive={workspace.notifyFeaturePending}
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
          </div>
        </section>
      </div>
    </main>
  );
}
