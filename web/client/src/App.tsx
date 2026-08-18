import React from "react";
import { getInitialLocale, localeLabels, messages, setStoredLocale, supportedLocales, type Locale } from "./i18n";
import { Sidebar } from "./components/Sidebar";
import { MasterPane } from "./components/MasterPane";
import { MailDetail } from "./components/MailDetail";
import { EventEditor } from "./components/EventEditor";
import { ContactEditor } from "./components/ContactEditor";
import { CanonicalItemEditor } from "./components/CanonicalItemEditor";
import { SettingsWorkspace } from "./components/SettingsWorkspace";
import { useClientWorkspace } from "./useClientWorkspace";
import type { ClientIdentity } from "./client-types";
import { Button, Card, Input, Select } from "../../ui/src/components/primitives";
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
  const [authToken, setAuthToken] = React.useState<string | null>("session");
  const [identity, setIdentity] = React.useState<ClientIdentity | null>(null);
  const [loginForm, setLoginForm] = React.useState({ email: "", password: "", totp_code: "" });
  const [loginError, setLoginError] = React.useState("");
  const [loginBusy, setLoginBusy] = React.useState(false);
  const [oidcMetadata, setOidcMetadata] = React.useState<ClientOidcMetadataResponse | null>(null);
  const [accountMenuOpen, setAccountMenuOpen] = React.useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = React.useState(false);
  const [sidebarMobileOpen, setSidebarMobileOpen] = React.useState(false);
  const [mobileDetailOpen, setMobileDetailOpen] = React.useState(false);
  const [isNarrowScreen, setIsNarrowScreen] = React.useState(() => window.matchMedia("(max-width: 900px)").matches);
  const accountMenuRef = React.useRef<HTMLDivElement | null>(null);
  const accountMenuTriggerRef = React.useRef<HTMLButtonElement | null>(null);
  const accountMenuActionRef = React.useRef<HTMLButtonElement | null>(null);
  const sidebarTriggerRef = React.useRef<HTMLButtonElement | null>(null);

  const closeAccountMenu = React.useCallback((restoreFocus = true) => {
    setAccountMenuOpen(false);
    if (restoreFocus) requestAnimationFrame(() => accountMenuTriggerRef.current?.focus());
  }, []);
  const closeSidebarMobile = React.useCallback((restoreFocus = true) => {
    setSidebarMobileOpen(false);
    if (restoreFocus) requestAnimationFrame(() => sidebarTriggerRef.current?.focus());
  }, []);
  const handleSessionExpired = React.useCallback(() => {
    setAuthToken(null);
    setIdentity(null);
    setAccountMenuOpen(false);
    setSidebarMobileOpen(false);
    setMobileDetailOpen(false);
    window.history.replaceState(null, "", "/mail/");
  }, []);
  const workspace = useClientWorkspace(copy, authToken, identity, handleSessionExpired);

  React.useEffect(() => {
    document.documentElement.lang = locale;
    setStoredLocale(locale);
  }, [locale]);

  React.useEffect(() => {
    if (!authToken) {
      setIdentity(null);
      return;
    }

    let cancelled = false;
    apiJson<ClientIdentity>("mail/auth/me")
      .then((account) => {
        if (!cancelled) setIdentity(account);
      })
      .catch(() => {
        if (!cancelled) {
          handleSessionExpired();
        }
      });

    return () => {
      cancelled = true;
    };
  }, [authToken, handleSessionExpired]);

  React.useEffect(() => {
    if (!accountMenuOpen) return;
    function handlePointerDown(event: PointerEvent) {
      if (accountMenuRef.current && !accountMenuRef.current.contains(event.target as Node)) {
        closeAccountMenu();
      }
    }
    window.addEventListener("pointerdown", handlePointerDown);
    return () => window.removeEventListener("pointerdown", handlePointerDown);
  }, [accountMenuOpen, closeAccountMenu]);

  React.useEffect(() => {
    if (!accountMenuOpen) return;
    accountMenuActionRef.current?.focus();
  }, [accountMenuOpen]);

  React.useEffect(() => {
    const mediaQuery = window.matchMedia("(max-width: 900px)");
    const updateScreen = () => setIsNarrowScreen(mediaQuery.matches);
    updateScreen();
    mediaQuery.addEventListener("change", updateScreen);
    return () => mediaQuery.removeEventListener("change", updateScreen);
  }, []);

  React.useEffect(() => {
    function handleEscape(event: KeyboardEvent) {
      if (event.key !== "Escape") return;
      if (accountMenuOpen) {
        event.preventDefault();
        closeAccountMenu();
      } else if (sidebarMobileOpen) {
        event.preventDefault();
        closeSidebarMobile();
      }
    }
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [accountMenuOpen, closeAccountMenu, closeSidebarMobile, sidebarMobileOpen]);

  React.useEffect(() => {
    if (sidebarMobileOpen) closeSidebarMobile(false);
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
      setAuthToken("session");
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
    if (authToken) await apiJson("mail/auth/logout", { method: "POST" }).catch(() => undefined);
    setAuthToken(null);
    setIdentity(null);
    setAccountMenuOpen(false);
  }

  if (!identity) {
    return (
      <main className="client-login-shell">
        <Card as="section" className="client-login-card">
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
              <Input type="email" value={loginForm.email} autoComplete="username" required onChange={(event) => setLoginForm((current) => ({ ...current, email: event.target.value }))} />
            </label>
            <label className="field">
              <span>{copy.loginPassword}</span>
              <Input type="password" value={loginForm.password} autoComplete="current-password" required onChange={(event) => setLoginForm((current) => ({ ...current, password: event.target.value }))} />
            </label>
            <label className="field">
              <span>{copy.loginTotp}</span>
              <Input type="text" value={loginForm.totp_code} inputMode="numeric" autoComplete="one-time-code" onChange={(event) => setLoginForm((current) => ({ ...current, totp_code: event.target.value }))} />
            </label>
            {loginError ? <p className="login-error">{loginError}</p> : null}
            <Button variant="primary" type="submit" disabled={loginBusy}>{copy.loginSubmit}</Button>
            {oidcMetadata?.enabled ? (
              <>
                <p className="feedback muted">{copy.loginOrDivider}</p>
                <Button variant="ghost" type="button" disabled={loginBusy} onClick={() => void loginWithOidc()}>
                  {copy.loginOidc}{oidcMetadata.provider_label ? ` · ${oidcMetadata.provider_label}` : ""}
                </Button>
              </>
            ) : null}
          </form>
          <label className="locale-picker">
            <span>{copy.languageLabel}</span>
            <Select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
              {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
            </Select>
          </label>
        </Card>
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
        : workspace.section === "tasks"
          ? workspace.filteredTasks.length
          : workspace.section === "notes"
            ? workspace.filteredNotes.length
            : workspace.section === "journal"
              ? workspace.filteredJournalEntries.length
              : workspace.section === "reminders"
                ? workspace.filteredReminders.length
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
    ? workspace.folder.startsWith("mailbox:")
      ? workspace.mailboxes.find((mailbox) => `mailbox:${mailbox.id}` === workspace.folder)?.name ?? copy.folders.inbox
      : copy.folders[workspace.folder as keyof typeof copy.folders]
    : copy.altViews[workspace.section];
  const pushState = workspace.syncStatus.pushConnected
    ? copy.syncStatus.connected
    : copy.syncStatus.reconnecting;

  return (
    <main className="app-shell">
      <header className="app-header">
        <div className="app-header-left">
          <button
            ref={sidebarTriggerRef}
            className="header-action shell-toggle"
            type="button"
            aria-label={sidebarMobileOpen ? copy.navigation.close : copy.navigation.open}
            aria-expanded={sidebarMobileOpen}
            aria-controls="client-sidebar"
            onClick={() => sidebarMobileOpen ? closeSidebarMobile(false) : setSidebarMobileOpen(true)}
          ><span className="menu-icon" aria-hidden="true" /></button>
          <span className="header-app-icon" aria-hidden="true"><span className="app-grid-icon" /></span>
          <div className="header-product">
            <strong>{copy.productTitle}</strong>
            <span>{copy.productSubtitle}</span>
          </div>
        </div>
        <div className="search-shell is-header">
          <span className="search-icon" aria-hidden="true"><span /></span>
          <input type="search" value={workspace.query} onChange={(event) => workspace.setQuery(event.target.value)} placeholder={copy.searchPlaceholder} aria-label={copy.searchPlaceholder} />
        </div>
        <div className="app-header-right">
          <div className="account-menu-shell" ref={accountMenuRef}>
            <button ref={accountMenuTriggerRef} className="account-menu-trigger" type="button" aria-haspopup="dialog" aria-expanded={accountMenuOpen} aria-controls="account-menu-popover" aria-label={copy.accountMenuLabel} onClick={() => setAccountMenuOpen((value) => !value)}>
              <span className="header-account">{copy.signedInAs.replace("{email}", identity.email)}</span>
            </button>
            {accountMenuOpen ? (
              <div className="account-menu-popover" id="account-menu-popover" role="dialog" aria-modal="false" aria-labelledby="account-menu-title">
                <strong id="account-menu-title">{copy.accountMenuTitle}</strong>
                <span>{identity.email}</span>
                <Button ref={accountMenuActionRef} variant="ghost" size="sm" type="button" onClick={() => void logoutClient()}>{copy.logout}</Button>
              </div>
            ) : null}
          </div>
        </div>
      </header>

      {sidebarMobileOpen ? <button className="shell-overlay" type="button" aria-label={copy.navigation.close} onClick={() => closeSidebarMobile()} /> : null}
      <div className={sidebarCollapsed ? "shell-row is-sidebar-collapsed" : "shell-row"}>
        <Sidebar
          copy={copy}
          section={workspace.section}
          setSection={workspace.setSection}
          folder={workspace.folder}
          setFolder={workspace.setFolder}
          counts={workspace.counts}
          customMailboxes={workspace.mailboxes}
          unreadCount={workspace.mail.filter((item) => item.unread).length}
          eventCount={workspace.events.length}
          draftCount={workspace.mail.filter((item) => item.folder === "drafts").length}
          mailboxOwner={identity.email}
          onCompose={() => { workspace.openComposer("new"); setMobileDetailOpen(true); }}
          onCloseComposer={workspace.closeComposer}
          collapsed={sidebarCollapsed}
          mobileOpen={sidebarMobileOpen}
          isNarrowScreen={isNarrowScreen}
          onToggleCollapse={() => setSidebarCollapsed((value) => !value)}
          onCloseMobile={() => closeSidebarMobile()}
        />

        <section className="workspace">
          <div className="workspace-toolbar">
            <div className="workspace-toolbar-actions">
              <Button className="workspace-compose-button" variant="primary" type="button" onClick={() => { workspace.openComposer("new"); setMobileDetailOpen(true); }}>{copy.compose}</Button>
              {isMailWorkspace && workspace.mailboxAccounts.length > 1 ? (
                <label className="locale-picker compact">
                  <span>{copy.mailboxLabel}</span>
                  <Select value={workspace.workspaceMailboxAccountId} onChange={(event) => workspace.selectWorkspaceMailbox(event.target.value)}>
                    {workspace.mailboxAccounts.map((mailbox) => <option key={mailbox.accountId} value={mailbox.accountId}>{`${mailbox.displayName} <${mailbox.email}>`}</option>)}
                  </Select>
                </label>
              ) : null}
            </div>
            <div className="workspace-toolbar-summary">
              {isMailWorkspace ? <span className="workspace-chip">{copy.summaryUnread.replace("{count}", String(unreadCount))}</span> : null}
              {isMailWorkspace ? <span className="workspace-chip">{copy.attachmentCount.replace("{count}", String(attachmentCount))}</span> : null}
              <Button variant="ghost" type="button" onClick={() => void workspace.refreshWorkspace()}>{copy.topActions.sync}</Button>
              <label className="locale-picker compact">
                <span>{copy.languageLabel}</span>
                <Select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
                  {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
                </Select>
              </label>
            </div>
          </div>

          <section className="workspace-heading-panel">
            <div>
              <h1>{workspaceTitle}</h1>
            </div>
            <div className="workspace-hero-meta">
              <span className="workspace-stat-pill">{copy.messageCount.replace("{count}", String(visibleCount))}</span>
              <span className="workspace-stat-pill is-soft">{`${copy.syncStatus.push}: ${pushState}`}</span>
            </div>
          </section>

          {workspace.notice ? <div className="notice-banner">{workspace.notice}</div> : null}

          <div className={`${showMailPane || workspace.section !== "mail" ? "content-grid has-detail" : "content-grid"}${mobileDetailOpen ? " is-mobile-detail-open" : ""}`}>
            {workspace.section !== "settings" ? (
              <MasterPane
                copy={copy}
                section={workspace.section}
                folder={workspace.folder}
                folderLabel={workspaceTitle}
                contactBook={workspace.contactBook}
                setContactBook={workspace.setContactBook}
                contactBooks={workspace.contactBooks}
                calendarCollectionId={workspace.calendarCollectionId}
                setCalendarCollectionId={workspace.setCalendarCollectionId}
                calendarCollections={workspace.calendarCollections}
                mode={workspace.mode}
                filteredMessages={workspace.filtered}
                events={workspace.filteredEvents}
                contacts={workspace.filteredContacts}
                tasks={workspace.filteredTasks}
                notes={workspace.filteredNotes}
                journalEntries={workspace.filteredJournalEntries}
                reminders={workspace.filteredReminders}
                messageId={workspace.messageId}
                eventId={workspace.eventId}
                contactId={workspace.contactId}
                taskId={workspace.taskId}
                noteId={workspace.noteId}
                journalEntryId={workspace.journalEntryId}
                reminderId={workspace.reminderId}
                onSelectMessage={(id) => { workspace.setMessageId(id); setMobileDetailOpen(true); }}
                onSelectEvent={(id) => { workspace.setEventId(id); setMobileDetailOpen(true); }}
                onSelectContact={(id) => { workspace.setContactId(id); setMobileDetailOpen(true); }}
                onSelectTask={(id) => { workspace.setTaskId(id); setMobileDetailOpen(true); }}
                onSelectNote={(id) => { workspace.setNoteId(id); setMobileDetailOpen(true); }}
                onSelectJournalEntry={(id) => { workspace.setJournalEntryId(id); setMobileDetailOpen(true); }}
                onSelectReminder={(id) => { workspace.setReminderId(id); setMobileDetailOpen(true); }}
              />
            ) : null}

            {showMailPane ? (
              <section className="detail-pane">
                <Button className="mobile-detail-back" variant="ghost" size="sm" type="button" onClick={() => setMobileDetailOpen(false)}>{copy.navigation.backToList}</Button>
                <MailDetail
                  copy={copy}
                  current={workspace.current}
                  mode={workspace.mode}
                  draft={workspace.draft}
                  messageBusy={workspace.messageBusy}
                  notice={workspace.notice}
                  composerMailboxes={workspace.composerMailboxes}
                  setDraft={workspace.setDraft}
                  onReply={(message) => workspace.openComposer("reply", message)}
                  onForward={(message) => workspace.openComposer("forward", message)}
                  onToggleFlag={(message) => void workspace.toggleMessageFlag(message)}
                  onCompleteFlag={(message, completed) => void workspace.completeMessageFlag(message, completed)}
                  onSetFlagDue={(message, daysFromToday) => void workspace.setMessageFlagDue(message, daysFromToday)}
                  onSetFlagReminder={(message, minutesFromNow) => void workspace.setMessageFlagReminder(message, minutesFromNow)}
                  onCancel={workspace.closeComposer}
                  onSaveDraft={() => void workspace.saveMessage(true)}
                  onSend={() => void workspace.saveMessage(false)}
                  onDeleteDraft={() => void workspace.deleteDraft()}
                  draftMessageId={workspace.draftMessageId}
                  onUploadAttachment={workspace.uploadDraftAttachment}
                  onOpenAttachment={workspace.openAttachment}
                />
              </section>
            ) : null}

            {workspace.section === "calendar" ? (
            <section className="detail-pane">
              <Button className="mobile-detail-back" variant="ghost" size="sm" type="button" onClick={() => setMobileDetailOpen(false)}>{copy.navigation.backToList}</Button>
              <EventEditor
                copy={copy}
                currentEvent={workspace.currentEvent}
                eventForm={workspace.eventForm}
                setEventForm={workspace.setEventForm}
                resources={workspace.resources}
                onNew={workspace.resetEventForm}
                onSave={() => void workspace.saveEvent()}
                onDelete={() => void workspace.deleteEvent()}
              />
            </section>
            ) : null}

            {workspace.section === "contacts" ? (
            <section className="detail-pane">
              <Button className="mobile-detail-back" variant="ghost" size="sm" type="button" onClick={() => setMobileDetailOpen(false)}>{copy.navigation.backToList}</Button>
              <ContactEditor
                copy={copy}
                currentContact={workspace.currentContact}
                contactForm={workspace.contactForm}
                setContactForm={workspace.setContactForm}
                onNew={workspace.resetContactForm}
                onSave={() => void workspace.saveContact()}
                onDelete={() => void workspace.deleteContact()}
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

            {["tasks", "notes", "journal", "reminders"].includes(workspace.section) ? (
            <section className="detail-pane">
              <Button className="mobile-detail-back" variant="ghost" size="sm" type="button" onClick={() => setMobileDetailOpen(false)}>{copy.navigation.backToList}</Button>
              <CanonicalItemEditor
                copy={copy}
                section={workspace.section}
                taskLists={workspace.taskLists}
                currentTask={workspace.currentTask}
                taskForm={workspace.taskForm}
                setTaskForm={workspace.setTaskForm}
                currentNote={workspace.currentNote}
                noteForm={workspace.noteForm}
                setNoteForm={workspace.setNoteForm}
                currentJournalEntry={workspace.currentJournalEntry}
                journalEntryForm={workspace.journalEntryForm}
                setJournalEntryForm={workspace.setJournalEntryForm}
                currentReminder={workspace.currentReminder}
                onNewTask={workspace.resetTaskForm}
                onSaveTask={() => void workspace.saveTask()}
                onDeleteTask={() => void workspace.deleteTask()}
                onNewNote={workspace.resetNoteForm}
                onSaveNote={() => void workspace.saveNote()}
                onDeleteNote={() => void workspace.deleteNote()}
                onNewJournalEntry={workspace.resetJournalEntryForm}
                onSaveJournalEntry={() => void workspace.saveJournalEntry()}
                onDeleteJournalEntry={() => void workspace.deleteJournalEntry()}
              />
            </section>
            ) : null}
          </div>
        </section>
      </div>
    </main>
  );
}
