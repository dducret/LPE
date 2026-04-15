import React from "react";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import { Sidebar } from "./components/Sidebar";
import { MasterPane } from "./components/MasterPane";
import { MailDetail } from "./components/MailDetail";
import { EventEditor } from "./components/EventEditor";
import { ContactEditor } from "./components/ContactEditor";
import { useClientWorkspace } from "./useClientWorkspace";
import "./styles.css";

export function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const copy = messages[locale];
  const workspace = useClientWorkspace(copy);

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

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
          <span className="header-icon">◫</span>
          <span className="header-icon">✉</span>
          <span className="header-icon">⚙</span>
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
          onCompose={() => workspace.openComposer("new")}
          onCloseComposer={workspace.closeComposer}
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
            {copy.ribbonActions.map((action) => <button key={action} className="ribbon-button" type="button">{action}</button>)}
            <div className="ribbon-separator" />
            {copy.ribbonSecondary.map((action) => <button key={action} className="ribbon-button" type="button">{action}</button>)}
          </div>

          <header className="workspace-meta">
            <div className="workspace-meta-left">
              <span className="status-pill">{copy.sections[workspace.section]}</span>
              <span className="status-pill is-muted">{workspace.filtered.length} visible</span>
              <span className="workspace-caption">{copy.productSubtitle}</span>
            </div>
            <div className="workspace-meta-right">
            <button className="ghost-button" type="button">{copy.topActions.sync}</button>
            <button className="ghost-button" type="button">{copy.topActions.rules}</button>
            <button className="ghost-button" type="button">{copy.topActions.schedule}</button>
            <label className="locale-picker">
              <span>{copy.languageLabel}</span>
              <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
                {supportedLocales.map((value) => <option key={value} value={value}>{localeLabels[value]}</option>)}
              </select>
            </label>
            </div>
          </header>

          {workspace.notice ? <div className="notice-banner">{workspace.notice}</div> : null}

          <div className="content-grid">
            <MasterPane
              copy={copy}
              section={workspace.section}
              folder={workspace.folder}
              mode={workspace.mode}
              filteredMessages={workspace.filtered}
              events={workspace.events}
              contacts={workspace.contacts}
              messageId={workspace.messageId}
              eventId={workspace.eventId}
              contactId={workspace.contactId}
              onSelectMessage={workspace.setMessageId}
              onSelectEvent={workspace.setEventId}
              onSelectContact={workspace.setContactId}
              onCloseComposer={workspace.closeComposer}
            />

            <section className="detail-pane">
              {workspace.section === "mail" ? (
                <MailDetail
                  copy={copy}
                  current={workspace.current}
                  mode={workspace.mode}
                  draft={workspace.draft}
                  setDraft={workspace.setDraft}
                  onReply={(message) => workspace.openComposer("reply", message)}
                  onForward={(message) => workspace.openComposer("forward", message)}
                  onCancel={workspace.closeComposer}
                  onSaveDraft={() => workspace.saveMessage(true)}
                  onSend={() => workspace.saveMessage(false)}
                />
              ) : null}

              {workspace.section === "calendar" ? (
                <EventEditor
                  copy={copy}
                  currentEvent={workspace.currentEvent}
                  eventForm={workspace.eventForm}
                  setEventForm={workspace.setEventForm}
                  onNew={workspace.resetEventForm}
                  onSave={workspace.saveEvent}
                />
              ) : null}

              {workspace.section === "contacts" ? (
                <ContactEditor
                  copy={copy}
                  currentContact={workspace.currentContact}
                  contactForm={workspace.contactForm}
                  setContactForm={workspace.setContactForm}
                  onNew={workspace.resetContactForm}
                  onSave={workspace.saveContact}
                />
              ) : null}
            </section>
          </div>
        </section>
      </div>
    </main>
  );
}
