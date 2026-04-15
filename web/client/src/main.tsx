import React from "react";
import ReactDOM from "react-dom/client";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import "./styles.css";

type AppSection = "mail" | "calendar" | "contacts";
type FolderKey = "focused" | "inbox" | "drafts" | "sent" | "archive";
type Attachment = {
  id: string;
  name: string;
  kind: "PDF" | "DOCX" | "ODT";
  size: string;
};
type MailMessage = {
  id: string;
  folder: FolderKey;
  from: string;
  fromAddress: string;
  subject: string;
  preview: string;
  receivedAt: string;
  timeLabel: string;
  unread: boolean;
  flagged: boolean;
  category: "priority" | "customer" | "internal";
  tags: string[];
  attachments: Attachment[];
  body: string[];
};
type CalendarItem = {
  id: string;
  time: string;
  title: string;
  location: string;
  attendees: string;
};
type ContactItem = {
  id: string;
  name: string;
  role: string;
  email: string;
  phone: string;
  team: string;
};

const mailMessages: MailMessage[] = [
  {
    id: "msg-1",
    folder: "focused",
    from: "Marta Vogel",
    fromAddress: "marta.vogel@northwind.example",
    subject: "Contract review before domain migration",
    preview: "I added the revised delivery clauses and the PDF for legal review before tonight's cutover window.",
    receivedAt: "08:42",
    timeLabel: "08:42",
    unread: true,
    flagged: true,
    category: "priority",
    tags: ["Legal", "Migration"],
    attachments: [{ id: "att-1", name: "northwind-domain-cutover.pdf", kind: "PDF", size: "2.4 MB" }],
    body: [
      "Hello team,",
      "I have attached the revised contract package for the Northwind domain migration. Please confirm the wording around delegated administration and retention before we freeze the window.",
      "If everything looks correct, I will approve the rollout for tonight and notify support.",
      "Marta"
    ]
  },
  {
    id: "msg-2",
    folder: "focused",
    from: "Support Queue",
    fromAddress: "support@lpe.example",
    subject: "Three users reported delayed sync on mobile",
    preview: "The incidents all point to the same JMAP session refresh path after mailbox rule changes.",
    receivedAt: "07:18",
    timeLabel: "07:18",
    unread: true,
    flagged: false,
    category: "customer",
    tags: ["Support", "JMAP"],
    attachments: [],
    body: [
      "Morning,",
      "We have three fresh reports from the Lyon office. Mail eventually arrives, but the mobile client takes around ninety seconds to refresh after a server-side rule update.",
      "The desktop web client stays consistent, so this likely sits on the session or push path rather than the search index.",
      "Support queue"
    ]
  },
  {
    id: "msg-3",
    folder: "inbox",
    from: "Elena Rossi",
    fromAddress: "elena.rossi@lpe.example",
    subject: "Attachment extraction metrics for this week",
    preview: "PDF, DOCX and ODT extraction stayed within budget; the weekly summary is attached as a DOCX memo.",
    receivedAt: "Yesterday",
    timeLabel: "Yesterday",
    unread: false,
    flagged: false,
    category: "internal",
    tags: ["Search", "Indexing"],
    attachments: [{ id: "att-2", name: "attachment-extraction-weekly.docx", kind: "DOCX", size: "418 KB" }],
    body: [
      "Hi,",
      "The extraction pipeline stayed stable across the supported v1 formats. PDF remained the largest share, with DOCX following and ODT staying marginal.",
      "I attached the memo we can reuse for the product status meeting.",
      "Elena"
    ]
  },
  {
    id: "msg-4",
    folder: "sent",
    from: "You",
    fromAddress: "alex.meyer@lpe.example",
    subject: "Re: offline-only local AI positioning",
    preview: "Confirmed that the UI language should describe local assistance without implying that inference leaves the server.",
    receivedAt: "Yesterday",
    timeLabel: "Yesterday",
    unread: false,
    flagged: true,
    category: "internal",
    tags: ["AI", "Docs"],
    attachments: [],
    body: [
      "Hi all,",
      "I confirmed the wording for the client copy: local assistance remains optional, the primary path stays PostgreSQL search, and no user data leaves the server.",
      "We should keep that framing visible in both settings and onboarding.",
      "Alex"
    ]
  },
  {
    id: "msg-5",
    folder: "drafts",
    from: "You",
    fromAddress: "alex.meyer@lpe.example",
    subject: "Draft: migration wave briefing",
    preview: "Preparing the executive summary for the next domain onboarding wave with risk, owners and schedule.",
    receivedAt: "Mon",
    timeLabel: "Mon",
    unread: false,
    flagged: false,
    category: "priority",
    tags: ["Draft"],
    attachments: [{ id: "att-3", name: "wave-briefing.odt", kind: "ODT", size: "280 KB" }],
    body: [
      "Draft notes:",
      "Outline the onboarding wave, responsible domain admins, mailbox transfer checkpoints and support escalation contacts.",
      "Need final numbers from finance before sending."
    ]
  },
  {
    id: "msg-6",
    folder: "archive",
    from: "Security Desk",
    fromAddress: "security@lpe.example",
    subject: "Audit trace closed for customer escalation #984",
    preview: "The trace is complete and the quarantine event has been correlated with the original SMTP submission.",
    receivedAt: "Fri",
    timeLabel: "Fri",
    unread: false,
    flagged: false,
    category: "customer",
    tags: ["Audit", "Closed"],
    attachments: [],
    body: [
      "Trace complete.",
      "The message path now includes the original submission, quarantine review and final mailbox delivery. No data-loss signal was found.",
      "Security desk"
    ]
  }
];

const calendarItems: CalendarItem[] = [
  { id: "cal-1", time: "09:30", title: "Migration stand-up", location: "Room Atlas", attendees: "Ops, Support, Product" },
  { id: "cal-2", time: "12:00", title: "Mailbox search review", location: "Video", attendees: "Search, Storage" },
  { id: "cal-3", time: "16:30", title: "Domain admin onboarding", location: "Room Rhine", attendees: "Customer Success" }
];

const contactItems: ContactItem[] = [
  { id: "ct-1", name: "Marta Vogel", role: "Customer migration lead", email: "marta.vogel@northwind.example", phone: "+49 30 555 0142", team: "Northwind" },
  { id: "ct-2", name: "Elena Rossi", role: "Search engineer", email: "elena.rossi@lpe.example", phone: "+39 02 555 2031", team: "Platform" },
  { id: "ct-3", name: "Jonas Keller", role: "Domain administrator", email: "jonas.keller@contoso.example", phone: "+41 44 555 9912", team: "Contoso" }
];

function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const [activeSection, setActiveSection] = React.useState<AppSection>("mail");
  const [activeFolder, setActiveFolder] = React.useState<FolderKey>("focused");
  const [query, setQuery] = React.useState("");
  const [selectedMessageId, setSelectedMessageId] = React.useState<string>(mailMessages[0]?.id ?? "");
  const copy = messages[locale];

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

  const folderCounts = React.useMemo(
    () =>
      mailMessages.reduce<Record<FolderKey, number>>(
        (counts, item) => {
          counts[item.folder] += 1;
          return counts;
        },
        { focused: 0, inbox: 0, drafts: 0, sent: 0, archive: 0 }
      ),
    []
  );

  const filteredMessages = React.useMemo(() => {
    const lowerQuery = query.trim().toLowerCase();
    return mailMessages.filter((item) => {
      if (item.folder !== activeFolder && !(activeFolder === "focused" && item.folder === "focused")) {
        return false;
      }

      if (!lowerQuery) {
        return true;
      }

      return [item.from, item.fromAddress, item.subject, item.preview, item.tags.join(" "), item.attachments.map((attachment) => attachment.name).join(" ")]
        .join(" ")
        .toLowerCase()
        .includes(lowerQuery);
    });
  }, [activeFolder, query]);

  React.useEffect(() => {
    const nextMessage = filteredMessages[0];
    if (!nextMessage) {
      setSelectedMessageId("");
      return;
    }

    const isCurrentVisible = filteredMessages.some((message) => message.id === selectedMessageId);
    if (!isCurrentVisible) {
      setSelectedMessageId(nextMessage.id);
    }
  }, [filteredMessages, selectedMessageId]);

  const selectedMessage = filteredMessages.find((message) => message.id === selectedMessageId) ?? filteredMessages[0] ?? null;
  const unreadCount = mailMessages.filter((message) => message.unread).length;
  const todayAgenda = calendarItems.length;
  const flaggedCount = mailMessages.filter((message) => message.flagged).length;

  return (
    <main className="app-shell">
      <aside className="rail">
        <div className="brand-lockup">
          <div className="brand-mark">LPE</div>
          <div>
            <p className="eyebrow">{copy.productLabel}</p>
            <h1>{copy.productTitle}</h1>
          </div>
        </div>

        <button className="compose-button" type="button">
          <span className="compose-plus">+</span>
          <span>{copy.compose}</span>
        </button>

        <nav className="section-nav" aria-label={copy.sectionLabel}>
          {(["mail", "calendar", "contacts"] as AppSection[]).map((section) => (
            <button key={section} className={activeSection === section ? "nav-button is-active" : "nav-button"} type="button" onClick={() => setActiveSection(section)}>
              <span className="nav-icon">{copy.sectionIcons[section]}</span>
              <span>{copy.sections[section]}</span>
            </button>
          ))}
        </nav>

        <div className="folder-panel">
          <p className="panel-title">{copy.mailboxLabel}</p>
          {(["focused", "inbox", "drafts", "sent", "archive"] as FolderKey[]).map((folder) => (
            <button key={folder} className={activeFolder === folder ? "folder-button is-active" : "folder-button"} type="button" onClick={() => { setActiveSection("mail"); setActiveFolder(folder); }}>
              <span>{copy.folders[folder]}</span>
              <span>{folderCounts[folder]}</span>
            </button>
          ))}
        </div>

        <div className="rail-summary">
          <p className="panel-title">{copy.workspaceSummary}</p>
          <div className="summary-card"><strong>{copy.summaryInbox}</strong><span>{copy.summaryUnread.replace("{count}", String(unreadCount))}</span></div>
          <div className="summary-card"><strong>{copy.summaryAgenda}</strong><span>{copy.summaryAgendaCount.replace("{count}", String(todayAgenda))}</span></div>
          <div className="summary-card"><strong>{copy.summaryFlagged}</strong><span>{copy.summaryFlaggedCount.replace("{count}", String(flaggedCount))}</span></div>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div className="search-shell">
            <span className="search-icon">/</span>
            <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder={copy.searchPlaceholder} aria-label={copy.searchPlaceholder} />
          </div>

          <div className="topbar-actions">
            <button className="ghost-button" type="button">{copy.topActions.sync}</button>
            <button className="ghost-button" type="button">{copy.topActions.rules}</button>
            <label className="locale-picker">
              <span>{copy.languageLabel}</span>
              <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
                {supportedLocales.map((entry) => <option key={entry} value={entry}>{localeLabels[entry]}</option>)}
              </select>
            </label>
          </div>
        </header>

        <section className="hero">
          <div>
            <p className="eyebrow">{copy.heroEyebrow}</p>
            <h2>{copy.heroTitle}</h2>
            <p>{copy.heroBody}</p>
          </div>
          <div className="hero-metrics">
            <div><span>{copy.metrics.reliability}</span><strong>99.97%</strong></div>
            <div><span>{copy.metrics.search}</span><strong>{copy.metrics.searchValue}</strong></div>
            <div><span>{copy.metrics.attachments}</span><strong>PDF / DOCX / ODT</strong></div>
          </div>
        </section>

        <div className="content-grid">
          <section className="list-pane">
            <div className="pane-header">
              <div>
                <p className="pane-kicker">{copy.sections[activeSection]}</p>
                <h3>{activeSection === "mail" ? copy.folders[activeFolder] : copy.altViews[activeSection]}</h3>
              </div>
              <span className="pane-count">{activeSection === "mail" ? copy.messageCount.replace("{count}", String(filteredMessages.length)) : activeSection === "calendar" ? copy.calendarCount.replace("{count}", String(calendarItems.length)) : copy.contactCount.replace("{count}", String(contactItems.length))}</span>
            </div>

            {activeSection === "mail" ? <div className="message-list">{filteredMessages.map((message) => (
              <button key={message.id} className={selectedMessage?.id === message.id ? "message-row is-active" : "message-row"} type="button" onClick={() => setSelectedMessageId(message.id)}>
                <div className="message-row-top"><strong>{message.from}</strong><span>{message.timeLabel}</span></div>
                <div className="message-row-main"><span className={message.unread ? "subject unread" : "subject"}>{message.subject}</span>{message.flagged ? <span className="flag-pill">{copy.flaggedShort}</span> : null}</div>
                <p>{message.preview}</p>
                <div className="row-meta"><span className={`category-pill is-${message.category}`}>{copy.categories[message.category]}</span>{message.attachments.length > 0 ? <span>{copy.attachmentCount.replace("{count}", String(message.attachments.length))}</span> : null}</div>
              </button>
            ))}{filteredMessages.length === 0 ? <div className="empty-state">{copy.noMessages}</div> : null}</div> : null}

            {activeSection === "calendar" ? <div className="agenda-list">{calendarItems.map((item) => <article className="agenda-card" key={item.id}><span className="agenda-time">{item.time}</span><div><strong>{item.title}</strong><p>{item.location}</p><span>{item.attendees}</span></div></article>)}</div> : null}

            {activeSection === "contacts" ? <div className="contact-list">{contactItems.map((contact) => <article className="contact-card" key={contact.id}><div className="contact-avatar">{contact.name.slice(0, 2).toUpperCase()}</div><div><strong>{contact.name}</strong><p>{contact.role}</p><span>{contact.team}</span></div></article>)}</div> : null}
          </section>

          <section className="detail-pane">
            {activeSection === "mail" && selectedMessage ? <>
              <div className="detail-header">
                <div><p className="detail-label">{copy.readingPane}</p><h3>{selectedMessage.subject}</h3></div>
                <div className="detail-actions">
                  <button className="ghost-button" type="button">{copy.messageActions.reply}</button>
                  <button className="ghost-button" type="button">{copy.messageActions.forward}</button>
                  <button className="ghost-button" type="button">{copy.messageActions.archive}</button>
                </div>
              </div>

              <div className="sender-card"><div className="sender-avatar">{selectedMessage.from.slice(0, 2).toUpperCase()}</div><div><strong>{selectedMessage.from}</strong><p>{selectedMessage.fromAddress}</p></div><span>{selectedMessage.receivedAt}</span></div>
              <div className="tag-row">{selectedMessage.tags.map((tag) => <span className="tag-pill" key={tag}>{tag}</span>)}</div>
              <article className="message-body">{selectedMessage.body.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}</article>

              <section className="attachment-panel">
                <div className="pane-header compact"><div><p className="pane-kicker">{copy.attachmentsTitle}</p><h4>{copy.attachmentsSubtitle}</h4></div></div>
                <div className="attachment-list">
                  {selectedMessage.attachments.length > 0 ? selectedMessage.attachments.map((attachment) => <article className="attachment-card" key={attachment.id}><span className="attachment-kind">{attachment.kind}</span><div><strong>{attachment.name}</strong><p>{attachment.size}</p></div></article>) : <div className="empty-state compact">{copy.noAttachments}</div>}
                </div>
              </section>
            </> : null}

            {activeSection === "calendar" ? <section className="alt-detail"><p className="detail-label">{copy.altDetailLabels.calendar}</p><h3>{copy.calendarHeadline}</h3><p>{copy.calendarBody}</p><div className="checklist">{copy.calendarPoints.map((item) => <div className="check-item" key={item}><span className="check-mark">+</span><span>{item}</span></div>)}</div></section> : null}

            {activeSection === "contacts" ? <section className="alt-detail"><p className="detail-label">{copy.altDetailLabels.contacts}</p><h3>{copy.contactsHeadline}</h3><p>{copy.contactsBody}</p><div className="contact-detail-list">{contactItems.map((contact) => <div className="contact-detail-row" key={contact.id}><strong>{contact.name}</strong><span>{contact.email}</span><span>{contact.phone}</span></div>)}</div></section> : null}
          </section>
        </div>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(<React.StrictMode><App /></React.StrictMode>);

