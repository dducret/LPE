import React from "react";
import type { ClientCopy } from "../i18n";
import type { Folder, Section } from "../client-types";

export function Sidebar(props: {
  copy: ClientCopy;
  section: Section;
  setSection: (section: Section) => void;
  folder: Folder;
  setFolder: (folder: Folder) => void;
  counts: Record<Folder, number>;
  unreadCount: number;
  eventCount: number;
  draftCount: number;
  mailboxOwner: string;
  onCompose: () => void;
  onCloseComposer: () => void;
  collapsed: boolean;
  mobileOpen: boolean;
  onToggleCollapse: () => void;
  onCloseMobile: () => void;
}) {
  const mailFolders: Array<{ id: Folder | null; label: string; count?: number }> = [
    { id: "inbox", label: props.copy.folders.inbox, count: props.counts.inbox },
    { id: "drafts", label: props.copy.folders.drafts, count: props.counts.drafts },
    { id: "sent", label: props.copy.folders.sent, count: props.counts.sent },
    { id: "archive", label: props.copy.folders.archive, count: props.counts.archive },
    { id: "trash", label: props.copy.folders.trash, count: props.counts.trash },
    { id: "junk", label: props.copy.folders.junk, count: props.counts.junk },
    { id: "outbox", label: props.copy.folders.outbox, count: props.counts.outbox },
    { id: "rss_feeds", label: props.copy.folders.rss_feeds, count: props.counts.rss_feeds },
    { id: "conversation_history", label: props.copy.folders.conversation_history, count: props.counts.conversation_history },
    { id: "sync_issues", label: props.copy.folders.sync_issues, count: props.counts.sync_issues },
    { id: "conflicts", label: props.copy.folders.conflicts, count: props.counts.conflicts },
    { id: "local_failures", label: props.copy.folders.local_failures, count: props.counts.local_failures },
    { id: "server_failures", label: props.copy.folders.server_failures, count: props.counts.server_failures }
  ];
  const sectionLinks: Array<{ id: Section; label: string }> = [
    { id: "mail", label: props.copy.sections.mail },
    { id: "calendar", label: props.copy.sections.calendar },
    { id: "contacts", label: props.copy.sections.contacts },
    { id: "tasks", label: props.copy.sections.tasks },
    { id: "notes", label: props.copy.sections.notes },
    { id: "journal", label: props.copy.sections.journal },
    { id: "reminders", label: props.copy.sections.reminders },
    { id: "settings", label: props.copy.sections.settings }
  ];

  return (
    <aside className={props.collapsed ? props.mobileOpen ? "rail is-collapsed is-mobile-open" : "rail is-collapsed" : props.mobileOpen ? "rail is-mobile-open" : "rail"}>
      <div className="sidebar-column">
        <div className="sidebar-toolbar">
          <div className="brand-lockup">
            <div className="brand-mark">LPE</div>
            <div className="brand-copy">
              <h1>{props.copy.productTitle}</h1>
              <p className="brand-subtitle">{props.copy.productSubtitle}</p>
            </div>
          </div>
          <button className="ghost-button collapse-toggle" type="button" aria-label={props.collapsed ? props.copy.compose : props.copy.rightPaneTitle} title={props.collapsed ? props.copy.compose : props.copy.rightPaneTitle} onClick={props.onToggleCollapse}>
            {props.collapsed ? "→" : "←"}
          </button>
        </div>

        <button className={props.collapsed ? "compose-button is-collapsed" : "compose-button"} type="button" title={props.copy.compose} aria-label={props.copy.compose} onClick={() => { props.onCompose(); props.onCloseMobile(); }}>
          <span className="compose-plus">+</span>
          <span className="sidebar-label">{props.copy.compose}</span>
        </button>

        <div className="sidebar-group">
          <p className="panel-title">{props.copy.sectionLabel}</p>
          <nav className="sidebar-section-nav" aria-label={props.copy.sectionLabel}>
            {sectionLinks.map((item) => (
              <button
                key={item.id}
                className={props.section === item.id ? "section-item is-active" : "section-item"}
                type="button"
                title={item.label}
                aria-label={item.label}
                onClick={() => {
                  props.setSection(item.id);
                  props.onCloseComposer();
                  props.onCloseMobile();
                }}
              >
                <span className="section-item-icon">{props.copy.sectionIcons[item.id]}</span>
                <span className="sidebar-label">{item.label}</span>
              </button>
            ))}
          </nav>
        </div>

        <div className="folder-panel is-tight">
          <p className="panel-title">{props.copy.favoritesLabel}</p>
          <button className="tree-item" type="button" title={props.copy.folders.focused} onClick={() => { props.setSection("mail"); props.setFolder("focused"); props.onCloseComposer(); props.onCloseMobile(); }}>
            <span className="tree-item-icon">•</span>
            <span className="sidebar-label">{props.copy.folders.focused}</span>
          </button>
          <button className="tree-item" type="button" title={props.copy.folders.inbox} onClick={() => { props.setSection("mail"); props.setFolder("inbox"); props.onCloseComposer(); props.onCloseMobile(); }}>
            <span className="tree-item-icon">•</span>
            <span className="sidebar-label">{props.copy.folders.inbox}</span>
            <span className="sidebar-meta">{props.counts.inbox}</span>
          </button>
        </div>

        <div className="mailbox-header">
          <strong className="sidebar-label">{props.mailboxOwner}</strong>
        </div>

        <div className="folder-panel is-tree">
          {mailFolders.map((item, index) => {
            const isActive = item.id ? props.folder === item.id : false;
            return (
              <button
                key={`${item.label}-${index}`}
                className={isActive ? "tree-item is-active" : "tree-item"}
                type="button"
                title={item.label}
                aria-label={item.label}
                onClick={() => {
                  if (item.id) {
                    props.setSection("mail");
                    props.setFolder(item.id);
                    props.onCloseComposer();
                    props.onCloseMobile();
                  }
                }}
              >
                <span className="tree-item-icon">{item.id === "inbox" ? "•" : item.id === "drafts" ? "◦" : item.id === "sent" ? "↗" : item.id === "trash" ? "⌫" : item.id === "junk" ? "!" : item.id === "outbox" ? "↥" : "▤"}</span>
                <span className="sidebar-label">{item.label}</span>
                <span className="sidebar-meta">{item.count ?? ""}</span>
              </button>
            );
          })}
        </div>

        <div className="rail-summary">
          <p className="panel-title">{props.copy.workspaceSummary}</p>
          <div className="summary-card"><strong className="sidebar-label">{props.copy.summaryInbox}</strong><span className="sidebar-label">{props.copy.summaryUnread.replace("{count}", String(props.unreadCount))}</span></div>
          <div className="summary-card"><strong className="sidebar-label">{props.copy.summaryAgenda}</strong><span className="sidebar-label">{props.copy.summaryAgendaCount.replace("{count}", String(props.eventCount))}</span></div>
          <div className="summary-card"><strong className="sidebar-label">{props.copy.summaryDrafts}</strong><span className="sidebar-label">{props.copy.summaryDraftsCount.replace("{count}", String(props.draftCount))}</span></div>
        </div>

        <button className="ghost-button sidebar-mobile-close" type="button" onClick={props.onCloseMobile}>{props.copy.editorActions.cancel}</button>
      </div>
    </aside>
  );
}
