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
}) {
  const mailFolders: Array<{ id: Folder | null; label: string; count?: number }> = [
    { id: "inbox", label: props.copy.folders.inbox, count: props.counts.inbox },
    { id: "drafts", label: props.copy.folders.drafts, count: props.counts.drafts },
    { id: "sent", label: props.copy.folders.sent, count: props.counts.sent },
    { id: "archive", label: props.copy.folders.archive, count: props.counts.archive }
  ];

  return (
    <aside className="rail">
      <div className="app-rail">
        <div className="app-rail-brand">☰</div>
        {(["mail", "calendar", "contacts"] as Section[]).map((value) => (
          <button key={value} className={props.section === value ? "app-rail-button is-active" : "app-rail-button"} type="button" onClick={() => props.setSection(value)}>
            {props.copy.sectionIcons[value]}
          </button>
        ))}
        <button className="app-rail-button" type="button">✓</button>
        <button className="app-rail-button" type="button">☁</button>
      </div>

      <div className="sidebar-column">
        <div className="brand-lockup">
          <div className="brand-mark">LPE</div>
          <div>
            <h1>{props.copy.productTitle}</h1>
            <p className="brand-subtitle">{props.copy.productSubtitle}</p>
          </div>
        </div>

        <button className="compose-button" type="button" onClick={props.onCompose}>
          <span className="compose-plus">+</span>
          <span>{props.copy.compose}</span>
        </button>

        <div className="folder-panel is-tight">
          <p className="panel-title">{props.copy.favoritesLabel}</p>
          <button className="tree-item" type="button" onClick={() => { props.setSection("mail"); props.setFolder("focused"); props.onCloseComposer(); }}>
            <span>{props.copy.folders.focused}</span>
          </button>
          <button className="tree-item" type="button" onClick={() => { props.setSection("mail"); props.setFolder("inbox"); props.onCloseComposer(); }}>
            <span>{props.copy.folders.inbox}</span>
            <span>{props.counts.inbox}</span>
          </button>
        </div>

        <div className="mailbox-header">
          <strong>{props.mailboxOwner}</strong>
        </div>

        <div className="folder-panel is-tree">
          {mailFolders.map((item, index) => {
            const isActive = item.id ? props.folder === item.id : false;
            return (
              <button
                key={`${item.label}-${index}`}
                className={isActive ? "tree-item is-active" : "tree-item"}
                type="button"
                onClick={() => {
                  if (item.id) {
                    props.setSection("mail");
                    props.setFolder(item.id);
                    props.onCloseComposer();
                  }
                }}
              >
                <span>{item.label}</span>
                <span>{item.count ?? ""}</span>
              </button>
            );
          })}
        </div>

        <div className="rail-summary">
          <p className="panel-title">{props.copy.workspaceSummary}</p>
          <div className="summary-card"><strong>{props.copy.summaryInbox}</strong><span>{props.copy.summaryUnread.replace("{count}", String(props.unreadCount))}</span></div>
          <div className="summary-card"><strong>{props.copy.summaryAgenda}</strong><span>{props.copy.summaryAgendaCount.replace("{count}", String(props.eventCount))}</span></div>
          <div className="summary-card"><strong>{props.copy.summaryDrafts}</strong><span>{props.copy.summaryDraftsCount.replace("{count}", String(props.draftCount))}</span></div>
        </div>
      </div>
    </aside>
  );
}
