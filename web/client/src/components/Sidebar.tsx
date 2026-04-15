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
  onCompose: () => void;
  onCloseComposer: () => void;
}) {
  return (
    <aside className="rail">
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

      <nav className="section-nav" aria-label={props.copy.sectionLabel}>
        {(["mail", "calendar", "contacts"] as Section[]).map((value) => (
          <button key={value} className={props.section === value ? "nav-button is-active" : "nav-button"} type="button" onClick={() => props.setSection(value)}>
            <span className="nav-icon">{props.copy.sectionIcons[value]}</span>
            <span>{props.copy.sections[value]}</span>
          </button>
        ))}
      </nav>

      <div className="folder-panel">
        <p className="panel-title">{props.copy.mailboxLabel}</p>
        {(["focused", "inbox", "drafts", "sent", "archive"] as Folder[]).map((value) => (
          <button
            key={value}
            className={props.folder === value ? "folder-button is-active" : "folder-button"}
            type="button"
            onClick={() => {
              props.setSection("mail");
              props.setFolder(value);
              props.onCloseComposer();
            }}
          >
            <span>{props.copy.folders[value]}</span>
            <span>{props.counts[value]}</span>
          </button>
        ))}
      </div>

      <div className="rail-summary">
        <p className="panel-title">{props.copy.workspaceSummary}</p>
        <div className="summary-card"><strong>{props.copy.summaryInbox}</strong><span>{props.copy.summaryUnread.replace("{count}", String(props.unreadCount))}</span></div>
        <div className="summary-card"><strong>{props.copy.summaryAgenda}</strong><span>{props.copy.summaryAgendaCount.replace("{count}", String(props.eventCount))}</span></div>
        <div className="summary-card"><strong>{props.copy.summaryDrafts}</strong><span>{props.copy.summaryDraftsCount.replace("{count}", String(props.draftCount))}</span></div>
      </div>
    </aside>
  );
}
