import React from "react";
import type { ClientCopy } from "../i18n";
import type { ClientMailbox, Folder, Section, SystemFolder } from "../client-types";
import { ClientIcon, type ClientIconName } from "./ClientIcon";

const sectionIcons: Record<Section, ClientIconName> = {
  mail: "mail",
  calendar: "calendar",
  contacts: "contacts",
  tasks: "tasks",
  notes: "notes",
  journal: "journal",
  reminders: "reminders",
  settings: "settings"
};

function folderIcon(folder: Folder | null): ClientIconName {
  if (folder === "inbox" || folder === "focused") return "inbox";
  if (folder === "drafts") return "draft";
  if (folder === "sent" || folder === "outbox") return "sent";
  if (folder === "archive") return "archive";
  if (folder === "trash") return "trash";
  return "folder";
}

export function Sidebar(props: {
  copy: ClientCopy;
  section: Section;
  setSection: (section: Section) => void;
  folder: Folder;
  setFolder: (folder: Folder) => void;
  counts: Record<SystemFolder, number>;
  customMailboxes: ClientMailbox[];
  unreadCount: number;
  eventCount: number;
  draftCount: number;
  mailboxOwner: string;
  onCompose: () => void;
  onCloseComposer: () => void;
  collapsed: boolean;
  mobileOpen: boolean;
  isNarrowScreen: boolean;
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
  const customMailboxes = props.customMailboxes.filter((mailbox) => mailbox.isSubscribed && mailbox.role === "custom");
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

  const selectSection = (section: Section) => {
    props.setSection(section);
    props.onCloseComposer();
    props.onCloseMobile();
  };
  const selectFolder = (folder: Folder) => {
    props.setSection("mail");
    props.setFolder(folder);
    props.onCloseComposer();
    props.onCloseMobile();
  };

  return (
    <aside
      id="client-sidebar"
      className={props.collapsed ? props.mobileOpen ? "rail is-collapsed is-mobile-open" : "rail is-collapsed" : props.mobileOpen ? "rail is-mobile-open" : "rail"}
      aria-hidden={props.isNarrowScreen && !props.mobileOpen ? true : undefined}
      inert={props.isNarrowScreen && !props.mobileOpen}
    >
      <nav className="app-rail" aria-label={props.copy.sectionLabel}>
        <div className="app-rail-brand" aria-label="LPE">L</div>
        <div className="app-rail-links">
          {sectionLinks.map((item) => (
            <button
              key={item.id}
              className={props.section === item.id ? "app-rail-button is-active" : "app-rail-button"}
              type="button"
              title={item.label}
              aria-label={item.label}
              onClick={() => selectSection(item.id)}
            >
              <ClientIcon name={sectionIcons[item.id]} />
            </button>
          ))}
        </div>
      </nav>

      <div className="folder-rail">
        <div className="sidebar-toolbar">
          <div className="brand-lockup">
            <div className="brand-copy">
              <h1>{props.copy.sections[props.section]}</h1>
              <p className="brand-subtitle">{props.mailboxOwner}</p>
            </div>
          </div>
          <button className="ghost-button collapse-toggle" type="button" aria-label={props.collapsed ? props.copy.navigation.expand : props.copy.navigation.collapse} title={props.collapsed ? props.copy.navigation.expand : props.copy.navigation.collapse} onClick={props.onToggleCollapse}>
            <span className="collapse-chevron" aria-hidden="true" />
          </button>
        </div>

        <button className="compose-button" type="button" title={props.copy.compose} aria-label={props.copy.compose} onClick={() => { props.onCompose(); props.onCloseMobile(); }}>
          <ClientIcon name="compose" />
          <span className="sidebar-label">{props.copy.compose}</span>
        </button>

        <div className="folder-panel is-tight">
          <p className="panel-title">{props.copy.favoritesLabel}</p>
          <button className={props.folder === "focused" ? "tree-item is-active" : "tree-item"} type="button" title={props.copy.folders.focused} aria-label={props.copy.folders.focused} onClick={() => selectFolder("focused")}>
            <ClientIcon className="tree-item-icon" name="inbox" />
            <span className="sidebar-label">{props.copy.folders.focused}</span>
          </button>
          <button className={props.folder === "inbox" ? "tree-item is-active" : "tree-item"} type="button" title={props.copy.folders.inbox} aria-label={props.copy.folders.inbox} onClick={() => selectFolder("inbox")}>
            <ClientIcon className="tree-item-icon" name="inbox" />
            <span className="sidebar-label">{props.copy.folders.inbox}</span>
            <span className="sidebar-meta">{props.counts.inbox}</span>
          </button>
        </div>

        <div className="mailbox-header"><strong className="sidebar-label">{props.mailboxOwner}</strong></div>

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
                onClick={() => item.id && selectFolder(item.id)}
              >
                <ClientIcon className="tree-item-icon" name={folderIcon(item.id)} />
                <span className="sidebar-label">{item.label}</span>
                <span className="sidebar-meta">{item.count ?? ""}</span>
              </button>
            );
          })}
          {customMailboxes.map((mailbox) => {
            const id = `mailbox:${mailbox.id}` as Folder;
            return (
              <button key={id} className={props.folder === id ? "tree-item is-active" : "tree-item"} type="button" title={mailbox.name} aria-label={mailbox.name} onClick={() => selectFolder(id)}>
                <ClientIcon className="tree-item-icon" name="folder" />
                <span className="sidebar-label">{mailbox.name}</span>
                <span className="sidebar-meta">{mailbox.totalEmails}</span>
              </button>
            );
          })}
        </div>

        <button className="ghost-button sidebar-mobile-close" type="button" onClick={props.onCloseMobile}>{props.copy.editorActions.cancel}</button>
      </div>
    </aside>
  );
}
