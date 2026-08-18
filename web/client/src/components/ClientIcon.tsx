import React from "react";

export type ClientIconName =
  | "archive"
  | "calendar"
  | "compose"
  | "contacts"
  | "draft"
  | "folder"
  | "inbox"
  | "journal"
  | "mail"
  | "notes"
  | "reminders"
  | "sent"
  | "settings"
  | "tasks"
  | "trash";

const iconPaths: Record<ClientIconName, React.ReactNode> = {
  archive: <><rect x="3" y="5" width="18" height="4" rx="1" /><path d="M5 9v10h14V9M9 13h6" /></>,
  calendar: <><rect x="3" y="5" width="18" height="16" rx="2" /><path d="M7 3v4M17 3v4M3 10h18M7 14h3M14 14h3M7 18h3" /></>,
  compose: <><path d="M4 20h4L19 9a2.1 2.1 0 0 0-3-3L5 17v3Z" /><path d="m14 8 3 3" /></>,
  contacts: <><circle cx="12" cy="8" r="3.2" /><path d="M5 21v-1.3A5.7 5.7 0 0 1 10.7 14h2.6a5.7 5.7 0 0 1 5.7 5.7V21" /><path d="M4 11.5h2M18 11.5h2" /></>,
  draft: <><path d="M5 3h10l4 4v14H5z" /><path d="M15 3v5h4M8 17l5.5-5.5 2 2L10 19H8v-2Z" /></>,
  folder: <path d="M3 6.5A1.5 1.5 0 0 1 4.5 5h5l1.5 2H19.5A1.5 1.5 0 0 1 21 8.5v9A1.5 1.5 0 0 1 19.5 19h-15A1.5 1.5 0 0 1 3 17.5v-11Z" />,
  inbox: <><path d="M4 5h16v13H4z" /><path d="M4 14h4l2 3h4l2-3h4" /></>,
  journal: <><rect x="5" y="3" width="14" height="18" rx="1.5" /><path d="M9 7h6M9 11h6M9 15h4M7 3v18" /></>,
  mail: <><rect x="3" y="5" width="18" height="14" rx="2" /><path d="m4 7 8 6 8-6" /></>,
  notes: <><path d="M5 3h14v14l-5 4H5z" /><path d="M14 21v-4h5M8 8h8M8 12h6" /></>,
  reminders: <><path d="M18 9a6 6 0 1 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4" /></>,
  sent: <><path d="m3 11 18-8-7 18-3-7-8-3Z" /><path d="m11 14 4-4" /></>,
  settings: <><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1-2.1 2.1-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.5v.2h-3v-.2a1.7 1.7 0 0 0-1-1.5 1.7 1.7 0 0 0-1.9.3l-.1.1-2.1-2.1.1-.1A1.7 1.7 0 0 0 7 15a1.7 1.7 0 0 0-1.5-1H5.3v-3h.2A1.7 1.7 0 0 0 7 10a1.7 1.7 0 0 0-.3-1.9l-.1-.1 2.1-2.1.1.1a1.7 1.7 0 0 0 1.9.3 1.7 1.7 0 0 0 1-1.5v-.2h3v.2a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.9-.3l.1-.1 2.1 2.1-.1.1A1.7 1.7 0 0 0 19.4 10a1.7 1.7 0 0 0 1.5 1h.2v3h-.2a1.7 1.7 0 0 0-1.5 1Z" /></>,
  tasks: <><rect x="4" y="4" width="16" height="16" rx="2" /><path d="m8 9 1.5 1.5L12 8M13.5 10h3M8 15l1.5 1.5 2.5-2.5M13.5 16h3" /></>,
  trash: <><path d="M4 7h16M9 7V4h6v3M7 7l1 14h8l1-14M10 11v6M14 11v6" /></>
};

export function ClientIcon({ name, className }: { name: ClientIconName; className?: string }) {
  return <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{iconPaths[name]}</svg>;
}
