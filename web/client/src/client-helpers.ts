import type { ContactBookId, ContactDraft, ContactItem, EventDraft, EventItem, Folder, JournalEntryDraft, JournalEntryItem, Message, MessageDraft, NoteDraft, NoteItem, TaskDraft, TaskItem } from "./client-types";

export const blankDraft = (mailboxAccountId = ""): MessageDraft => ({
  mailboxAccountId,
  senderMode: "send_as",
  to: "",
  cc: "",
  subject: "",
  body: ""
});

export const blankContact = (contact?: ContactItem): ContactDraft => ({
  name: contact?.name ?? "",
  role: contact?.role ?? "",
  email: contact?.email ?? "",
  phone: contact?.phone ?? "",
  team: contact?.team ?? "",
  notes: contact?.notes ?? ""
});

export const blankEvent = (event?: EventItem): EventDraft => ({
  date: event?.date ?? new Date().toISOString().slice(0, 10),
  time: event?.time ?? "09:00",
  title: event?.title ?? "",
  location: event?.location ?? "",
  attendees: event?.attendees ?? "",
  notes: event?.notes ?? ""
});

export const blankTask = (task?: TaskItem, taskListId?: string): TaskDraft => ({
  taskListId: task?.taskListId ?? taskListId ?? null,
  title: task?.title ?? "",
  description: task?.description ?? "",
  status: task?.status ?? "needs-action",
  dueAt: task?.dueAt ?? null,
  completedAt: task?.completedAt ?? null,
  sortOrder: task?.sortOrder ?? 0
});

export const blankNote = (note?: NoteItem): NoteDraft => ({
  title: note?.title ?? "",
  bodyText: note?.bodyText ?? "",
  color: note?.color ?? "yellow",
  categoriesJson: note?.categoriesJson ?? "[]"
});

export const blankJournalEntry = (entry?: JournalEntryItem): JournalEntryDraft => ({
  subject: entry?.subject ?? "",
  bodyText: entry?.bodyText ?? "",
  entryType: entry?.entryType ?? "Phone call",
  messageClass: entry?.messageClass ?? "IPM.Activity",
  startsAt: entry?.startsAt ?? null,
  endsAt: entry?.endsAt ?? null,
  occurredAt: entry?.occurredAt ?? null,
  companiesJson: entry?.companiesJson ?? "[]",
  contactsJson: entry?.contactsJson ?? "[]"
});

export const quoteMessage = (message: Message) => ["", "", `--- ${message.from} <${message.fromAddress}> ---`, ...message.body].join("\n");

export function countFolders(messages: Message[]): Record<Folder, number> {
  const value: Record<Folder, number> = {
    focused: 0,
    inbox: 0,
    drafts: 0,
    sent: 0,
    archive: 0,
    junk: 0,
    outbox: 0,
    rss_feeds: 0,
    conversation_history: 0,
    sync_issues: 0,
    conflicts: 0,
    local_failures: 0,
    server_failures: 0
  };
  for (const item of messages) value[item.folder] += 1;
  value.focused = value.inbox;
  return value;
}

export function filterMessages(messages: Message[], folder: Folder, query: string): Message[] {
  const needle = query.trim().toLowerCase();
  return messages.filter((item) =>
    (folder === "focused" ? item.folder === "inbox" : item.folder === folder) &&
    [item.from, item.fromAddress, item.to, item.cc, item.subject, item.preview, item.tags.join(" "), item.body.join(" ")]
      .join(" ")
      .toLowerCase()
      .includes(needle)
  );
}

export function filterContacts(contacts: ContactItem[], contactBook: ContactBookId, query: string): ContactItem[] {
  const needle = query.trim().toLowerCase();
  return contacts.filter((item) =>
    item.addressBookId === contactBook &&
    [item.name, item.role, item.email, item.phone, item.team, item.notes].join(" ").toLowerCase().includes(needle)
  );
}

export function filterTasks(tasks: TaskItem[], query: string): TaskItem[] {
  const needle = query.trim().toLowerCase();
  return tasks.filter((item) =>
    [item.title, item.description, item.status, item.ownerEmail, item.dueAt ?? ""].join(" ").toLowerCase().includes(needle)
  );
}

export function filterNotes(notes: NoteItem[], query: string): NoteItem[] {
  const needle = query.trim().toLowerCase();
  return notes.filter((item) =>
    [item.title, item.bodyText, item.color, item.categoriesJson].join(" ").toLowerCase().includes(needle)
  );
}

export function filterJournalEntries(entries: JournalEntryItem[], query: string): JournalEntryItem[] {
  const needle = query.trim().toLowerCase();
  return entries.filter((item) =>
    [item.subject, item.bodyText, item.entryType, item.messageClass, item.companiesJson, item.contactsJson].join(" ").toLowerCase().includes(needle)
  );
}
