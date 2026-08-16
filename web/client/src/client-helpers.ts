import type { ContactBookId, ContactDraft, ContactItem, ContactValue, EventDraft, EventItem, Folder, JournalEntryDraft, JournalEntryItem, Message, MessageDraft, NoteDraft, NoteItem, SystemFolder, TaskDraft, TaskItem } from "./client-types";

export const blankDraft = (mailboxAccountId = ""): MessageDraft => ({
  mailboxAccountId,
  senderMode: "send_as",
  to: "",
  cc: "",
  bcc: "",
  subject: "",
  body: ""
});

export const blankContact = (contact?: ContactItem): ContactDraft => ({
  name: contact?.name ?? "",
  role: contact?.role ?? "",
  email: contact?.email ?? "",
  phone: contact?.phone ?? "",
  team: contact?.team ?? "",
  notes: contact?.notes ?? "",
  emailsJson: contact?.emailsJson ?? [],
  phonesJson: contact?.phonesJson ?? [],
  addressesJson: contact?.addressesJson ?? [],
  urlsJson: contact?.urlsJson ?? [],
  photoData: contact?.photoData ?? null,
  photoContentType: contact?.photoContentType ?? null,
  categoriesJson: contact?.categoriesJson ?? [],
  birthday: contact?.birthday ?? null,
  anniversary: contact?.anniversary ?? null,
  childrenJson: contact?.childrenJson ?? [],
  spouse: contact?.spouse ?? "",
  assistantName: contact?.assistantName ?? "",
  assistantPhone: contact?.assistantPhone ?? ""
});

export function contactValue(values: ContactValue[], key: string, label: string, fallbackFirst = false): string {
  const matching = values.find((value) => value.label === label) ?? (fallbackFirst ? values[0] : undefined);
  const value = matching?.[key] ?? (key === "address" ? matching?.full : key === "url" ? matching?.href : undefined);
  return typeof value === "string" ? value : "";
}

export function setContactValue(values: ContactValue[], key: string, label: string, value: string, fallbackFirst = false): ContactValue[] {
  const index = values.findIndex((item) => item.label === label);
  const target = index >= 0 ? index : (fallbackFirst && values.length ? 0 : -1);
  if (target >= 0) {
    if (!value.trim()) return values.filter((_, itemIndex) => itemIndex !== target);
    return values.map((item, itemIndex) => itemIndex === target ? { ...item, [key]: value, label } : item);
  }
  return value.trim() ? [...values, { [key]: value, label }] : values;
}

export function normalizedContactDraft(contact: ContactDraft): ContactDraft {
  return {
    ...contact,
    emailsJson: setContactValue(contact.emailsJson, "email", "work", contact.email, true),
    phonesJson: setContactValue(contact.phonesJson, "phone", "work", contact.phone, true)
  };
}

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
  entryType: entry?.entryType ?? "phone-call",
  messageClass: entry?.messageClass ?? "IPM.Activity",
  startsAt: entry?.startsAt ?? null,
  endsAt: entry?.endsAt ?? null,
  occurredAt: entry?.occurredAt ?? null,
  companiesJson: entry?.companiesJson ?? "[]",
  contactsJson: entry?.contactsJson ?? "[]"
});

export const quoteMessage = (message: Message) => ["", "", `--- ${message.from} <${message.fromAddress}> ---`, ...message.body].join("\n");

export function countFolders(messages: Message[]): Record<SystemFolder, number> {
  const value: Record<SystemFolder, number> = {
    focused: 0,
    inbox: 0,
    drafts: 0,
    sent: 0,
    archive: 0,
    trash: 0,
    junk: 0,
    outbox: 0,
    rss_feeds: 0,
    conversation_history: 0,
    sync_issues: 0,
    conflicts: 0,
    local_failures: 0,
    server_failures: 0
  };
  for (const item of messages) {
    if (item.folder in value) value[item.folder as SystemFolder] += 1;
  }
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
    [item.name, item.role, item.email, item.phone, item.team, item.notes, item.birthday, item.anniversary, item.spouse, item.assistantName, item.assistantPhone, ...item.categoriesJson, ...item.childrenJson, JSON.stringify(item.emailsJson), JSON.stringify(item.phonesJson), JSON.stringify(item.addressesJson), JSON.stringify(item.urlsJson)].join(" ").toLowerCase().includes(needle)
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
