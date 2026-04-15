import type { ContactDraft, ContactItem, EventDraft, EventItem, Folder, Message, MessageDraft } from "./client-types";

export const mkId = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`;

export const blankDraft = (): MessageDraft => ({ to: "", cc: "", subject: "", body: "" });

export const blankContact = (contact?: ContactItem): ContactDraft => ({
  name: contact?.name ?? "",
  role: contact?.role ?? "",
  email: contact?.email ?? "",
  phone: contact?.phone ?? "",
  team: contact?.team ?? "",
  notes: contact?.notes ?? ""
});

export const blankEvent = (event?: EventItem): EventDraft => ({
  date: event?.date ?? "2026-04-15",
  time: event?.time ?? "09:00",
  title: event?.title ?? "",
  location: event?.location ?? "",
  attendees: event?.attendees ?? "",
  notes: event?.notes ?? ""
});

export const quoteMessage = (message: Message) => ["", "", `--- ${message.from} <${message.fromAddress}> ---`, ...message.body].join("\n");

export function countFolders(messages: Message[]): Record<Folder, number> {
  const value: Record<Folder, number> = { focused: 0, inbox: 0, drafts: 0, sent: 0, archive: 0 };
  for (const item of messages) value[item.folder] += 1;
  return value;
}

export function filterMessages(messages: Message[], folder: Folder, query: string): Message[] {
  const needle = query.trim().toLowerCase();
  return messages.filter((item) =>
    item.folder === folder &&
    [item.from, item.fromAddress, item.to, item.cc, item.subject, item.preview, item.tags.join(" "), item.body.join(" ")]
      .join(" ")
      .toLowerCase()
      .includes(needle)
  );
}
