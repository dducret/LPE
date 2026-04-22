import type { ContactDraft, ContactItem, EventDraft, EventItem, Folder, Message, MessageDraft } from "./client-types";

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

export const quoteMessage = (message: Message) => ["", "", `--- ${message.from} <${message.fromAddress}> ---`, ...message.body].join("\n");

export function countFolders(messages: Message[]): Record<Folder, number> {
  const value: Record<Folder, number> = { focused: 0, inbox: 0, drafts: 0, sent: 0, archive: 0 };
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
