export type Section = "mail" | "calendar" | "contacts";
export type Folder = "focused" | "inbox" | "drafts" | "sent" | "archive";
export type Category = "priority" | "customer" | "internal";
export type Mode = "closed" | "new" | "reply" | "forward";

export type Attachment = {
  id: string;
  name: string;
  kind: "PDF" | "DOCX" | "ODT";
  size: string;
};

export type Message = {
  id: string;
  folder: Folder;
  from: string;
  fromAddress: string;
  to: string;
  cc: string;
  subject: string;
  preview: string;
  receivedAt: string;
  timeLabel: string;
  unread: boolean;
  flagged: boolean;
  category: Category;
  tags: string[];
  attachments: Attachment[];
  body: string[];
};

export type EventItem = {
  id: string;
  date: string;
  time: string;
  title: string;
  location: string;
  attendees: string;
  notes: string;
};

export type ContactItem = {
  id: string;
  name: string;
  role: string;
  email: string;
  phone: string;
  team: string;
  notes: string;
};

export type MessageDraft = {
  to: string;
  cc: string;
  subject: string;
  body: string;
};

export type EventDraft = Omit<EventItem, "id">;
export type ContactDraft = Omit<ContactItem, "id">;
