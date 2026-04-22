export type Section = "mail" | "calendar" | "contacts" | "settings";
export type Folder = "focused" | "inbox" | "drafts" | "sent" | "archive";
export type Mode = "closed" | "new" | "draft" | "reply" | "forward";

export type Attachment = {
  id: string;
  name: string;
  kind: string;
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
  tags: string[];
  attachments: Attachment[];
  body: string[];
};

export type EventItem = {
  id: string;
  date: string;
  time: string;
  timeZone?: string;
  durationMinutes?: number;
  recurrenceRule?: string;
  title: string;
  location: string;
  attendees: string;
  attendeesJson?: string;
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
  mailboxAccountId: string;
  senderMode: "send_as" | "send_on_behalf";
  to: string;
  cc: string;
  subject: string;
  body: string;
};

export type EventDraft = Omit<EventItem, "id">;
export type ContactDraft = Omit<ContactItem, "id">;

export type ClientIdentity = {
  account_id: string;
  email: string;
  display_name: string;
  expires_at: string;
};

export type ClientWorkspacePayload = {
  messages: Message[];
  events: EventItem[];
  contacts: ContactItem[];
};

export type CollaborationRights = {
  mayRead: boolean;
  mayWrite: boolean;
  mayDelete: boolean;
  mayShare: boolean;
};

export type CollaborationGrant = {
  id: string;
  kind: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  granteeAccountId: string;
  granteeEmail: string;
  granteeDisplayName: string;
  rights: CollaborationRights;
  createdAt: string;
  updatedAt: string;
};

export type CollaborationCollection = {
  id: string;
  kind: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  displayName: string;
  isOwned: boolean;
  rights: CollaborationRights;
};

export type CollaborationOverview = {
  outgoingContacts: CollaborationGrant[];
  outgoingCalendars: CollaborationGrant[];
  incomingContactCollections: CollaborationCollection[];
  incomingCalendarCollections: CollaborationCollection[];
};

export type MailboxAccountAccess = {
  accountId: string;
  email: string;
  displayName: string;
  isOwned: boolean;
  mayRead: boolean;
  mayWrite: boolean;
  maySendAs: boolean;
  maySendOnBehalf: boolean;
};

export type MailboxDelegationGrant = {
  id: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  granteeAccountId: string;
  granteeEmail: string;
  granteeDisplayName: string;
  createdAt: string;
  updatedAt: string;
};

export type SenderDelegationGrant = {
  id: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  granteeAccountId: string;
  granteeEmail: string;
  granteeDisplayName: string;
  senderRight: string;
  createdAt: string;
  updatedAt: string;
};

export type MailboxDelegationOverview = {
  outgoingMailboxes: MailboxDelegationGrant[];
  incomingMailboxes: MailboxAccountAccess[];
  outgoingSenderRights: SenderDelegationGrant[];
};

export type SieveScriptSummary = {
  name: string;
  isActive: boolean;
  sizeOctets: number;
  updatedAt: string;
};

export type SieveScriptDocument = {
  name: string;
  content: string;
  isActive: boolean;
  updatedAt: string;
};

export type SieveOverview = {
  scripts: SieveScriptSummary[];
  activeScript: SieveScriptDocument | null;
};
