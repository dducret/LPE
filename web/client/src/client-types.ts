export type Section = "mail" | "calendar" | "contacts" | "tasks" | "notes" | "journal" | "reminders" | "settings";
export type Folder = "focused" | "inbox" | "drafts" | "sent" | "archive" | "junk" | "outbox" | "rss_feeds" | "conversation_history" | "sync_issues" | "conflicts" | "local_failures" | "server_failures";
export type ContactBookId = "default" | "suggested_contacts" | "quick_contacts" | "im_contact_list";
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
  followupFlagStatus: "none" | "flagged" | "complete";
  followupStartAt: string | null;
  followupDueAt: string | null;
  followupCompletedAt: string | null;
  reminderSet: boolean;
  reminderAt: string | null;
  reminderDismissedAt: string | null;
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
  addressBookId: ContactBookId;
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
export type ContactDraft = Omit<ContactItem, "id" | "addressBookId">;

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
  tasks: TaskItem[];
};

export type ClientSyncStatus = {
  lastRefreshedAt: string | null;
  pushConnected: boolean;
  counts: {
    mail: number;
    calendar: number;
    contacts: number;
    tasks: number;
    notes: number;
    journal: number;
    reminders: number;
    delegation: number;
  };
};

export type TaskItem = {
  id: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  isOwned: boolean;
  rights: CollaborationRights;
  taskListId: string;
  taskListSortOrder: number;
  title: string;
  description: string;
  status: string;
  dueAt: string | null;
  completedAt: string | null;
  sortOrder: number;
  updatedAt: string;
};

export type TaskDraft = {
  taskListId: string | null;
  title: string;
  description: string;
  status: string;
  dueAt: string | null;
  completedAt: string | null;
  sortOrder: number;
};

export type NoteItem = {
  id: string;
  title: string;
  bodyText: string;
  color: string;
  categoriesJson: string;
  createdAt: string;
  updatedAt: string;
};

export type NoteDraft = Omit<NoteItem, "id" | "createdAt" | "updatedAt">;

export type JournalEntryItem = {
  id: string;
  subject: string;
  bodyText: string;
  entryType: string;
  messageClass: string;
  startsAt: string | null;
  endsAt: string | null;
  occurredAt: string | null;
  companiesJson: string;
  contactsJson: string;
  createdAt: string;
  updatedAt: string;
};

export type JournalEntryDraft = Omit<JournalEntryItem, "id" | "createdAt" | "updatedAt">;

export type ReminderItem = {
  sourceType: string;
  sourceId: string;
  title: string;
  dueAt: string | null;
  reminderAt: string;
  dismissedAt: string | null;
  completedAt: string | null;
  status: string;
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
  outgoingTaskLists: TaskListGrant[];
  incomingContactCollections: CollaborationCollection[];
  incomingCalendarCollections: CollaborationCollection[];
  incomingTaskListCollections: CollaborationCollection[];
};

export type TaskListGrant = {
  id: string;
  taskListId: string;
  taskListName: string;
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

export type ClientTaskList = {
  id: string;
  ownerAccountId: string;
  ownerEmail: string;
  ownerDisplayName: string;
  isOwned: boolean;
  rights: CollaborationRights;
  name: string;
  role: string | null;
  sortOrder: number;
  updatedAt: string;
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
