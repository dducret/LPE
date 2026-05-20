import React from "react";
import { blankContact, blankDraft, blankEvent, blankJournalEntry, blankNote, blankTask, countFolders, filterContacts, filterJournalEntries, filterMessages, filterNotes, filterTasks, quoteMessage } from "./client-helpers";
import type { ClientCopy } from "./i18n";
import type {
  ClientIdentity,
  CollaborationOverview,
  ClientWorkspacePayload,
  ContactItem,
  ClientTaskList,
  EventItem,
  Folder,
  ContactBookId,
  JournalEntryDraft,
  JournalEntryItem,
  MailboxAccountAccess,
  MailboxDelegationOverview,
  Message,
  MessageDraft,
  Mode,
  NoteDraft,
  NoteItem,
  ReminderItem,
  SieveOverview,
  Section,
  TaskDraft,
  TaskItem
} from "./client-types";

async function apiJson<T>(path: string, token: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...(options.headers ?? {})
    },
    credentials: "same-origin"
  });
  if (!response.ok) {
    const detail = (await response.text()).trim();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

function mapClientError(error: unknown, fallback: string) {
  const detail = error instanceof Error ? error.message.toLowerCase() : "";
  if (detail.includes("quota")) return "Quota exceeded. Free space or ask an administrator for a larger quota.";
  if (detail.includes("not found")) return "The requested item no longer exists.";
  if (detail.includes("forbidden")) return "This action is not allowed for the current account.";
  return fallback;
}

function mapSubmitError(error: unknown, copy: ClientCopy) {
  const detail = error instanceof Error ? error.message : "";
  const lowered = detail.toLowerCase();
  if (lowered.includes("at least one recipient") || lowered.includes("subject or body_text")) {
    return copy.validationMessage;
  }
  if (
    lowered.includes("send as is not granted")
    || lowered.includes("send on behalf is not granted")
    || lowered.includes("from email must match")
    || lowered.includes("sender email must match")
  ) {
    return copy.sendPermissionError;
  }
  if (detail && !lowered.startsWith("request failed")) return detail;
  return copy.saveError;
}

function splitRecipients(value: string) {
  return value
    .split(/[;,]/)
    .map((address) => address.trim())
    .filter(Boolean)
    .map((address) => ({ address }));
}

function buildMessagePayload(
  identity: ClientIdentity,
  mailbox: MailboxAccountAccess,
  draft: MessageDraft,
  draftMessageId: string | null
) {
  const sendOnBehalf = mailbox.accountId !== identity.account_id && draft.senderMode === "send_on_behalf";
  return {
    draft_message_id: draftMessageId,
    account_id: mailbox.accountId,
    source: "web-client",
    from_display: mailbox.displayName,
    from_address: mailbox.email,
    sender_display: sendOnBehalf ? identity.display_name : null,
    sender_address: sendOnBehalf ? identity.email : null,
    to: splitRecipients(draft.to),
    cc: splitRecipients(draft.cc),
    bcc: [],
    subject: draft.subject,
    body_text: draft.body,
    body_html_sanitized: null,
    internet_message_id: null,
    mime_blob_ref: null,
    size_octets: new Blob([draft.subject, draft.body]).size
  };
}

function draftFromMessage(message: Message, mailboxAccountId: string): MessageDraft {
  return {
    mailboxAccountId,
    senderMode: "send_as",
    to: message.to,
    cc: message.cc,
    subject: message.subject,
    body: message.body.join("\n")
  };
}

function followupDueIso(daysFromToday: number) {
  const value = new Date();
  value.setDate(value.getDate() + daysFromToday);
  value.setHours(23, 59, 59, 0);
  return value.toISOString();
}

function reminderIso(minutesFromNow: number) {
  return new Date(Date.now() + minutesFromNow * 60 * 1000).toISOString();
}

export function useClientWorkspace(copy: ClientCopy, authToken: string | null, identity: ClientIdentity | null) {
  const [section, setSection] = React.useState<Section>("mail");
  const [folder, setFolder] = React.useState<Folder>("inbox");
  const [contactBook, setContactBook] = React.useState<ContactBookId>("default");
  const [query, setQuery] = React.useState("");
  const [mail, setMail] = React.useState<Message[]>([]);
  const [events, setEvents] = React.useState<EventItem[]>([]);
  const [contacts, setContacts] = React.useState<ContactItem[]>([]);
  const [tasks, setTasks] = React.useState<TaskItem[]>([]);
  const [notes, setNotes] = React.useState<NoteItem[]>([]);
  const [journalEntries, setJournalEntries] = React.useState<JournalEntryItem[]>([]);
  const [reminders, setReminders] = React.useState<ReminderItem[]>([]);
  const [taskLists, setTaskLists] = React.useState<ClientTaskList[]>([]);
  const [messageId, setMessageId] = React.useState("");
  const [eventId, setEventId] = React.useState("");
  const [contactId, setContactId] = React.useState("");
  const [taskId, setTaskId] = React.useState("");
  const [noteId, setNoteId] = React.useState("");
  const [journalEntryId, setJournalEntryId] = React.useState("");
  const [reminderId, setReminderId] = React.useState("");
  const [mode, setMode] = React.useState<Mode>("closed");
  const [notice, setNotice] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [loadError, setLoadError] = React.useState("");
  const [messageBusy, setMessageBusy] = React.useState(false);
  const [draft, setDraft] = React.useState<MessageDraft>(() => blankDraft(identity?.account_id));
  const [draftMessageId, setDraftMessageId] = React.useState<string | null>(null);
  const [eventForm, setEventForm] = React.useState(blankEvent());
  const [contactForm, setContactForm] = React.useState(blankContact());
  const [taskForm, setTaskForm] = React.useState<TaskDraft>(() => blankTask());
  const [noteForm, setNoteForm] = React.useState<NoteDraft>(() => blankNote());
  const [journalEntryForm, setJournalEntryForm] = React.useState<JournalEntryDraft>(() => blankJournalEntry());
  const [collaboration, setCollaboration] = React.useState<CollaborationOverview | null>(null);
  const [mailboxDelegation, setMailboxDelegation] = React.useState<MailboxDelegationOverview | null>(null);
  const [sieve, setSieve] = React.useState<SieveOverview | null>(null);
  const [shareForm, setShareForm] = React.useState({
    kind: "contacts" as "contacts" | "calendar" | "tasks",
    taskListId: "",
    granteeEmail: "",
    mayRead: true,
    mayWrite: false,
    mayDelete: false,
    mayShare: false
  });
  const [mailboxForm, setMailboxForm] = React.useState({
    granteeEmail: "",
    senderRight: "send_as" as "send_as" | "send_on_behalf"
  });
  const [sieveForm, setSieveForm] = React.useState({
    name: "vacation",
    content: "require [\"vacation\"];\n\nvacation :days 7 :subject \"Out of office\" \"I am currently away.\";",
    activate: true
  });

  const loadWorkspace = React.useCallback(async () => {
    if (!authToken || !identity) {
      setMail([]);
      setEvents([]);
      setContacts([]);
      setTasks([]);
      setNotes([]);
      setJournalEntries([]);
      setReminders([]);
      setTaskLists([]);
      return;
    }

    setLoading(true);
    setLoadError("");
    try {
      const [payload, nextNotes, nextJournalEntries, nextReminders] = await Promise.all([
        apiJson<ClientWorkspacePayload>("mail/workspace", authToken),
        apiJson<NoteItem[]>("mail/notes", authToken),
        apiJson<JournalEntryItem[]>("mail/journal", authToken),
        apiJson<ReminderItem[]>("mail/reminders", authToken)
      ]);
      setMail(payload.messages);
      setEvents(payload.events);
      setContacts(payload.contacts);
      setTasks(payload.tasks);
      setNotes(nextNotes);
      setJournalEntries(nextJournalEntries);
      setReminders(nextReminders);
    } catch {
      setLoadError(copy.loadError);
    } finally {
      setLoading(false);
    }
  }, [authToken, copy.loadError, identity]);

  const loadSettings = React.useCallback(async () => {
    if (!authToken || !identity) {
      setCollaboration(null);
      setMailboxDelegation(null);
      setSieve(null);
      setTaskLists([]);
      return;
    }

    try {
      const [nextCollaboration, nextMailboxDelegation, nextSieve, nextTaskLists] = await Promise.all([
        apiJson<CollaborationOverview>("mail/shares", authToken),
        apiJson<{ overview: MailboxDelegationOverview }>("mail/delegation", authToken),
        apiJson<SieveOverview>("mail/sieve", authToken),
        apiJson<ClientTaskList[]>("mail/task-lists", authToken)
      ]);
      setCollaboration(nextCollaboration);
      setMailboxDelegation(nextMailboxDelegation.overview);
      setSieve(nextSieve);
      setTaskLists(nextTaskLists);
      if (nextSieve.activeScript) {
        setSieveForm({
          name: nextSieve.activeScript.name,
          content: nextSieve.activeScript.content,
          activate: true
        });
      }
    } catch {
      // Keep the main workspace available even when settings endpoints fail.
    }
  }, [authToken, identity]);

  React.useEffect(() => {
    void loadWorkspace();
  }, [loadWorkspace]);

  React.useEffect(() => {
    void loadSettings();
  }, [loadSettings]);

  const ownedTaskLists = React.useMemo(() => taskLists.filter((item) => item.isOwned), [taskLists]);

  React.useEffect(() => {
    if (shareForm.kind !== "tasks") return;
    const selected = ownedTaskLists.find((item) => item.id === shareForm.taskListId);
    if (selected) return;
    setShareForm((value) => ({ ...value, taskListId: ownedTaskLists[0]?.id ?? "" }));
  }, [ownedTaskLists, shareForm.kind, shareForm.taskListId]);

  const composerMailboxes = React.useMemo<MailboxAccountAccess[]>(() => {
    if (!identity) return [];
    return [
      {
        accountId: identity.account_id,
        email: identity.email,
        displayName: identity.display_name,
        isOwned: true,
        mayRead: true,
        mayWrite: true,
        maySendAs: true,
        maySendOnBehalf: false
      },
      ...(mailboxDelegation?.incomingMailboxes ?? []).filter((entry) => entry.accountId !== identity.account_id)
    ];
  }, [identity, mailboxDelegation]);

  React.useEffect(() => {
    if (!identity || composerMailboxes.length === 0) return;
    setDraft((current) => {
      const selected = composerMailboxes.find((entry) => entry.accountId === current.mailboxAccountId) ?? composerMailboxes[0];
      const nextSenderMode = selected.accountId === identity.account_id || !selected.maySendOnBehalf
        ? "send_as"
        : !selected.maySendAs
          ? "send_on_behalf"
          : current.senderMode;
      if (selected.accountId === current.mailboxAccountId && nextSenderMode === current.senderMode) {
        return current;
      }
      return { ...current, mailboxAccountId: selected.accountId, senderMode: nextSenderMode };
    });
  }, [composerMailboxes, identity]);

  React.useEffect(() => {
    if (!authToken) return;
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${protocol}://${window.location.host}/api/jmap/ws`, "jmap");

    socket.addEventListener("open", () => {
      socket.send(JSON.stringify({
        "@type": "WebSocketPushEnable",
        dataTypes: ["Mailbox", "Email", "CalendarEvent", "ContactCard", "Task", "Note", "JournalEntry"]
      }));
    });
    socket.addEventListener("message", (event) => {
      try {
        const payload = JSON.parse(event.data as string) as { "@type"?: string };
        if (payload["@type"] === "StateChange") {
          void loadWorkspace();
          void loadSettings();
          pushNotice("Workspace updated from the canonical server state.");
        }
      } catch {
        // Ignore malformed push payloads.
      }
    });
    socket.addEventListener("error", () => undefined);

    return () => socket.close();
  }, [authToken, loadSettings, loadWorkspace]);

  const counts = React.useMemo(() => countFolders(mail), [mail]);
  const filtered = React.useMemo(() => filterMessages(mail, folder, query), [folder, mail, query]);
  const filteredEvents = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return events;
    return events.filter((item) => [item.title, item.location, item.attendees, item.notes, item.date, item.time].join(" ").toLowerCase().includes(needle));
  }, [events, query]);
  const filteredContacts = React.useMemo(() => filterContacts(contacts, contactBook, query), [contactBook, contacts, query]);
  const filteredTasks = React.useMemo(() => filterTasks(tasks, query), [query, tasks]);
  const filteredNotes = React.useMemo(() => filterNotes(notes, query), [notes, query]);
  const filteredJournalEntries = React.useMemo(() => filterJournalEntries(journalEntries, query), [journalEntries, query]);
  const filteredReminders = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return reminders;
    return reminders.filter((item) => [item.title, item.sourceType, item.status, item.dueAt ?? "", item.reminderAt].join(" ").toLowerCase().includes(needle));
  }, [query, reminders]);

  React.useEffect(() => {
    if (messageId && !filtered.some((item) => item.id === messageId)) setMessageId("");
  }, [filtered, messageId]);
  React.useEffect(() => {
    if (eventId && !filteredEvents.some((item) => item.id === eventId)) setEventId("");
  }, [eventId, filteredEvents]);
  React.useEffect(() => {
    if (contactId && !filteredContacts.some((item) => item.id === contactId)) setContactId("");
  }, [contactId, filteredContacts]);
  React.useEffect(() => {
    if (taskId && !filteredTasks.some((item) => item.id === taskId)) setTaskId("");
  }, [filteredTasks, taskId]);
  React.useEffect(() => {
    if (noteId && !filteredNotes.some((item) => item.id === noteId)) setNoteId("");
  }, [filteredNotes, noteId]);
  React.useEffect(() => {
    if (journalEntryId && !filteredJournalEntries.some((item) => item.id === journalEntryId)) setJournalEntryId("");
  }, [filteredJournalEntries, journalEntryId]);
  React.useEffect(() => {
    if (reminderId && !filteredReminders.some((item) => `${item.sourceType}:${item.sourceId}` === reminderId)) setReminderId("");
  }, [filteredReminders, reminderId]);

  const current = filtered.find((item) => item.id === messageId) ?? null;
  const currentEvent = filteredEvents.find((item) => item.id === eventId) ?? events.find((item) => item.id === eventId);
  const currentContact = filteredContacts.find((item) => item.id === contactId) ?? contacts.find((item) => item.id === contactId);
  const currentTask = filteredTasks.find((item) => item.id === taskId) ?? tasks.find((item) => item.id === taskId);
  const currentNote = filteredNotes.find((item) => item.id === noteId) ?? notes.find((item) => item.id === noteId);
  const currentJournalEntry = filteredJournalEntries.find((item) => item.id === journalEntryId) ?? journalEntries.find((item) => item.id === journalEntryId);
  const currentReminder = filteredReminders.find((item) => `${item.sourceType}:${item.sourceId}` === reminderId) ?? reminders.find((item) => `${item.sourceType}:${item.sourceId}` === reminderId);

  React.useEffect(() => setEventForm(blankEvent(currentEvent)), [currentEvent]);
  React.useEffect(() => setContactForm(blankContact(currentContact)), [currentContact]);
  React.useEffect(() => setTaskForm(blankTask(currentTask, ownedTaskLists[0]?.id)), [currentTask, ownedTaskLists]);
  React.useEffect(() => setNoteForm(blankNote(currentNote)), [currentNote]);
  React.useEffect(() => setJournalEntryForm(blankJournalEntry(currentJournalEntry)), [currentJournalEntry]);

  const pushNotice = React.useCallback((value: string) => {
    setNotice(value);
    window.setTimeout(() => setNotice(""), 2200);
  }, []);

  const resources = React.useMemo(
    () => contacts.filter((item) => /room|equipment|resource/i.test(`${item.role} ${item.team}`)),
    [contacts]
  );

  const openComposer = React.useCallback((next: Mode, item?: Message) => {
    const defaultMailboxAccountId = identity?.account_id ?? "";
    setSection("mail");
    setMode(next);
    setDraftMessageId(next === "draft" && item ? item.id : null);
    setMessageId(item?.id ?? "");
    if (next === "draft" && item) return setDraft(draftFromMessage(item, defaultMailboxAccountId));
    if (!item || next === "new") return setDraft(blankDraft(defaultMailboxAccountId));
    if (next === "reply") {
      return setDraft({
        mailboxAccountId: defaultMailboxAccountId,
        senderMode: "send_as",
        to: item.fromAddress,
        cc: "",
        subject: item.subject.toLowerCase().startsWith("re:") ? item.subject : `Re: ${item.subject}`,
        body: quoteMessage(item)
      });
    }
    setDraft({
      mailboxAccountId: defaultMailboxAccountId,
      senderMode: "send_as",
      to: "",
      cc: "",
      subject: item.subject.toLowerCase().startsWith("fwd:") ? item.subject : `Fwd: ${item.subject}`,
      body: quoteMessage(item)
    });
  }, [identity]);

  const closeComposer = React.useCallback(() => {
    setMode("closed");
    setDraftMessageId(null);
    setDraft(blankDraft(identity?.account_id));
    setMessageId("");
  }, [identity]);

  const selectMessage = React.useCallback((id: string) => {
    const item = mail.find((message) => message.id === id);
    setMessageId(id);
    if (item?.folder === "drafts") {
      openComposer("draft", item);
    } else {
      setMode("closed");
      setDraftMessageId(null);
    }
  }, [mail, openComposer]);

  const saveMessage = React.useCallback(async (asDraft: boolean) => {
    if (messageBusy) return;
    if (!authToken || !identity) return;
    if (!draft.subject.trim() && !draft.body.trim()) return pushNotice(copy.validationMessage);
    if (!asDraft && !draft.to.trim()) return pushNotice(copy.validationMessage);
    const mailbox = composerMailboxes.find((entry) => entry.accountId === draft.mailboxAccountId) ?? composerMailboxes[0];
    if (!mailbox) return pushNotice(copy.saveError);
    if (asDraft && mailbox.accountId !== identity.account_id && !mailbox.mayWrite) {
      return pushNotice(copy.saveError);
    }
    if (!asDraft && mailbox.accountId !== identity.account_id && !mailbox.maySendAs && !mailbox.maySendOnBehalf) {
      return pushNotice(copy.saveError);
    }
    if (!asDraft && draft.senderMode === "send_on_behalf" && !mailbox.maySendOnBehalf) {
      return pushNotice(copy.saveError);
    }
    if (!asDraft && draft.senderMode === "send_as" && mailbox.accountId !== identity.account_id && !mailbox.maySendAs) {
      return pushNotice(copy.saveError);
    }

    setMessageBusy(true);
    if (!asDraft) pushNotice(copy.sendingMessage);
    try {
      await apiJson(asDraft ? "mail/messages/draft" : "mail/messages/submit", authToken, {
        method: "POST",
        body: JSON.stringify(buildMessagePayload(identity, mailbox, draft, draftMessageId))
      });
      setFolder(asDraft ? "drafts" : "sent");
      setMode("closed");
      setDraftMessageId(null);
      setMessageId("");
      setDraft(blankDraft(identity.account_id));
      await loadWorkspace();
      pushNotice(asDraft ? copy.noticeDraft : copy.noticeSent);
    } catch (error) {
      pushNotice(asDraft ? mapClientError(error, copy.saveError) : mapSubmitError(error, copy));
    } finally {
      setMessageBusy(false);
    }
  }, [authToken, composerMailboxes, copy, draft, draftMessageId, identity, loadWorkspace, messageBusy, pushNotice]);

  const refreshWorkspace = React.useCallback(async () => {
    await loadWorkspace();
    pushNotice(copy.noticeSyncDone);
  }, [copy.noticeSyncDone, loadWorkspace, pushNotice]);

  const toggleMessageFlag = React.useCallback(async (message: Message) => {
    if (!authToken || messageBusy) return;
    const flagged = !message.flagged;
    setMessageBusy(true);
    try {
      await apiJson<{ status: string }>(`mail/messages/${message.id}/flag`, authToken, {
        method: "PUT",
        body: JSON.stringify({ flagged })
      });
      setMail((items) => items.map((item) => item.id === message.id ? {
        ...item,
        flagged,
        followupFlagStatus: flagged ? "flagged" : "none",
        followupStartAt: null,
        followupDueAt: null,
        followupCompletedAt: null,
        reminderSet: false,
        reminderAt: null,
        reminderDismissedAt: null
      } : item));
      await loadWorkspace();
      pushNotice(flagged ? copy.noticeFlagged : copy.noticeUnflagged);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    } finally {
      setMessageBusy(false);
    }
  }, [authToken, copy.noticeFlagged, copy.noticeUnflagged, copy.saveError, loadWorkspace, messageBusy, pushNotice]);

  const completeMessageFlag = React.useCallback(async (message: Message, completed: boolean) => {
    if (!authToken || messageBusy) return;
    setMessageBusy(true);
    try {
      await apiJson<{ status: string }>(`mail/messages/${message.id}/flag`, authToken, {
        method: "PUT",
        body: JSON.stringify({ flagged: true, completed })
      });
      setMail((items) => items.map((item) => item.id === message.id ? {
        ...item,
        flagged: true,
        followupFlagStatus: completed ? "complete" : "flagged",
        followupStartAt: item.followupStartAt,
        followupDueAt: item.followupDueAt,
        followupCompletedAt: completed ? new Date().toISOString() : null,
        reminderSet: completed ? false : item.reminderSet,
        reminderAt: completed ? null : item.reminderAt,
        reminderDismissedAt: completed ? null : item.reminderDismissedAt
      } : item));
      await loadWorkspace();
      pushNotice(completed ? copy.noticeFlagCompleted : copy.noticeFlagReopened);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    } finally {
      setMessageBusy(false);
    }
  }, [authToken, copy.noticeFlagCompleted, copy.noticeFlagReopened, copy.saveError, loadWorkspace, messageBusy, pushNotice]);

  const setMessageFlagDue = React.useCallback(async (message: Message, daysFromToday: number | null) => {
    if (!authToken || messageBusy) return;
    const dueAt = daysFromToday === null ? null : followupDueIso(daysFromToday);
    setMessageBusy(true);
    try {
      await apiJson<{ status: string }>(`mail/messages/${message.id}/flag`, authToken, {
        method: "PUT",
        body: JSON.stringify(daysFromToday === null
          ? { flagged: true, completed: false, clear_due: true }
          : { flagged: true, completed: false, due_at: dueAt })
      });
      setMail((items) => items.map((item) => item.id === message.id ? {
        ...item,
        flagged: true,
        followupFlagStatus: "flagged",
        followupStartAt: dueAt,
        followupDueAt: dueAt,
        followupCompletedAt: null,
        reminderSet: item.reminderSet,
        reminderAt: item.reminderAt,
        reminderDismissedAt: item.reminderDismissedAt
      } : item));
      await loadWorkspace();
      pushNotice(daysFromToday === null ? copy.noticeFlagDueCleared : copy.noticeFlagDueUpdated);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    } finally {
      setMessageBusy(false);
    }
  }, [authToken, copy.noticeFlagDueCleared, copy.noticeFlagDueUpdated, copy.saveError, loadWorkspace, messageBusy, pushNotice]);

  const setMessageFlagReminder = React.useCallback(async (message: Message, minutesFromNow: number | null) => {
    if (!authToken || messageBusy) return;
    const remindAt = minutesFromNow === null ? null : reminderIso(minutesFromNow);
    setMessageBusy(true);
    try {
      await apiJson<{ status: string }>(`mail/messages/${message.id}/flag`, authToken, {
        method: "PUT",
        body: JSON.stringify(minutesFromNow === null
          ? { flagged: true, completed: false, clear_reminder: true }
          : { flagged: true, completed: false, reminder_at: remindAt })
      });
      setMail((items) => items.map((item) => item.id === message.id ? {
        ...item,
        flagged: true,
        followupFlagStatus: "flagged",
        followupCompletedAt: null,
        reminderSet: minutesFromNow !== null,
        reminderAt: remindAt,
        reminderDismissedAt: null
      } : item));
      await loadWorkspace();
      pushNotice(minutesFromNow === null ? copy.noticeReminderCleared : copy.noticeReminderUpdated);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    } finally {
      setMessageBusy(false);
    }
  }, [authToken, copy.noticeReminderCleared, copy.noticeReminderUpdated, copy.saveError, loadWorkspace, messageBusy, pushNotice]);

  const deleteDraft = React.useCallback(async () => {
    if (!authToken || !draftMessageId) return;
    try {
      await apiJson(`mail/messages/${draftMessageId}/draft`, authToken, { method: "DELETE" });
      setMode("closed");
      setDraftMessageId(null);
      setMessageId("");
      setDraft(blankDraft(identity?.account_id));
      await loadWorkspace();
      pushNotice(copy.noticeDraftDeleted);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, copy.noticeDraftDeleted, copy.saveError, draftMessageId, identity, loadWorkspace, pushNotice]);

  const saveContact = React.useCallback(async () => {
    if (!authToken) return;
    if (!contactForm.name.trim() || !contactForm.email.trim()) return pushNotice(copy.validationContact);
    try {
      const item = await apiJson<ContactItem>("mail/contacts", authToken, {
        method: "POST",
        body: JSON.stringify({ id: currentContact?.id ?? null, collectionId: currentContact?.addressBookId ?? contactBook, ...contactForm })
      });
      await loadWorkspace();
      setContactId(item.id);
      pushNotice(currentContact ? copy.noticeContactUpdated : copy.noticeContactCreated);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, contactBook, contactForm, copy, currentContact, loadWorkspace, pushNotice]);

  const deleteContact = React.useCallback(async () => {
    if (!authToken || !currentContact) return;
    if (!window.confirm(copy.contactDeleteConfirm)) return;
    try {
      await apiJson(`mail/contacts/${currentContact.id}`, authToken, { method: "DELETE" });
      setContactId("");
      setContactForm(blankContact());
      await loadWorkspace();
      pushNotice(copy.noticeContactDeleted);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.contactDeleteConfirm, copy.noticeContactDeleted, copy.saveError, currentContact, loadWorkspace, pushNotice]);

  const resetContactForm = React.useCallback(() => {
    setContactId("");
    setContactForm(blankContact());
  }, []);

  const saveEvent = React.useCallback(async () => {
    if (!authToken) return;
    if (!eventForm.title.trim() || !eventForm.date.trim() || !eventForm.time.trim()) return pushNotice(copy.validationCalendar);
    try {
      const item = await apiJson<EventItem>("mail/calendar/events", authToken, {
        method: "POST",
        body: JSON.stringify({ id: currentEvent?.id ?? null, ...eventForm })
      });
      await loadWorkspace();
      setEventId(item.id);
      pushNotice(currentEvent ? copy.noticeCalendarUpdated : copy.noticeCalendarCreated);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, copy, currentEvent, eventForm, loadWorkspace, pushNotice]);

  const deleteEvent = React.useCallback(async () => {
    if (!authToken || !currentEvent) return;
    if (!window.confirm(copy.calendarDeleteConfirm)) return;
    try {
      await apiJson(`mail/calendar/events/${currentEvent.id}`, authToken, { method: "DELETE" });
      setEventId("");
      setEventForm(blankEvent());
      await loadWorkspace();
      pushNotice(copy.noticeCalendarDeleted);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.calendarDeleteConfirm, copy.noticeCalendarDeleted, copy.saveError, currentEvent, loadWorkspace, pushNotice]);

  const resetEventForm = React.useCallback(() => {
    setEventId("");
    setEventForm(blankEvent());
  }, []);

  const saveTask = React.useCallback(async () => {
    if (!authToken) return;
    if (!taskForm.title.trim()) return pushNotice(copy.validationMessage);
    try {
      const item = await apiJson<TaskItem>("mail/tasks", authToken, {
        method: "POST",
        body: JSON.stringify({
          id: currentTask?.id ?? null,
          task_list_id: taskForm.taskListId,
          title: taskForm.title,
          description: taskForm.description,
          status: taskForm.status,
          due_at: taskForm.dueAt,
          completed_at: taskForm.completedAt,
          sort_order: taskForm.sortOrder
        })
      });
      await loadWorkspace();
      setTaskId(item.id);
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, copy.validationMessage, currentTask, loadWorkspace, pushNotice, taskForm]);

  const deleteTask = React.useCallback(async () => {
    if (!authToken || !currentTask) return;
    try {
      await apiJson(`mail/tasks/${currentTask.id}`, authToken, { method: "DELETE" });
      setTaskId("");
      setTaskForm(blankTask(undefined, ownedTaskLists[0]?.id));
      await loadWorkspace();
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, currentTask, loadWorkspace, ownedTaskLists, pushNotice]);

  const resetTaskForm = React.useCallback(() => {
    setTaskId("");
    setTaskForm(blankTask(undefined, ownedTaskLists[0]?.id));
  }, [ownedTaskLists]);

  const saveNote = React.useCallback(async () => {
    if (!authToken) return;
    if (!noteForm.title.trim() && !noteForm.bodyText.trim()) return pushNotice(copy.validationMessage);
    try {
      const item = await apiJson<NoteItem>("mail/notes", authToken, {
        method: "POST",
        body: JSON.stringify({ id: currentNote?.id ?? null, ...noteForm })
      });
      await loadWorkspace();
      setNoteId(item.id);
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, copy.validationMessage, currentNote, loadWorkspace, noteForm, pushNotice]);

  const deleteNote = React.useCallback(async () => {
    if (!authToken || !currentNote) return;
    try {
      await apiJson(`mail/notes/${currentNote.id}`, authToken, { method: "DELETE" });
      setNoteId("");
      setNoteForm(blankNote());
      await loadWorkspace();
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, currentNote, loadWorkspace, pushNotice]);

  const resetNoteForm = React.useCallback(() => {
    setNoteId("");
    setNoteForm(blankNote());
  }, []);

  const saveJournalEntry = React.useCallback(async () => {
    if (!authToken) return;
    if (!journalEntryForm.subject.trim()) return pushNotice(copy.validationMessage);
    try {
      const item = await apiJson<JournalEntryItem>("mail/journal", authToken, {
        method: "POST",
        body: JSON.stringify({ id: currentJournalEntry?.id ?? null, ...journalEntryForm })
      });
      await loadWorkspace();
      setJournalEntryId(item.id);
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, copy.validationMessage, currentJournalEntry, journalEntryForm, loadWorkspace, pushNotice]);

  const deleteJournalEntry = React.useCallback(async () => {
    if (!authToken || !currentJournalEntry) return;
    try {
      await apiJson(`mail/journal/${currentJournalEntry.id}`, authToken, { method: "DELETE" });
      setJournalEntryId("");
      setJournalEntryForm(blankJournalEntry());
      await loadWorkspace();
      pushNotice(copy.noticeSyncDone);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.noticeSyncDone, copy.saveError, currentJournalEntry, loadWorkspace, pushNotice]);

  const resetJournalEntryForm = React.useCallback(() => {
    setJournalEntryId("");
    setJournalEntryForm(blankJournalEntry());
  }, []);

  const saveShare = React.useCallback(async () => {
    if (!authToken || !shareForm.granteeEmail.trim()) return pushNotice(copy.validationContact);
    try {
      if (shareForm.kind === "tasks") {
        if (!shareForm.taskListId) return pushNotice(copy.settings.validationTaskList);
        await apiJson(`mail/task-lists/${shareForm.taskListId}/shares`, authToken, {
          method: "PUT",
          body: JSON.stringify({
            granteeEmail: shareForm.granteeEmail,
            mayRead: shareForm.mayRead,
            mayWrite: shareForm.mayWrite,
            mayDelete: shareForm.mayDelete,
            mayShare: shareForm.mayShare
          })
        });
      } else {
        await apiJson("mail/shares", authToken, {
          method: "PUT",
          body: JSON.stringify({
            kind: shareForm.kind,
            granteeEmail: shareForm.granteeEmail,
            mayRead: shareForm.mayRead,
            mayWrite: shareForm.mayWrite,
            mayDelete: shareForm.mayDelete,
            mayShare: shareForm.mayShare
          })
        });
      }
      await loadSettings();
      setShareForm((value) => ({ ...value, granteeEmail: "" }));
      pushNotice(copy.settings.shareUpdated);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.settings.shareUpdated, copy.settings.validationTaskList, copy.validationContact, loadSettings, pushNotice, shareForm]);

  const deleteShare = React.useCallback(async (kind: string, granteeAccountId: string, taskListId?: string) => {
    if (!authToken) return;
    try {
      if (kind === "tasks") {
        if (!taskListId) return pushNotice(copy.saveError);
        await apiJson(`mail/task-lists/${taskListId}/shares/${granteeAccountId}`, authToken, { method: "DELETE" });
      } else {
        await apiJson(`mail/shares/${kind}/${granteeAccountId}`, authToken, { method: "DELETE" });
      }
      await loadSettings();
      pushNotice(copy.settings.shareRemoved);
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.settings.shareRemoved, loadSettings, pushNotice]);

  const saveMailboxDelegation = React.useCallback(async () => {
    if (!authToken || !mailboxForm.granteeEmail.trim()) return pushNotice(copy.validationContact);
    try {
      await apiJson("mail/delegation/mailboxes", authToken, {
        method: "PUT",
        body: JSON.stringify({ granteeEmail: mailboxForm.granteeEmail })
      });
      await loadSettings();
      pushNotice("Mailbox delegation updated.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.validationContact, loadSettings, mailboxForm.granteeEmail, pushNotice]);

  const deleteMailboxDelegation = React.useCallback(async (granteeAccountId: string) => {
    if (!authToken) return;
    try {
      await apiJson(`mail/delegation/mailboxes/${granteeAccountId}`, authToken, { method: "DELETE" });
      await loadSettings();
      pushNotice("Mailbox delegation removed.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, loadSettings, pushNotice]);

  const saveSenderDelegation = React.useCallback(async () => {
    if (!authToken || !mailboxForm.granteeEmail.trim()) return pushNotice(copy.validationContact);
    try {
      await apiJson("mail/delegation/sender", authToken, {
        method: "PUT",
        body: JSON.stringify({ granteeEmail: mailboxForm.granteeEmail, senderRight: mailboxForm.senderRight })
      });
      await loadSettings();
      pushNotice("Sender delegation updated.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.validationContact, loadSettings, mailboxForm, pushNotice]);

  const deleteSenderDelegation = React.useCallback(async (senderRight: string, granteeAccountId: string) => {
    if (!authToken) return;
    try {
      await apiJson(`mail/delegation/sender/${senderRight}/${granteeAccountId}`, authToken, { method: "DELETE" });
      await loadSettings();
      pushNotice("Sender delegation removed.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, loadSettings, pushNotice]);

  const saveSieve = React.useCallback(async () => {
    if (!authToken || !sieveForm.name.trim() || !sieveForm.content.trim()) return pushNotice(copy.validationMessage);
    try {
      await apiJson("mail/sieve", authToken, {
        method: "POST",
        body: JSON.stringify(sieveForm)
      });
      await loadSettings();
      pushNotice("Sieve script saved.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.validationMessage, loadSettings, pushNotice, sieveForm]);

  const loadSieveScript = React.useCallback(async (name: string) => {
    if (!authToken) return;
    try {
      const script = await apiJson<{ name: string; content: string; isActive: boolean }>(`mail/sieve/${encodeURIComponent(name)}`, authToken);
      setSieveForm({ name: script.name, content: script.content, activate: script.isActive });
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, pushNotice]);

  const deleteSieve = React.useCallback(async (name: string) => {
    if (!authToken) return;
    try {
      await apiJson(`mail/sieve/${encodeURIComponent(name)}`, authToken, { method: "DELETE" });
      await loadSettings();
      pushNotice("Sieve script deleted.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, loadSettings, pushNotice]);

  const activateSieve = React.useCallback(async (name: string | null) => {
    if (!authToken) return;
    try {
      await apiJson("mail/sieve/active", authToken, {
        method: "PUT",
        body: JSON.stringify({ name })
      });
      await loadSettings();
      pushNotice(name ? "Sieve script activated." : "Sieve script disabled.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, loadSettings, pushNotice]);

  return {
    section,
    setSection,
    folder,
    setFolder,
    contactBook,
    setContactBook,
    query,
    setQuery,
    mail,
    events,
    contacts,
    tasks,
    notes,
    journalEntries,
    reminders,
    taskLists: ownedTaskLists,
    messageId,
    setMessageId: selectMessage,
    eventId,
    setEventId,
    contactId,
    setContactId,
    taskId,
    setTaskId,
    noteId,
    setNoteId,
    journalEntryId,
    setJournalEntryId,
    reminderId,
    setReminderId,
    mode,
    closeComposer,
    notice: notice || loadError || (loading ? copy.loadingWorkspace : ""),
    messageBusy,
    draft,
    setDraft,
    eventForm,
    setEventForm,
    contactForm,
    setContactForm,
    taskForm,
    setTaskForm,
    noteForm,
    setNoteForm,
    journalEntryForm,
    setJournalEntryForm,
    counts,
    filtered,
    filteredEvents,
    filteredContacts,
    filteredTasks,
    filteredNotes,
    filteredJournalEntries,
    filteredReminders,
    current,
    composerMailboxes,
    currentEvent,
    currentContact,
    currentTask,
    currentNote,
    currentJournalEntry,
    currentReminder,
    openComposer,
    saveMessage,
    toggleMessageFlag,
    completeMessageFlag,
    setMessageFlagDue,
    setMessageFlagReminder,
    deleteDraft,
    refreshWorkspace,
    saveContact,
    saveEvent,
    deleteContact,
    deleteEvent,
    saveTask,
    deleteTask,
    saveNote,
    deleteNote,
    saveJournalEntry,
    deleteJournalEntry,
    resources,
    collaboration,
    mailboxDelegation,
    sieve,
    shareForm,
    setShareForm,
    mailboxForm,
    setMailboxForm,
    sieveForm,
    setSieveForm,
    saveShare,
    deleteShare,
    saveMailboxDelegation,
    deleteMailboxDelegation,
    saveSenderDelegation,
    deleteSenderDelegation,
    saveSieve,
    loadSieveScript,
    deleteSieve,
    activateSieve,
    resetContactForm,
    resetEventForm,
    resetTaskForm,
    resetNoteForm,
    resetJournalEntryForm
  };
}
