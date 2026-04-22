import React from "react";
import { blankContact, blankDraft, blankEvent, countFolders, filterMessages, quoteMessage } from "./client-helpers";
import type { ClientCopy } from "./i18n";
import type {
  ClientIdentity,
  CollaborationOverview,
  ClientWorkspacePayload,
  ContactItem,
  EventItem,
  Folder,
  MailboxAccountAccess,
  MailboxDelegationOverview,
  Message,
  MessageDraft,
  Mode,
  SieveOverview,
  Section
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

export function useClientWorkspace(copy: ClientCopy, authToken: string | null, identity: ClientIdentity | null) {
  const [section, setSection] = React.useState<Section>("mail");
  const [folder, setFolder] = React.useState<Folder>("inbox");
  const [query, setQuery] = React.useState("");
  const [mail, setMail] = React.useState<Message[]>([]);
  const [events, setEvents] = React.useState<EventItem[]>([]);
  const [contacts, setContacts] = React.useState<ContactItem[]>([]);
  const [messageId, setMessageId] = React.useState("");
  const [eventId, setEventId] = React.useState("");
  const [contactId, setContactId] = React.useState("");
  const [mode, setMode] = React.useState<Mode>("closed");
  const [notice, setNotice] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [loadError, setLoadError] = React.useState("");
  const [draft, setDraft] = React.useState<MessageDraft>(() => blankDraft(identity?.account_id));
  const [draftMessageId, setDraftMessageId] = React.useState<string | null>(null);
  const [eventForm, setEventForm] = React.useState(blankEvent());
  const [contactForm, setContactForm] = React.useState(blankContact());
  const [collaboration, setCollaboration] = React.useState<CollaborationOverview | null>(null);
  const [mailboxDelegation, setMailboxDelegation] = React.useState<MailboxDelegationOverview | null>(null);
  const [sieve, setSieve] = React.useState<SieveOverview | null>(null);
  const [shareForm, setShareForm] = React.useState({
    kind: "contacts" as "contacts" | "calendar",
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
      return;
    }

    setLoading(true);
    setLoadError("");
    try {
      const payload = await apiJson<ClientWorkspacePayload>("mail/workspace", authToken);
      setMail(payload.messages);
      setEvents(payload.events);
      setContacts(payload.contacts);
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
      return;
    }

    try {
      const [nextCollaboration, nextMailboxDelegation, nextSieve] = await Promise.all([
        apiJson<CollaborationOverview>("mail/shares", authToken),
        apiJson<{ overview: MailboxDelegationOverview }>("mail/delegation", authToken),
        apiJson<SieveOverview>("mail/sieve", authToken)
      ]);
      setCollaboration(nextCollaboration);
      setMailboxDelegation(nextMailboxDelegation.overview);
      setSieve(nextSieve);
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
        dataTypes: ["Mailbox", "Email", "CalendarEvent", "ContactCard"]
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
  const filteredContacts = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return contacts;
    return contacts.filter((item) => [item.name, item.role, item.email, item.phone, item.team, item.notes].join(" ").toLowerCase().includes(needle));
  }, [contacts, query]);

  React.useEffect(() => {
    if (messageId && !filtered.some((item) => item.id === messageId)) setMessageId("");
  }, [filtered, messageId]);
  React.useEffect(() => {
    if (eventId && !filteredEvents.some((item) => item.id === eventId)) setEventId("");
  }, [eventId, filteredEvents]);
  React.useEffect(() => {
    if (contactId && !filteredContacts.some((item) => item.id === contactId)) setContactId("");
  }, [contactId, filteredContacts]);

  const current = filtered.find((item) => item.id === messageId) ?? null;
  const currentEvent = filteredEvents.find((item) => item.id === eventId) ?? events.find((item) => item.id === eventId);
  const currentContact = filteredContacts.find((item) => item.id === contactId) ?? contacts.find((item) => item.id === contactId);

  React.useEffect(() => setEventForm(blankEvent(currentEvent)), [currentEvent]);
  React.useEffect(() => setContactForm(blankContact(currentContact)), [currentContact]);

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

    try {
      await apiJson(asDraft ? "mail/messages/draft" : "mail/messages/submit", authToken, {
        method: "POST",
        body: JSON.stringify(buildMessagePayload(identity, mailbox, draft, draftMessageId))
      });
      setFolder(asDraft ? "drafts" : draftMessageId ? "inbox" : "sent");
      setMode("closed");
      setDraftMessageId(null);
      setMessageId("");
      setDraft(blankDraft(identity.account_id));
      await loadWorkspace();
      pushNotice(asDraft ? copy.noticeDraft : copy.noticeSent);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, composerMailboxes, copy, draft, draftMessageId, identity, loadWorkspace, pushNotice]);

  const refreshWorkspace = React.useCallback(async () => {
    await loadWorkspace();
    pushNotice(copy.noticeSyncDone);
  }, [copy.noticeSyncDone, loadWorkspace, pushNotice]);

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
        body: JSON.stringify({ id: currentContact?.id ?? null, ...contactForm })
      });
      await loadWorkspace();
      setContactId(item.id);
      pushNotice(currentContact ? copy.noticeContactUpdated : copy.noticeContactCreated);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, contactForm, copy, currentContact, loadWorkspace, pushNotice]);

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

  const resetEventForm = React.useCallback(() => {
    setEventId("");
    setEventForm(blankEvent());
  }, []);

  const saveShare = React.useCallback(async () => {
    if (!authToken || !shareForm.granteeEmail.trim()) return pushNotice(copy.validationContact);
    try {
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
      await loadSettings();
      setShareForm((value) => ({ ...value, granteeEmail: "" }));
      pushNotice("Share updated.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, copy.validationContact, loadSettings, pushNotice, shareForm]);

  const deleteShare = React.useCallback(async (kind: string, granteeAccountId: string) => {
    if (!authToken) return;
    try {
      await apiJson(`mail/shares/${kind}/${granteeAccountId}`, authToken, { method: "DELETE" });
      await loadSettings();
      pushNotice("Share removed.");
    } catch (error) {
      pushNotice(mapClientError(error, copy.saveError));
    }
  }, [authToken, copy.saveError, loadSettings, pushNotice]);

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
    query,
    setQuery,
    mail,
    events,
    contacts,
    messageId,
    setMessageId: selectMessage,
    eventId,
    setEventId,
    contactId,
    setContactId,
    mode,
    closeComposer,
    notice: notice || loadError || (loading ? copy.loadingWorkspace : ""),
    draft,
    setDraft,
    eventForm,
    setEventForm,
    contactForm,
    setContactForm,
    counts,
    filtered,
    filteredEvents,
    filteredContacts,
    current,
    composerMailboxes,
    currentEvent,
    currentContact,
    openComposer,
    saveMessage,
    deleteDraft,
    refreshWorkspace,
    saveContact,
    saveEvent,
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
    resetEventForm
  };
}
