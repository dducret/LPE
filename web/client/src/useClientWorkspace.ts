import React from "react";
import { blankContact, blankDraft, blankEvent, countFolders, filterMessages, quoteMessage } from "./client-helpers";
import type { ClientCopy } from "./i18n";
import type {
  ClientIdentity,
  ClientWorkspacePayload,
  ContactItem,
  EventItem,
  Folder,
  Message,
  MessageDraft,
  Mode,
  Section
} from "./client-types";

async function apiJson<T>(path: string, token: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...(options.headers ?? {})
    }
  });
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return (await response.json()) as T;
}

function splitRecipients(value: string) {
  return value
    .split(/[;,]/)
    .map((address) => address.trim())
    .filter(Boolean)
    .map((address) => ({ address }));
}

function buildMessagePayload(identity: ClientIdentity, draft: MessageDraft) {
  return {
    account_id: identity.account_id,
    source: "web-client",
    from_display: identity.display_name,
    from_address: identity.email,
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

export function useClientWorkspace(copy: ClientCopy, authToken: string | null, identity: ClientIdentity | null) {
  const [section, setSection] = React.useState<Section>("mail");
  const [folder, setFolder] = React.useState<Folder>("focused");
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
  const [draft, setDraft] = React.useState(blankDraft());
  const [eventForm, setEventForm] = React.useState(blankEvent());
  const [contactForm, setContactForm] = React.useState(blankContact());

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

  React.useEffect(() => {
    void loadWorkspace();
  }, [loadWorkspace]);

  const counts = React.useMemo(() => countFolders(mail), [mail]);
  const filtered = React.useMemo(() => filterMessages(mail, folder, query), [folder, mail, query]);

  React.useEffect(() => {
    if (!filtered.some((item) => item.id === messageId)) setMessageId(filtered[0]?.id ?? "");
  }, [filtered, messageId]);

  const current = filtered.find((item) => item.id === messageId) ?? null;
  const currentEvent = events.find((item) => item.id === eventId);
  const currentContact = contacts.find((item) => item.id === contactId);

  React.useEffect(() => setEventForm(blankEvent(currentEvent)), [currentEvent]);
  React.useEffect(() => setContactForm(blankContact(currentContact)), [currentContact]);

  const pushNotice = React.useCallback((value: string) => {
    setNotice(value);
    window.setTimeout(() => setNotice(""), 2200);
  }, []);

  const openComposer = React.useCallback((next: Mode, item?: Message) => {
    setSection("mail");
    setMode(next);
    if (!item || next === "new") return setDraft(blankDraft());
    if (next === "reply") {
      return setDraft({
        to: item.fromAddress,
        cc: "",
        subject: item.subject.toLowerCase().startsWith("re:") ? item.subject : `Re: ${item.subject}`,
        body: quoteMessage(item)
      });
    }
    setDraft({
      to: "",
      cc: "",
      subject: item.subject.toLowerCase().startsWith("fwd:") ? item.subject : `Fwd: ${item.subject}`,
      body: quoteMessage(item)
    });
  }, []);

  const closeComposer = React.useCallback(() => setMode("closed"), []);

  const saveMessage = React.useCallback(async (asDraft: boolean) => {
    if (!authToken || !identity) return;
    if (!draft.subject.trim() && !draft.body.trim()) return pushNotice(copy.validationMessage);
    if (!asDraft && !draft.to.trim()) return pushNotice(copy.validationMessage);

    try {
      await apiJson(asDraft ? "mail/messages/draft" : "mail/messages/submit", authToken, {
        method: "POST",
        body: JSON.stringify(buildMessagePayload(identity, draft))
      });
      setFolder(asDraft ? "drafts" : "sent");
      setMode("closed");
      setDraft(blankDraft());
      await loadWorkspace();
      pushNotice(asDraft ? copy.noticeDraft : copy.noticeSent);
    } catch {
      pushNotice(copy.saveError);
    }
  }, [authToken, copy, draft, identity, loadWorkspace, pushNotice]);

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
    setMessageId,
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
    current,
    currentEvent,
    currentContact,
    openComposer,
    saveMessage,
    saveContact,
    saveEvent,
    resetContactForm,
    resetEventForm
  };
}
