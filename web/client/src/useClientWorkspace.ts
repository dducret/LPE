import React from "react";
import { currentUser, seedContacts, seedEvents, seedMessages } from "./client-data";
import { blankContact, blankDraft, blankEvent, countFolders, filterMessages, mkId, quoteMessage } from "./client-helpers";
import type { ClientCopy } from "./i18n";
import type { ContactItem, EventItem, Folder, Message, Mode, Section } from "./client-types";

export function useClientWorkspace(copy: ClientCopy) {
  const [section, setSection] = React.useState<Section>("mail");
  const [folder, setFolder] = React.useState<Folder>("focused");
  const [query, setQuery] = React.useState("");
  const [mail, setMail] = React.useState(seedMessages);
  const [events, setEvents] = React.useState(seedEvents);
  const [contacts, setContacts] = React.useState(seedContacts);
  const [messageId, setMessageId] = React.useState("m1");
  const [eventId, setEventId] = React.useState("e1");
  const [contactId, setContactId] = React.useState("c1");
  const [mode, setMode] = React.useState<Mode>("closed");
  const [notice, setNotice] = React.useState("");
  const [draft, setDraft] = React.useState(blankDraft());
  const [eventForm, setEventForm] = React.useState(blankEvent(seedEvents[0]));
  const [contactForm, setContactForm] = React.useState(blankContact(seedContacts[0]));

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

  const saveMessage = React.useCallback((asDraft: boolean) => {
    if (!draft.subject.trim() && !draft.body.trim()) return pushNotice(copy.validationMessage);
    if (!asDraft && !draft.to.trim()) return pushNotice(copy.validationMessage);

    const item: Message = {
      id: mkId("m"),
      folder: asDraft ? "drafts" : "sent",
      from: currentUser.name,
      fromAddress: currentUser.email,
      to: draft.to.trim(),
      cc: draft.cc.trim(),
      subject: draft.subject.trim() || copy.untitledDraft,
      preview: draft.body.trim().split(/\n+/)[0] || copy.emptyDraftPreview,
      receivedAt: "2026-04-15 11:35",
      timeLabel: copy.nowLabel,
      unread: false,
      flagged: false,
      category: "internal",
      tags: [asDraft ? copy.tags.draft : mode === "reply" ? copy.tags.reply : mode === "forward" ? copy.tags.forward : copy.tags.outgoing],
      attachments: [],
      body: draft.body.split("\n").filter(Boolean)
    };

    setMail((currentMail) => [item, ...currentMail]);
    setFolder(item.folder);
    setMessageId(item.id);
    setMode("closed");
    setDraft(blankDraft());
    pushNotice(asDraft ? copy.noticeDraft : copy.noticeSent);
  }, [copy, draft, mode, pushNotice]);

  const saveContact = React.useCallback(() => {
    if (!contactForm.name.trim() || !contactForm.email.trim()) return pushNotice(copy.validationContact);
    if (currentContact) {
      setContacts((value) => value.map((item) => (item.id === currentContact.id ? { ...item, ...contactForm } : item)));
      return pushNotice(copy.noticeContactUpdated);
    }
    const item: ContactItem = { id: mkId("c"), ...contactForm };
    setContacts((value) => [item, ...value]);
    setContactId(item.id);
    pushNotice(copy.noticeContactCreated);
  }, [contactForm, copy, currentContact, pushNotice]);

  const resetContactForm = React.useCallback(() => {
    setContactId("");
    setContactForm(blankContact());
  }, []);

  const saveEvent = React.useCallback(() => {
    if (!eventForm.title.trim() || !eventForm.date.trim() || !eventForm.time.trim()) return pushNotice(copy.validationCalendar);
    if (currentEvent) {
      setEvents((value) => value.map((item) => (item.id === currentEvent.id ? { ...item, ...eventForm } : item)));
      return pushNotice(copy.noticeCalendarUpdated);
    }
    const item: EventItem = { id: mkId("e"), ...eventForm };
    setEvents((value) => [...value, item].sort((a, b) => `${a.date}${a.time}`.localeCompare(`${b.date}${b.time}`)));
    setEventId(item.id);
    pushNotice(copy.noticeCalendarCreated);
  }, [copy, currentEvent, eventForm, pushNotice]);

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
    notice,
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
