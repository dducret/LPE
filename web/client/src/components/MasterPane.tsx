import React from "react";
import type { ClientCopy } from "../i18n";
import type { CollaborationCollection, ContactBookId, ContactItem, EventItem, Folder, JournalEntryItem, Message, Mode, NoteItem, ReminderItem, Section, TaskItem } from "../client-types";
import { TabButton, Tabs } from "../../../ui/src/components/primitives";

export function MasterPane(props: {
  copy: ClientCopy;
  section: Section;
  folder: Folder;
  folderLabel: string;
  contactBook: ContactBookId;
  setContactBook: (contactBook: ContactBookId) => void;
  contactBooks: CollaborationCollection[];
  calendarCollectionId: string;
  setCalendarCollectionId: (collectionId: string) => void;
  calendarCollections: CollaborationCollection[];
  mode: Mode;
  filteredMessages: Message[];
  events: EventItem[];
  contacts: ContactItem[];
  tasks: TaskItem[];
  notes: NoteItem[];
  journalEntries: JournalEntryItem[];
  reminders: ReminderItem[];
  messageId: string;
  eventId: string;
  contactId: string;
  taskId: string;
  noteId: string;
  journalEntryId: string;
  reminderId: string;
  onSelectMessage: (id: string) => void;
  onSelectEvent: (id: string) => void;
  onSelectContact: (id: string) => void;
  onSelectTask: (id: string) => void;
  onSelectNote: (id: string) => void;
  onSelectJournalEntry: (id: string) => void;
  onSelectReminder: (id: string) => void;
}) {
  function taskStatusLabel(status: string) {
    if (status === "needs-action") return props.copy.taskStatuses.needsAction;
    if (status === "in-process") return props.copy.taskStatuses.inProcess;
    if (status === "completed") return props.copy.taskStatuses.completed;
    if (status === "cancelled") return props.copy.taskStatuses.cancelled;
    return status;
  }

  function enumLabel(value: string) {
    if (value === "active") return props.copy.enumValues.active;
    if (value === "dismissed") return props.copy.enumValues.dismissed;
    if (value === "mail") return props.copy.enumValues.mail;
    if (value === "task") return props.copy.enumValues.task;
    if (value === "calendar") return props.copy.enumValues.calendar;
    if (value === "phone-call") return props.copy.enumValues.phoneCall;
    return value;
  }

  const countLabel = props.section === "mail"
    ? props.copy.messageCount.replace("{count}", String(props.filteredMessages.length))
    : props.section === "calendar"
      ? props.copy.calendarCount.replace("{count}", String(props.events.length))
      : props.section === "contacts"
        ? props.copy.contactCount.replace("{count}", String(props.contacts.length))
        : props.section === "tasks"
          ? props.copy.objectCount.tasks.replace("{count}", String(props.tasks.length))
          : props.section === "notes"
            ? props.copy.objectCount.notes.replace("{count}", String(props.notes.length))
            : props.section === "journal"
              ? props.copy.objectCount.journal.replace("{count}", String(props.journalEntries.length))
              : props.section === "reminders"
                ? props.copy.objectCount.reminders.replace("{count}", String(props.reminders.length))
                : "";
  const messageGroups = new Map<string, Message[]>();
  for (const message of props.filteredMessages) {
    const date = message.receivedAt.split(" ", 1)[0] || message.receivedAt;
    messageGroups.set(date, [...(messageGroups.get(date) ?? []), message]);
  }

  return (
    <section className="list-pane">
      <div className="pane-header">
        <div>
          <p className="pane-kicker">{props.copy.sections[props.section]}</p>
          <h3>{props.section === "mail" ? props.folderLabel : props.copy.altViews[props.section]}</h3>
        </div>
        <span className="pane-count">{countLabel}</span>
      </div>

      {props.section === "mail" ? (
        <>
          <div className="message-list">
            <div className="message-list-columns" aria-hidden="true">
              <span>{props.copy.listColumns.from}</span>
              <span>{props.copy.listColumns.subject}</span>
              <span>{props.copy.listColumns.received}</span>
            </div>
            {[...messageGroups].map(([date, messages]) => (
              <div className="message-group" key={date}>
                <p className="message-group-label">{date}</p>
                {messages.map((item) => (
                  <button
                    key={item.id}
                    className={props.messageId === item.id ? "mail-list-row is-active" : "mail-list-row"}
                    type="button"
                    onClick={() => props.onSelectMessage(item.id)}
                  >
                    <span className={item.unread ? "mail-list-from unread" : "mail-list-from"}>{item.from}</span>
                    <span className="mail-list-subject">
                      <span className={item.unread ? "subject unread" : "subject"}>{item.subject}</span>
                      <span className="message-preview">{item.preview}</span>
                      {item.flagged ? <span className="flag-pill">{props.copy.flaggedShort}</span> : null}
                      {item.attachments.length > 0 ? <span className="message-meta-pill">{props.copy.attachmentCount.replace("{count}", String(item.attachments.length))}</span> : null}
                    </span>
                    <span className="message-time">{item.timeLabel}</span>
                  </button>
                ))}
              </div>
            ))}
            {props.filteredMessages.length === 0 ? <div className="empty-state">{props.copy.noMessages}</div> : null}
          </div>
        </>
      ) : null}
      {props.section === "calendar" ? <><Tabs className="collection-tabs" aria-label={props.copy.sections.calendar}><TabButton active={!props.calendarCollectionId} onClick={() => props.setCalendarCollectionId("")}>{props.copy.sections.calendar}</TabButton>{props.calendarCollections.map((collection) => <TabButton active={props.calendarCollectionId === collection.id} key={collection.id} onClick={() => props.setCalendarCollectionId(collection.id)}>{collection.displayName}</TabButton>)}</Tabs><div className="agenda-list">{props.events.map((item) => <button className={props.eventId === item.id ? "agenda-card is-active" : "agenda-card"} key={item.id} type="button" onClick={() => props.onSelectEvent(item.id)}><span className="agenda-time">{item.time}</span><div><strong>{item.title}</strong><p>{item.location}</p><span>{item.attendees}</span></div></button>)}{props.events.length === 0 ? <div className="empty-state">{props.copy.noCalendarEvents}</div> : null}</div></> : null}
      {props.section === "contacts" ? (
        <>
          <Tabs className="collection-tabs" aria-label={props.copy.sections.contacts}>
            {props.contactBooks.map((book) => (
              <TabButton
                key={book.id}
                active={props.contactBook === book.id}
                onClick={() => props.setContactBook(book.id)}
              >
                {book.displayName}
              </TabButton>
            ))}
          </Tabs>
          <div className="contact-list">{props.contacts.map((item) => <button className={props.contactId === item.id ? "contact-card is-active" : "contact-card"} key={item.id} type="button" onClick={() => props.onSelectContact(item.id)}><div className="contact-avatar">{item.photoData ? <img src={`data:${item.photoContentType ?? "image/jpeg"};base64,${item.photoData}`} alt="" /> : item.name.slice(0, 2).toUpperCase()}</div><div><strong>{item.name}</strong><p>{item.role}</p><span>{item.team}</span></div></button>)}{props.contacts.length === 0 ? <div className="empty-state">{props.copy.noContacts}</div> : null}</div>
        </>
      ) : null}
      {props.section === "tasks" ? <div className="agenda-list">{props.tasks.map((item) => <button className={props.taskId === item.id ? "agenda-card is-active" : "agenda-card"} key={item.id} type="button" onClick={() => props.onSelectTask(item.id)}><span className="agenda-time">{taskStatusLabel(item.status)}</span><div><strong>{item.title}</strong><p>{item.description}</p><span>{item.dueAt ?? props.copy.noDate}</span></div></button>)}{props.tasks.length === 0 ? <div className="empty-state">{props.copy.emptyObjects.tasks}</div> : null}</div> : null}
      {props.section === "notes" ? <div className="contact-list">{props.notes.map((item) => <button className={props.noteId === item.id ? "contact-card is-active" : "contact-card"} key={item.id} type="button" onClick={() => props.onSelectNote(item.id)}><div className="contact-avatar">{item.color.slice(0, 2).toUpperCase()}</div><div><strong>{item.title || props.copy.untitledDraft}</strong><p>{item.bodyText}</p><span>{item.updatedAt}</span></div></button>)}{props.notes.length === 0 ? <div className="empty-state">{props.copy.emptyObjects.notes}</div> : null}</div> : null}
      {props.section === "journal" ? <div className="agenda-list">{props.journalEntries.map((item) => <button className={props.journalEntryId === item.id ? "agenda-card is-active" : "agenda-card"} key={item.id} type="button" onClick={() => props.onSelectJournalEntry(item.id)}><span className="agenda-time">{enumLabel(item.entryType)}</span><div><strong>{item.subject}</strong><p>{item.bodyText}</p><span>{item.occurredAt ?? item.startsAt ?? props.copy.noDate}</span></div></button>)}{props.journalEntries.length === 0 ? <div className="empty-state">{props.copy.emptyObjects.journal}</div> : null}</div> : null}
      {props.section === "reminders" ? <div className="agenda-list">{props.reminders.map((item) => { const id = `${item.sourceType}:${item.sourceId}`; return <button className={props.reminderId === id ? "agenda-card is-active" : "agenda-card"} key={id} type="button" onClick={() => props.onSelectReminder(id)}><span className="agenda-time">{enumLabel(item.status)}</span><div><strong>{item.title}</strong><p>{enumLabel(item.sourceType)}</p><span>{item.reminderAt}</span></div></button>; })}{props.reminders.length === 0 ? <div className="empty-state">{props.copy.emptyObjects.reminders}</div> : null}</div> : null}
    </section>
  );
}
