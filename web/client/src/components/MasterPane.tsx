import React from "react";
import type { ClientCopy } from "../i18n";
import type { ContactItem, EventItem, Folder, Message, Mode, Section } from "../client-types";

export function MasterPane(props: {
  copy: ClientCopy;
  section: Section;
  folder: Folder;
  mode: Mode;
  filteredMessages: Message[];
  events: EventItem[];
  contacts: ContactItem[];
  messageId: string;
  eventId: string;
  contactId: string;
  onSelectMessage: (id: string) => void;
  onSelectEvent: (id: string) => void;
  onSelectContact: (id: string) => void;
  onCloseComposer: () => void;
}) {
  return (
    <section className="list-pane">
      <div className="pane-header">
        <div>
          <p className="pane-kicker">{props.copy.sections[props.section]}</p>
          <h3>{props.section === "mail" ? props.copy.folders[props.folder] : props.copy.altViews[props.section]}</h3>
        </div>
        <span className="pane-count">
          {props.section === "mail"
            ? props.copy.messageCount.replace("{count}", String(props.filteredMessages.length))
            : props.section === "calendar"
              ? props.copy.calendarCount.replace("{count}", String(props.events.length))
              : props.copy.contactCount.replace("{count}", String(props.contacts.length))}
        </span>
      </div>

      {props.section === "mail" ? (
        <>
          <div className="list-toolbar">
            {props.copy.toolbarChips.map((label) => <button key={label} className="toolbar-chip" type="button">{label}</button>)}
          </div>
          <div className="message-table-head">
            <span>{props.copy.listColumns.from}</span>
            <span>{props.copy.listColumns.subject}</span>
            <span>{props.copy.listColumns.received}</span>
          </div>
          <div className="message-list">{props.filteredMessages.map((item) => <button key={item.id} className={props.messageId === item.id && props.mode === "closed" ? "message-row is-active" : "message-row"} type="button" onClick={() => { props.onSelectMessage(item.id); props.onCloseComposer(); }}><span className="message-from"><strong>{item.from}</strong></span><span className="message-subject-block"><span className={item.unread ? "subject unread" : "subject"}>{item.subject}</span><span className="message-preview">{item.preview}</span><span className="message-inline-meta"><span className={`category-pill is-${item.category}`}>{props.copy.categories[item.category]}</span>{item.flagged ? <span className="flag-pill">{props.copy.flaggedShort}</span> : null}{item.attachments.length > 0 ? <span>{props.copy.attachmentCount.replace("{count}", String(item.attachments.length))}</span> : null}</span></span><span className="message-time">{item.timeLabel}</span></button>)}{props.filteredMessages.length === 0 ? <div className="empty-state">{props.copy.noMessages}</div> : null}</div>
        </>
      ) : null}
      {props.section === "calendar" ? <div className="agenda-list">{props.events.map((item) => <button className={props.eventId === item.id ? "agenda-card is-active" : "agenda-card"} key={item.id} type="button" onClick={() => props.onSelectEvent(item.id)}><span className="agenda-time">{item.time}</span><div><strong>{item.title}</strong><p>{item.location}</p><span>{item.attendees}</span></div></button>)}{props.events.length === 0 ? <div className="empty-state">{props.copy.noCalendarEvents}</div> : null}</div> : null}
      {props.section === "contacts" ? <div className="contact-list">{props.contacts.map((item) => <button className={props.contactId === item.id ? "contact-card is-active" : "contact-card"} key={item.id} type="button" onClick={() => props.onSelectContact(item.id)}><div className="contact-avatar">{item.name.slice(0, 2).toUpperCase()}</div><div><strong>{item.name}</strong><p>{item.role}</p><span>{item.team}</span></div></button>)}{props.contacts.length === 0 ? <div className="empty-state">{props.copy.noContacts}</div> : null}</div> : null}
    </section>
  );
}
