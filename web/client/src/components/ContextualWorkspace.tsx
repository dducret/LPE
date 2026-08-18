import React from "react";
import type { ClientCopy } from "../i18n";
import type { ClientTaskList, ContactItem, EventItem, Message, TaskItem } from "../client-types";

export function MailRibbon(props: {
  copy: ClientCopy;
  current: Message | null;
  busy: boolean;
  onCompose: () => void;
  onReply: (message: Message) => void;
  onForward: (message: Message) => void;
  onToggleFlag: (message: Message) => void;
  onRefresh: () => void;
}) {
  const current = props.current;
  return (
    <div className="contextual-ribbon" aria-label={props.copy.sections.mail}>
      <div className="ribbon-group">
        <button className="ribbon-command is-primary" type="button" onClick={props.onCompose}>{props.copy.compose}</button>
      </div>
      <div className="ribbon-group">
        <button className="ribbon-command" type="button" disabled={!current || props.busy} onClick={() => current && props.onReply(current)}>{props.copy.messageActions.reply}</button>
        <button className="ribbon-command" type="button" disabled={!current || props.busy} onClick={() => current && props.onForward(current)}>{props.copy.messageActions.forward}</button>
      </div>
      <div className="ribbon-group">
        <button className="ribbon-command" type="button" disabled={!current || props.busy} onClick={() => current && props.onToggleFlag(current)}>{current?.flagged ? props.copy.messageActions.clearFlag : props.copy.messageActions.flag}</button>
        <button className="ribbon-command" type="button" onClick={() => window.print()}>{props.copy.ribbonSecondary[3]}</button>
      </div>
      <div className="ribbon-group">
        <button className="ribbon-command" type="button" onClick={props.onRefresh}>{props.copy.topActions.sync}</button>
      </div>
    </div>
  );
}

export function ContactsRibbon(props: {
  copy: ClientCopy;
  current: ContactItem | undefined;
  onNew: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onRefresh: () => void;
}) {
  return <div className="contextual-ribbon" aria-label={props.copy.sections.contacts}>
    <div className="ribbon-group"><button className="ribbon-command is-primary" type="button" onClick={props.onNew}>{props.copy.contactActions.new}</button></div>
    <div className="ribbon-group">
      <button className="ribbon-command" type="button" disabled={!props.current} onClick={props.onEdit}>{props.copy.contactActions.save}</button>
      <button className="ribbon-command" type="button" disabled={!props.current} onClick={props.onDelete}>{props.copy.contactActions.delete}</button>
    </div>
    <div className="ribbon-group"><button className="ribbon-command" type="button" onClick={() => window.print()}>{props.copy.ribbonSecondary[3]}</button></div>
    <div className="ribbon-group"><button className="ribbon-command" type="button" onClick={props.onRefresh}>{props.copy.topActions.sync}</button></div>
  </div>;
}

function startOfWeek(value: Date) {
  const result = new Date(value);
  const day = result.getDay();
  result.setDate(result.getDate() - (day === 0 ? 6 : day - 1));
  result.setHours(0, 0, 0, 0);
  return result;
}

function sameDay(left: Date, right: Date) {
  return left.getFullYear() === right.getFullYear() && left.getMonth() === right.getMonth() && left.getDate() === right.getDate();
}

function isoDate(value: Date) {
  return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
}

export function CalendarWorkspace(props: {
  copy: ClientCopy;
  events: EventItem[];
  selectedDate: Date;
  selectedEventId: string;
  onSelectEvent: (id: string) => void;
}) {
  const weekStart = startOfWeek(props.selectedDate);
  const weekDays = Array.from({ length: 7 }, (_, index) => {
    const date = new Date(weekStart);
    date.setDate(weekStart.getDate() + index);
    return date;
  });
  const hours = Array.from({ length: 11 }, (_, index) => index + 8);
  const formatter = new Intl.DateTimeFormat(undefined, { weekday: "short", month: "short", day: "numeric" });
  return (
    <section className="calendar-workspace" aria-label={props.copy.sections.calendar}>
      <div className="calendar-week-pane">
        <header className="calendar-week-heading">
          <strong>{weekDays.map((day) => formatter.format(day)).join(" – ")}</strong>
        </header>
        <div className="calendar-week-grid">
          <div className="calendar-time-column">{hours.map((hour) => <span key={hour}>{`${hour}:00`}</span>)}</div>
          <div className="calendar-days">
            {weekDays.map((day) => <div className={sameDay(day, props.selectedDate) ? "calendar-day is-selected" : "calendar-day"} key={isoDate(day)}>
              <div className="calendar-day-label">{formatter.format(day)}</div>
              {hours.map((hour) => <div className="calendar-hour" key={hour} />)}
              {props.events.filter((event) => event.date === isoDate(day)).map((event) => {
                const [hour, minute] = event.time.split(":").map(Number);
                const top = Math.max(0, ((hour - 8) * 60 + minute) / 60) * 48 + 32;
                const height = Math.max(38, Math.min(144, (event.durationMinutes ?? 60) / 60 * 48));
                return <button className={props.selectedEventId === event.id ? "calendar-event is-active" : "calendar-event"} key={event.id} style={{ top, height }} type="button" onClick={() => props.onSelectEvent(event.id)}><strong>{event.title}</strong><span>{event.location}</span></button>;
              })}
            </div>)}
          </div>
        </div>
      </div>
    </section>
  );
}

export function ContactsWorkspace(props: { copy: ClientCopy; contacts: ContactItem[]; selectedId: string; onSelect: (id: string) => void }) {
  return (
    <section className="contacts-workspace" aria-label={props.copy.sections.contacts}>
      <header><strong>{props.copy.altViews.contacts}</strong><span>{props.copy.contactCount.replace("{count}", String(props.contacts.length))}</span></header>
      <div className="contacts-table-head"><span>{props.copy.contactFields.name}</span><span>{props.copy.contactFields.email}</span></div>
      <div className="contacts-table">
        {props.contacts.map((contact) => <button className={props.selectedId === contact.id ? "contact-table-row is-active" : "contact-table-row"} key={contact.id} type="button" onClick={() => props.onSelect(contact.id)}>
          <span className="contact-table-name"><span className="contact-initials">{contact.name.slice(0, 2).toUpperCase()}</span><span><strong>{contact.name}</strong><small>{contact.role || contact.team}</small></span></span>
          <span className="contact-table-info"><span>{contact.email || contact.team}</span><small>{contact.phone}</small></span>
        </button>)}
        {props.contacts.length === 0 ? <p className="empty-state">{props.copy.noContacts}</p> : null}
      </div>
    </section>
  );
}

export function TasksWorkspace(props: { copy: ClientCopy; taskLists: ClientTaskList[]; tasks: TaskItem[]; selectedId: string; onSelect: (id: string) => void }) {
  return (
    <section className="tasks-workspace" aria-label={props.copy.sections.tasks}>
      <aside className="task-list-pane"><strong>{props.copy.sections.tasks}</strong>{props.taskLists.map((list) => <span key={list.id}>{list.name}</span>)}</aside>
      <div className="task-items-pane">
        <header><strong>{props.copy.objectEditor.tasks.new}</strong><span>{props.copy.objectCount.tasks.replace("{count}", String(props.tasks.length))}</span></header>
        <div className="task-item-list">{props.tasks.map((task) => <button className={props.selectedId === task.id ? "task-item-row is-active" : "task-item-row"} key={task.id} type="button" onClick={() => props.onSelect(task.id)}><span className={task.status === "completed" ? "task-check is-complete" : "task-check"} /><span><strong>{task.title}</strong><small>{task.dueAt ?? props.copy.noDate}</small></span></button>)}{props.tasks.length === 0 ? <p className="empty-state">{props.copy.emptyObjects.tasks}</p> : null}</div>
      </div>
    </section>
  );
}
