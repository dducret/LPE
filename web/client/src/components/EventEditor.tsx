import React from "react";
import type { ClientCopy } from "../i18n";
import type { ContactItem, EventDraft, EventItem } from "../client-types";
import { Button, Input, Select, Textarea } from "../../../ui/src/components/primitives";

export function EventEditor(props: {
  copy: ClientCopy;
  currentEvent?: EventItem;
  eventForm: EventDraft;
  setEventForm: React.Dispatch<React.SetStateAction<EventDraft>>;
  resources: ContactItem[];
  onNew: () => void;
  onSave: () => void;
  onDelete: () => void;
}) {
  function addResource(resourceId: string) {
    const resource = props.resources.find((item) => item.id === resourceId);
    if (!resource) return;
    props.setEventForm((value) => {
      const attendees = value.attendees.split(",").map((entry) => entry.trim()).filter(Boolean);
      if (!attendees.includes(resource.name)) attendees.push(resource.name);
      return {
        ...value,
        location: value.location || resource.name,
        attendees: attendees.join(", ")
      };
    });
  }

  return <section className="editor-shell"><div className="detail-header"><div><p className="detail-label">{props.copy.altDetailLabels.calendar}</p><h3>{props.currentEvent ? props.copy.calendarEditTitle : props.copy.calendarCreateTitle}</h3></div><div className="detail-actions">{props.currentEvent ? <Button variant="danger" type="button" onClick={props.onDelete}>{props.copy.calendarActions.delete}</Button> : null}<Button variant="ghost" type="button" onClick={props.onNew}>{props.copy.calendarActions.new}</Button><Button variant="primary" type="button" onClick={props.onSave}>{props.currentEvent ? props.copy.calendarActions.save : props.copy.calendarActions.create}</Button></div></div><div className="form-grid"><label className="field"><span>{props.copy.calendarFields.date}</span><Input type="date" value={props.eventForm.date} onChange={(event) => props.setEventForm((value) => ({ ...value, date: event.target.value }))} /></label><label className="field"><span>{props.copy.calendarFields.time}</span><Input type="time" value={props.eventForm.time} onChange={(event) => props.setEventForm((value) => ({ ...value, time: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.calendarFields.title}</span><Input value={props.eventForm.title} onChange={(event) => props.setEventForm((value) => ({ ...value, title: event.target.value }))} /></label><label className="field"><span>{props.copy.calendarFields.location}</span><Input value={props.eventForm.location} onChange={(event) => props.setEventForm((value) => ({ ...value, location: event.target.value }))} /></label><label className="field"><span>{props.copy.calendarFields.resource}</span><Select defaultValue="" onChange={(event) => { if (event.target.value) addResource(event.target.value); event.target.value = ""; }}><option value="">{props.copy.calendarFields.resourcePlaceholder}</option>{props.resources.map((resource) => <option key={resource.id} value={resource.id}>{resource.name} · {resource.role || resource.team}</option>)}</Select></label><label className="field"><span>{props.copy.calendarFields.attendees}</span><Input value={props.eventForm.attendees} onChange={(event) => props.setEventForm((value) => ({ ...value, attendees: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.calendarFields.notes}</span><Textarea rows={8} value={props.eventForm.notes} onChange={(event) => props.setEventForm((value) => ({ ...value, notes: event.target.value }))} /></label></div></section>;
}
