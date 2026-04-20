import React from "react";
import type { ClientCopy } from "../i18n";
import type { ContactItem, EventDraft, EventItem } from "../client-types";

export function EventEditor(props: {
  copy: ClientCopy;
  currentEvent?: EventItem;
  eventForm: EventDraft;
  setEventForm: React.Dispatch<React.SetStateAction<EventDraft>>;
  resources: ContactItem[];
  onNew: () => void;
  onSave: () => void;
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

  return <section className="editor-shell"><div className="detail-header"><div><p className="detail-label">{props.copy.altDetailLabels.calendar}</p><h3>{props.currentEvent ? props.copy.calendarEditTitle : props.copy.calendarCreateTitle}</h3></div><div className="detail-actions"><button className="ghost-button" type="button" onClick={props.onNew}>{props.copy.calendarActions.new}</button><button className="primary-button" type="button" onClick={props.onSave}>{props.currentEvent ? props.copy.calendarActions.save : props.copy.calendarActions.create}</button></div></div><div className="form-grid"><label className="field"><span>{props.copy.calendarFields.date}</span><input type="date" value={props.eventForm.date} onChange={(event) => props.setEventForm((value) => ({ ...value, date: event.target.value }))} /></label><label className="field"><span>{props.copy.calendarFields.time}</span><input type="time" value={props.eventForm.time} onChange={(event) => props.setEventForm((value) => ({ ...value, time: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.calendarFields.title}</span><input value={props.eventForm.title} onChange={(event) => props.setEventForm((value) => ({ ...value, title: event.target.value }))} /></label><label className="field"><span>{props.copy.calendarFields.location}</span><input value={props.eventForm.location} onChange={(event) => props.setEventForm((value) => ({ ...value, location: event.target.value }))} /></label><label className="field"><span>Book resource</span><select defaultValue="" onChange={(event) => { if (event.target.value) addResource(event.target.value); event.target.value = ""; }}><option value="">Select room or equipment</option>{props.resources.map((resource) => <option key={resource.id} value={resource.id}>{resource.name} · {resource.role || resource.team}</option>)}</select></label><label className="field"><span>{props.copy.calendarFields.attendees}</span><input value={props.eventForm.attendees} onChange={(event) => props.setEventForm((value) => ({ ...value, attendees: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.calendarFields.notes}</span><textarea rows={8} value={props.eventForm.notes} onChange={(event) => props.setEventForm((value) => ({ ...value, notes: event.target.value }))} /></label></div></section>;
}
