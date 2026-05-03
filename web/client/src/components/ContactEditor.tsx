import React from "react";
import type { ClientCopy } from "../i18n";
import type { ContactDraft, ContactItem } from "../client-types";
import { Button, Input, Textarea } from "../../../ui/src/components/primitives";

export function ContactEditor(props: {
  copy: ClientCopy;
  currentContact?: ContactItem;
  contactForm: ContactDraft;
  setContactForm: React.Dispatch<React.SetStateAction<ContactDraft>>;
  onNew: () => void;
  onSave: () => void;
  onDelete: () => void;
}) {
  return <section className="editor-shell"><div className="detail-header"><div><p className="detail-label">{props.copy.altDetailLabels.contacts}</p><h3>{props.currentContact ? props.copy.contactsEditTitle : props.copy.contactsCreateTitle}</h3></div><div className="detail-actions">{props.currentContact ? <Button variant="danger" type="button" onClick={props.onDelete}>{props.copy.contactActions.delete}</Button> : null}<Button variant="ghost" type="button" onClick={props.onNew}>{props.copy.contactActions.new}</Button><Button variant="primary" type="button" onClick={props.onSave}>{props.currentContact ? props.copy.contactActions.save : props.copy.contactActions.create}</Button></div></div><div className="form-grid"><label className="field"><span>{props.copy.contactFields.name}</span><Input value={props.contactForm.name} onChange={(event) => props.setContactForm((value) => ({ ...value, name: event.target.value }))} /></label><label className="field"><span>{props.copy.contactFields.role}</span><Input value={props.contactForm.role} onChange={(event) => props.setContactForm((value) => ({ ...value, role: event.target.value }))} /></label><label className="field"><span>{props.copy.contactFields.email}</span><Input value={props.contactForm.email} onChange={(event) => props.setContactForm((value) => ({ ...value, email: event.target.value }))} /></label><label className="field"><span>{props.copy.contactFields.phone}</span><Input value={props.contactForm.phone} onChange={(event) => props.setContactForm((value) => ({ ...value, phone: event.target.value }))} /></label><label className="field"><span>{props.copy.contactFields.team}</span><Input value={props.contactForm.team} onChange={(event) => props.setContactForm((value) => ({ ...value, team: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.contactFields.notes}</span><Textarea rows={8} value={props.contactForm.notes} onChange={(event) => props.setContactForm((value) => ({ ...value, notes: event.target.value }))} /></label></div></section>;
}
