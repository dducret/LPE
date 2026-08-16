import React from "react";
import { contactValue, setContactValue } from "../client-helpers";
import type { ContactDraft, ContactItem } from "../client-types";
import type { ClientCopy } from "../i18n";
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
  const update = <K extends keyof ContactDraft>(key: K, value: ContactDraft[K]) => props.setContactForm((current) => ({ ...current, [key]: value }));
  const updateJson = (key: "emailsJson" | "phonesJson" | "addressesJson" | "urlsJson", valueKey: string, label: string, value: string, fallbackFirst = false) => props.setContactForm((current) => ({ ...current, [key]: setContactValue(current[key], valueKey, label, value, fallbackFirst) }));
  const photoSource = props.contactForm.photoData ? `data:${props.contactForm.photoContentType ?? "image/jpeg"};base64,${props.contactForm.photoData}` : null;
  const changePhoto = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result);
      update("photoData", result.slice(result.indexOf(",") + 1));
      update("photoContentType", file.type || "application/octet-stream");
    };
    reader.readAsDataURL(file);
  };
  const setDelimited = (key: "categoriesJson" | "childrenJson", value: string) => update(key, value.split(",").map((item) => item.trim()).filter(Boolean));

  return <section className="editor-shell"><div className="detail-header"><div><p className="detail-label">{props.copy.altDetailLabels.contacts}</p><h3>{props.currentContact ? props.copy.contactsEditTitle : props.copy.contactsCreateTitle}</h3></div><div className="detail-actions">{props.currentContact ? <Button variant="danger" type="button" onClick={props.onDelete}>{props.copy.contactActions.delete}</Button> : null}<Button variant="ghost" type="button" onClick={props.onNew}>{props.copy.contactActions.new}</Button><Button variant="primary" type="button" onClick={props.onSave}>{props.currentContact ? props.copy.contactActions.save : props.copy.contactActions.create}</Button></div></div><div className="form-grid">
    <div className="contact-photo field-wide"><div className="contact-photo-preview">{photoSource ? <img src={photoSource} alt="" /> : props.contactForm.name.slice(0, 2).toUpperCase()}</div><label className="field"><span>{props.copy.contactFields.photo}</span><Input type="file" accept="image/*" onChange={changePhoto} /></label>{photoSource ? <Button variant="ghost" type="button" onClick={() => { update("photoData", null); update("photoContentType", null); }}>{props.copy.contactFields.removePhoto}</Button> : null}</div>
    <label className="field"><span>{props.copy.contactFields.name}</span><Input value={props.contactForm.name} onChange={(event) => update("name", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.role}</span><Input value={props.contactForm.role} onChange={(event) => update("role", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.email}</span><Input type="email" value={props.contactForm.email} onChange={(event) => update("email", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.secondaryEmail}</span><Input type="email" value={contactValue(props.contactForm.emailsJson, "email", "email2")} onChange={(event) => updateJson("emailsJson", "email", "email2", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.phone}</span><Input type="tel" value={props.contactForm.phone} onChange={(event) => update("phone", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.secondaryPhone}</span><Input type="tel" value={contactValue(props.contactForm.phonesJson, "phone", "work2")} onChange={(event) => updateJson("phonesJson", "phone", "work2", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.team}</span><Input value={props.contactForm.team} onChange={(event) => update("team", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.webPage}</span><Input type="url" value={contactValue(props.contactForm.urlsJson, "url", "work", true)} onChange={(event) => updateJson("urlsJson", "url", "work", event.target.value, true)} /></label>
    <label className="field"><span>{props.copy.contactFields.birthday}</span><Input type="date" value={props.contactForm.birthday ?? ""} onChange={(event) => update("birthday", event.target.value || null)} /></label>
    <label className="field"><span>{props.copy.contactFields.anniversary}</span><Input type="date" value={props.contactForm.anniversary ?? ""} onChange={(event) => update("anniversary", event.target.value || null)} /></label>
    <label className="field"><span>{props.copy.contactFields.spouse}</span><Input value={props.contactForm.spouse} onChange={(event) => update("spouse", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.children}</span><Input value={props.contactForm.childrenJson.join(", ")} onChange={(event) => setDelimited("childrenJson", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.assistantName}</span><Input value={props.contactForm.assistantName} onChange={(event) => update("assistantName", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.assistantPhone}</span><Input type="tel" value={props.contactForm.assistantPhone} onChange={(event) => update("assistantPhone", event.target.value)} /></label>
    <label className="field field-wide"><span>{props.copy.contactFields.categories}</span><Input value={props.contactForm.categoriesJson.join(", ")} onChange={(event) => setDelimited("categoriesJson", event.target.value)} /></label>
    <label className="field"><span>{props.copy.contactFields.businessAddress}</span><Textarea rows={3} value={contactValue(props.contactForm.addressesJson, "address", "work", true)} onChange={(event) => updateJson("addressesJson", "address", "work", event.target.value, true)} /></label>
    <label className="field"><span>{props.copy.contactFields.homeAddress}</span><Textarea rows={3} value={contactValue(props.contactForm.addressesJson, "address", "home")} onChange={(event) => updateJson("addressesJson", "address", "home", event.target.value)} /></label>
    <label className="field field-wide"><span>{props.copy.contactFields.otherAddress}</span><Textarea rows={3} value={contactValue(props.contactForm.addressesJson, "address", "other")} onChange={(event) => updateJson("addressesJson", "address", "other", event.target.value)} /></label>
    <label className="field field-wide"><span>{props.copy.contactFields.notes}</span><Textarea rows={8} value={props.contactForm.notes} onChange={(event) => update("notes", event.target.value)} /></label>
  </div></section>;
}
