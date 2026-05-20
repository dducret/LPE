import React from "react";
import { Button, Input, Select, Textarea } from "../../../ui/src/components/primitives";
import type { ClientCopy } from "../i18n";
import type { ClientTaskList, JournalEntryDraft, JournalEntryItem, NoteDraft, NoteItem, ReminderItem, Section, TaskDraft, TaskItem } from "../client-types";

export function OutlookObjectEditor(props: {
  copy: ClientCopy;
  section: Section;
  taskLists: ClientTaskList[];
  currentTask: TaskItem | null | undefined;
  taskForm: TaskDraft;
  setTaskForm: React.Dispatch<React.SetStateAction<TaskDraft>>;
  currentNote: NoteItem | null | undefined;
  noteForm: NoteDraft;
  setNoteForm: React.Dispatch<React.SetStateAction<NoteDraft>>;
  currentJournalEntry: JournalEntryItem | null | undefined;
  journalEntryForm: JournalEntryDraft;
  setJournalEntryForm: React.Dispatch<React.SetStateAction<JournalEntryDraft>>;
  currentReminder: ReminderItem | null | undefined;
  onNewTask: () => void;
  onSaveTask: () => void;
  onDeleteTask: () => void;
  onNewNote: () => void;
  onSaveNote: () => void;
  onDeleteNote: () => void;
  onNewJournalEntry: () => void;
  onSaveJournalEntry: () => void;
  onDeleteJournalEntry: () => void;
}) {
  if (props.section === "tasks") {
    return (
      <section className="editor-panel">
        <Header title={props.currentTask ? props.copy.objectEditor.tasks.edit : props.copy.objectEditor.tasks.create} onNew={props.onNewTask} newLabel={props.copy.objectEditor.tasks.new} />
        <label className="field"><span>{props.copy.objectFields.taskList}</span><Select value={props.taskForm.taskListId ?? ""} onChange={(event) => props.setTaskForm((current) => ({ ...current, taskListId: event.target.value || null }))}>{props.taskLists.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</Select></label>
        <label className="field"><span>{props.copy.objectFields.title}</span><Input value={props.taskForm.title} onChange={(event) => props.setTaskForm((current) => ({ ...current, title: event.target.value }))} /></label>
        <label className="field"><span>{props.copy.objectFields.status}</span><Select value={props.taskForm.status} onChange={(event) => props.setTaskForm((current) => ({ ...current, status: event.target.value }))}><option value="needs-action">needs-action</option><option value="in-process">in-process</option><option value="completed">completed</option><option value="cancelled">cancelled</option></Select></label>
        <label className="field"><span>{props.copy.objectFields.dueAt}</span><Input type="datetime-local" value={toLocalInput(props.taskForm.dueAt)} onChange={(event) => props.setTaskForm((current) => ({ ...current, dueAt: fromLocalInput(event.target.value) }))} /></label>
        <label className="field"><span>{props.copy.objectFields.body}</span><Textarea rows={8} value={props.taskForm.description} onChange={(event) => props.setTaskForm((current) => ({ ...current, description: event.target.value }))} /></label>
        <Actions saveLabel={props.copy.objectEditor.save} deleteLabel={props.copy.objectEditor.delete} onSave={props.onSaveTask} onDelete={props.currentTask ? props.onDeleteTask : undefined} />
      </section>
    );
  }
  if (props.section === "notes") {
    return (
      <section className="editor-panel">
        <Header title={props.currentNote ? props.copy.objectEditor.notes.edit : props.copy.objectEditor.notes.create} onNew={props.onNewNote} newLabel={props.copy.objectEditor.notes.new} />
        <label className="field"><span>{props.copy.objectFields.title}</span><Input value={props.noteForm.title} onChange={(event) => props.setNoteForm((current) => ({ ...current, title: event.target.value }))} /></label>
        <label className="field"><span>{props.copy.objectFields.color}</span><Select value={props.noteForm.color} onChange={(event) => props.setNoteForm((current) => ({ ...current, color: event.target.value }))}><option value="yellow">yellow</option><option value="blue">blue</option><option value="green">green</option><option value="pink">pink</option><option value="white">white</option></Select></label>
        <label className="field"><span>{props.copy.objectFields.categories}</span><Input value={props.noteForm.categoriesJson} onChange={(event) => props.setNoteForm((current) => ({ ...current, categoriesJson: event.target.value }))} /></label>
        <label className="field"><span>{props.copy.objectFields.body}</span><Textarea rows={10} value={props.noteForm.bodyText} onChange={(event) => props.setNoteForm((current) => ({ ...current, bodyText: event.target.value }))} /></label>
        <Actions saveLabel={props.copy.objectEditor.save} deleteLabel={props.copy.objectEditor.delete} onSave={props.onSaveNote} onDelete={props.currentNote ? props.onDeleteNote : undefined} />
      </section>
    );
  }
  if (props.section === "journal") {
    return (
      <section className="editor-panel">
        <Header title={props.currentJournalEntry ? props.copy.objectEditor.journal.edit : props.copy.objectEditor.journal.create} onNew={props.onNewJournalEntry} newLabel={props.copy.objectEditor.journal.new} />
        <label className="field"><span>{props.copy.objectFields.subject}</span><Input value={props.journalEntryForm.subject} onChange={(event) => props.setJournalEntryForm((current) => ({ ...current, subject: event.target.value }))} /></label>
        <label className="field"><span>{props.copy.objectFields.type}</span><Input value={props.journalEntryForm.entryType} onChange={(event) => props.setJournalEntryForm((current) => ({ ...current, entryType: event.target.value }))} /></label>
        <label className="field"><span>{props.copy.objectFields.startsAt}</span><Input type="datetime-local" value={toLocalInput(props.journalEntryForm.startsAt)} onChange={(event) => props.setJournalEntryForm((current) => ({ ...current, startsAt: fromLocalInput(event.target.value) }))} /></label>
        <label className="field"><span>{props.copy.objectFields.endsAt}</span><Input type="datetime-local" value={toLocalInput(props.journalEntryForm.endsAt)} onChange={(event) => props.setJournalEntryForm((current) => ({ ...current, endsAt: fromLocalInput(event.target.value) }))} /></label>
        <label className="field"><span>{props.copy.objectFields.body}</span><Textarea rows={8} value={props.journalEntryForm.bodyText} onChange={(event) => props.setJournalEntryForm((current) => ({ ...current, bodyText: event.target.value }))} /></label>
        <Actions saveLabel={props.copy.objectEditor.save} deleteLabel={props.copy.objectEditor.delete} onSave={props.onSaveJournalEntry} onDelete={props.currentJournalEntry ? props.onDeleteJournalEntry : undefined} />
      </section>
    );
  }
  return (
    <section className="editor-panel">
      <div className="editor-header"><div><p className="pane-kicker">{props.copy.sections.reminders}</p><h3>{props.currentReminder?.title ?? props.copy.emptyObjects.reminders}</h3></div></div>
      {props.currentReminder ? <div className="list"><div className="row"><strong>{props.copy.objectFields.type}</strong><span>{props.currentReminder.sourceType}</span></div><div className="row"><strong>{props.copy.objectFields.status}</strong><span>{props.currentReminder.status}</span></div><div className="row"><strong>{props.copy.objectFields.reminderAt}</strong><span>{props.currentReminder.reminderAt}</span></div><div className="row"><strong>{props.copy.objectFields.dueAt}</strong><span>{props.currentReminder.dueAt ?? props.copy.noDate}</span></div></div> : null}
    </section>
  );
}

function Header(props: { title: string; newLabel: string; onNew: () => void }) {
  return <div className="editor-header"><div><p className="pane-kicker">{props.newLabel}</p><h3>{props.title}</h3></div><Button variant="ghost" type="button" onClick={props.onNew}>{props.newLabel}</Button></div>;
}

function Actions(props: { saveLabel: string; deleteLabel: string; onSave: () => void; onDelete?: () => void }) {
  return <div className="editor-actions"><Button variant="primary" type="button" onClick={props.onSave}>{props.saveLabel}</Button>{props.onDelete ? <Button variant="ghost" type="button" onClick={props.onDelete}>{props.deleteLabel}</Button> : null}</div>;
}

function toLocalInput(value: string | null) {
  return value ? value.slice(0, 16) : "";
}

function fromLocalInput(value: string) {
  return value ? `${value}:00Z` : null;
}
