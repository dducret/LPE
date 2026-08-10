---
type: TypeScript Function
title: MasterPane
resource: web/client/src/components/MasterPane.tsx#L6-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/client/src/components/MasterPane/taskStatusLabel
---

# Signature

`function MasterPane(props: { copy: ClientCopy; section: Section; folder: Folder; folderLabel: string; contactBook: ContactBookId; setContactBook: (contactBook: ContactBookId) => void; contactBooks: CollaborationCollection[]; calendarCollectionId: string; setCalendarCollectionId: (collectionId: string) => void; calendarCollections: CollaborationCollection[]; mode: Mode; filteredMessages: Message[]; events: EventItem[]; contacts: ContactItem[]; tasks: TaskItem[]; notes: NoteItem[]; journalEntries: JournalEntryItem[]; reminders: ReminderItem[]; messageId: string; eventId: string; contactId: string; taskId: string; noteId: string; journalEntryId: string; reminderId: string; onSelectMessage: (id: string) => void; onSelectEvent: (id: string) => void; onSelectContact: (id: string) => void; onSelectTask: (id: string) => void; onSelectNote: (id: string) => void; onSelectJournalEntry: (id: string) => void; onSelectReminder: (id: string) => void; })`

# Calls

- [taskStatusLabel](../../../../../../functions/web/client/src/components/MasterPane/taskStatusLabel.md)