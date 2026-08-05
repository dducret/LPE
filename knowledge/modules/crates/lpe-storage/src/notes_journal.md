---
type: Rust Module
title: notes_journal
resource: crates/lpe-storage/src/notes_journal.rs#L1-L878
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-serialize
  - external/serde-json-json
  - external/uuid-uuid
  - external/crate-canonicalchangecategory-clientnoterow-clientreminderrow-journalentryrow-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ClientNote](../../../../classes/crates/lpe-storage/src/notes_journal/ClientNote.md)
- [UpsertClientNoteInput](../../../../classes/crates/lpe-storage/src/notes_journal/UpsertClientNoteInput.md)
- [JournalEntry](../../../../classes/crates/lpe-storage/src/notes_journal/JournalEntry.md)
- [UpsertJournalEntryInput](../../../../classes/crates/lpe-storage/src/notes_journal/UpsertJournalEntryInput.md)
- [ClientReminder](../../../../classes/crates/lpe-storage/src/notes_journal/ClientReminder.md)
- [ReminderQuery](../../../../classes/crates/lpe-storage/src/notes_journal/ReminderQuery.md)
- [map_note](../../../../functions/crates/lpe-storage/src/notes_journal/map_note.md)
- [map_journal_entry](../../../../functions/crates/lpe-storage/src/notes_journal/map_journal_entry.md)
- [map_reminder](../../../../functions/crates/lpe-storage/src/notes_journal/map_reminder.md)
- [dismiss_reminder_occurrence](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/dismiss_reminder_occurrence.md)
- [fetch_client_notes](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_client_notes.md)
- [fetch_client_notes_by_ids](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_client_notes_by_ids.md)
- [upsert_client_note](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note.md)
- [delete_client_note](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_client_note.md)
- [fetch_journal_entries](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries.md)
- [fetch_journal_entries_by_ids](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries_by_ids.md)
- [upsert_journal_entry](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry.md)
- [delete_journal_entry](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_journal_entry.md)
- [query_client_reminders](../../../../functions/crates/lpe-storage/src/notes_journal/Storage/query_client_reminders.md)
- [journal_select_sql](../../../../functions/crates/lpe-storage/src/notes_journal/journal_select_sql.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::Serialize`
- `serde_json::json`
- `uuid::Uuid`
- `crate::{CanonicalChangeCategory, ClientNoteRow, ClientReminderRow, JournalEntryRow, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)