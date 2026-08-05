---
type: Rust Module
title: notes_journal
resource: crates/lpe-jmap/src/notes_journal.rs#L1-L826
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-clientnote-clientreminder-journalentry-reminderquery-upsertclientnoteinput-upsertjournalentryinput
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-convert-insert-if-error-set-error-parse-parse-optional-string-parse-required-string-parse-uuid-parse-uuid-list-protocol-changesarguments-journalentrygetarguments-journalentryqueryarguments-journalentryqueryfilter-journalentrysetarguments-notegetarguments-notequeryarguments-notequeryfilter-notesetarguments-querychangesarguments-reminderqueryarguments-state-encode-query-state-encode-query-state-reference-query-changes-response-query-position-jmapservice-default-get-limit-max-query-limit
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_note_get](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get.md)
- [handle_note_query](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query.md)
- [handle_note_query_changes](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes.md)
- [handle_note_changes](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes.md)
- [handle_note_set](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_get](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get.md)
- [handle_journal_entry_query](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query.md)
- [handle_journal_entry_query_changes](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes.md)
- [handle_journal_entry_changes](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes.md)
- [handle_journal_entry_set](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [handle_reminder_query](../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query.md)
- [note_properties](../../../../functions/crates/lpe-jmap/src/notes_journal/note_properties.md)
- [journal_entry_properties](../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_properties.md)
- [note_to_value](../../../../functions/crates/lpe-jmap/src/notes_journal/note_to_value.md)
- [journal_entry_to_value](../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_to_value.md)
- [note_state_fingerprint](../../../../functions/crates/lpe-jmap/src/notes_journal/note_state_fingerprint.md)
- [journal_entry_state_fingerprint](../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_state_fingerprint.md)
- [reminder_state_fingerprint](../../../../functions/crates/lpe-jmap/src/notes_journal/reminder_state_fingerprint.md)
- [reminder_to_value](../../../../functions/crates/lpe-jmap/src/notes_journal/reminder_to_value.md)
- [reminder_id](../../../../functions/crates/lpe-jmap/src/notes_journal/reminder_id.md)
- [note_matches_filter](../../../../functions/crates/lpe-jmap/src/notes_journal/note_matches_filter.md)
- [journal_entry_matches_filter](../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_matches_filter.md)
- [parse_note_input](../../../../functions/crates/lpe-jmap/src/notes_journal/parse_note_input.md)
- [parse_journal_entry_input](../../../../functions/crates/lpe-jmap/src/notes_journal/parse_journal_entry_input.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::{
    ClientNote, ClientReminder, JournalEntry, ReminderQuery, UpsertClientNoteInput,
    UpsertJournalEntryInput,
}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, JournalEntryGetArguments, JournalEntryQueryArguments,
        JournalEntryQueryFilter, JournalEntrySetArguments, NoteGetArguments, NoteQueryArguments,
        NoteQueryFilter, NoteSetArguments, QueryChangesArguments, ReminderQueryArguments,
    },
    state::{
        encode_query_state, encode_query_state_reference, query_changes_response, query_position,
    },
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)