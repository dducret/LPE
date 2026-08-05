---
type: Rust Function
title: journal_entry_matches_filter
resource: crates/lpe-jmap/src/notes_journal.rs#L765-L781
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes
---

# Signature

`fn journal_entry_matches_filter(entry: &JournalEntry, filter: &JournalEntryQueryFilter) -> bool`

# Called by

- [handle_journal_entry_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query.md)
- [handle_journal_entry_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes.md)