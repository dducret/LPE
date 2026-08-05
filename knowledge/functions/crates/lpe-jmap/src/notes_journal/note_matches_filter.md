---
type: Rust Function
title: note_matches_filter
resource: crates/lpe-jmap/src/notes_journal.rs#L752-L763
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes
---

# Signature

`fn note_matches_filter(note: &ClientNote, filter: &NoteQueryFilter) -> bool`

# Called by

- [handle_note_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query.md)
- [handle_note_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes.md)