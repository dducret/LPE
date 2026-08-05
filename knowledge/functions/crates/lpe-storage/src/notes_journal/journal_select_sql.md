---
type: Rust Function
title: journal_select_sql
resource: crates/lpe-storage/src/notes_journal.rs#L857-L878
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries
  - functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries_by_ids
---

# Signature

`fn journal_select_sql(where_clause: &str) -> String`

# Called by

- [fetch_journal_entries](../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries.md)
- [fetch_journal_entries_by_ids](../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/fetch_journal_entries_by_ids.md)