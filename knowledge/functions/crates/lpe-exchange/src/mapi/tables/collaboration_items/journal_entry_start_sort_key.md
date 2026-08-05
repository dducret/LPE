---
type: Rust Function
title: journal_entry_start_sort_key
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L30-L36
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries
---

# Signature

`pub(in crate::mapi) fn journal_entry_start_sort_key(entry: &JournalEntry) -> &str`

# Called by

- [sort_journal_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_journal_entries.md)