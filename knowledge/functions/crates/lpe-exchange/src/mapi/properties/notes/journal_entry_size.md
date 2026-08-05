---
type: Rust Function
title: journal_entry_size
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L233-L242
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object
---

# Signature

`pub(in crate::mapi) fn journal_entry_size(entry: &JournalEntry) -> i64`

# Called by

- [journal_entry_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [journal_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object.md)