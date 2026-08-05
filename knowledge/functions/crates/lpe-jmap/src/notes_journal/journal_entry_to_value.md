---
type: Rust Function
title: journal_entry_to_value
resource: crates/lpe-jmap/src/notes_journal.rs#L641-L682
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get
---

# Signature

`fn journal_entry_to_value(entry: &JournalEntry, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_journal_entry_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get.md)