---
type: Rust Function
title: journal_entry_properties
resource: crates/lpe-jmap/src/notes_journal.rs#L600-L621
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get
---

# Signature

`fn journal_entry_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_journal_entry_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get.md)