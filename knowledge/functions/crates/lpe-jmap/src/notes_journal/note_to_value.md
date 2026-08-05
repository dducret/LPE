---
type: Rust Function
title: note_to_value
resource: crates/lpe-jmap/src/notes_journal.rs#L623-L639
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get
---

# Signature

`fn note_to_value(note: &ClientNote, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_note_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get.md)