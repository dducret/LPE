---
type: Rust Function
title: journal_entry_state_fingerprint
resource: crates/lpe-jmap/src/notes_journal.rs#L696-L710
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`pub(crate) fn journal_entry_state_fingerprint(entry: &JournalEntry) -> String`

# Called by

- [object_state_entries](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)