---
type: Rust Function
title: parse_journal_entry_input
resource: crates/lpe-jmap/src/notes_journal.rs#L802-L826
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  called_by:
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
---

# Signature

`fn parse_journal_entry_input( id: Option<Uuid>, account_id: Uuid, value: Value, ) -> Result<UpsertJournalEntryInput>`

# Calls

- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)

# Called by

- [handle_journal_entry_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)