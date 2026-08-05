---
type: Rust Function
title: reject_unsupported_mapi_journal_entry_properties
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L395-L430
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values
---

# Signature

`fn reject_unsupported_mapi_journal_entry_properties( properties: &HashMap<u32, MapiValue>, ) -> Result<()>`

# Called by

- [apply_canonical_journal_entry_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values.md)