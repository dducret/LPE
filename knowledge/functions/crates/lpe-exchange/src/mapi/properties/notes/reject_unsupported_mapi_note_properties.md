---
type: Rust Function
title: reject_unsupported_mapi_note_properties
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L303-L320
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values
---

# Signature

`fn reject_unsupported_mapi_note_properties(properties: &HashMap<u32, MapiValue>) -> Result<()>`

# Called by

- [apply_canonical_note_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values.md)