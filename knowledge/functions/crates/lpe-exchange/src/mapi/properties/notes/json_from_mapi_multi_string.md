---
type: Rust Function
title: json_from_mapi_multi_string
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L218-L224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi
---

# Signature

`fn json_from_mapi_multi_string( properties: &HashMap<u32, MapiValue>, tag: u32, existing: &str, ) -> String`

# Calls

- [json_from_mapi_multi_string_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string_value.md)

# Called by

- [journal_entry_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi.md)