---
type: Rust Function
title: json_from_mapi_multi_string_value
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L205-L216
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi
---

# Signature

`fn json_from_mapi_multi_string_value( properties: &HashMap<u32, MapiValue>, tag: u32, ) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [json_from_mapi_multi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string.md)
- [journal_entry_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi.md)