---
type: Rust Function
title: parse_swapped_todo_data
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L922-L972
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_u32
  - functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_utf16z
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/message/swapped_todo_minutes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/swapped_todo_data_parser_accepts_documented_layout
---

# Signature

`pub(in crate::mapi) fn parse_swapped_todo_data(bytes: &[u8]) -> Result<SwappedToDoData>`

# Calls

- [read_swapped_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_u32.md)
- [read_swapped_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_utf16z.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [swapped_todo_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/swapped_todo_minutes.md)

# Called by

- [message_followup_update_from_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [swapped_todo_data_parser_accepts_documented_layout](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/swapped_todo_data_parser_accepts_documented_layout.md)