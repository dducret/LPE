---
type: Rust Function
title: message_followup_update_from_mapi_values
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L765-L877
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data
  - functions/crates/lpe-exchange/src/mapi/properties/message/categories_from_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values
---

# Signature

`pub(in crate::mapi) fn message_followup_update_from_mapi_values( values: Vec<(u32, MapiValue)>, ) -> Result<lpe_storage::JmapEmailFollowupUpdate>`

# Calls

- [into_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [parse_swapped_todo_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data.md)
- [categories_from_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/categories_from_mapi_value.md)

# Called by

- [stage_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values.md)
- [apply_canonical_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values.md)