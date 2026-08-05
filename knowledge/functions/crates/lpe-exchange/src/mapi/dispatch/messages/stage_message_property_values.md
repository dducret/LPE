---
type: Rust Function
title: stage_message_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L344-L379
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) fn stage_message_property_values( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, values: Vec<(u32, MapiValue)>, ) -> Result<()>`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [message_followup_update_from_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [apply_mapi_property_values_to_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)