---
type: Rust Function
title: stage_virtual_conversation_action_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L226-L260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_set_properties_is_rejected
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/virtual_default_conversation_action_set_rejects_wrong_folder
---

# Signature

`pub(super) fn stage_virtual_conversation_action_property_values( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, values: Vec<(u32, MapiValue)>, ) -> Option<Result<()>>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [is_outlook_default_conversation_action_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [conversation_action_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)
- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [apply_mapi_property_values_to_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [unadvertised_default_conversation_action_set_properties_is_rejected](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_set_properties_is_rejected.md)
- [virtual_default_conversation_action_set_rejects_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/virtual_default_conversation_action_set_rejects_wrong_folder.md)