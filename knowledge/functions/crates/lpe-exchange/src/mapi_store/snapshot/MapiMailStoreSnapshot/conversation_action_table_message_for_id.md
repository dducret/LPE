---
type: Rust Method
title: conversation_action_table_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1498-L1503
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`pub(crate) fn conversation_action_table_message_for_id( &self, item_id: u64, ) -> Option<MapiConversationActionMessage>`

# Calls

- [conversation_action_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id.md)

# Called by

- [conversation_action_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open.md)
- [stage_virtual_conversation_action_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_virtual_conversation_action_property_delete](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)