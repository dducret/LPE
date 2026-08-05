---
type: Rust Method
title: conversation_action_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1441-L1448
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
---

# Signature

`pub(crate) fn conversation_action_message_for_id( &self, item_id: u64, ) -> Option<&MapiConversationActionMessage>`

# Called by

- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [debug_object_scope_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [conversation_action_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)