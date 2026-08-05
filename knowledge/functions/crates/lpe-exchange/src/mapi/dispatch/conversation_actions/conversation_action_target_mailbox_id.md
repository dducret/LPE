---
type: Rust Function
title: conversation_action_target_mailbox_id
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L176-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn conversation_action_target_mailbox_id( action: &lpe_storage::ConversationAction, mailboxes: &[JmapMailbox], ) -> Option<Uuid>`

# Calls

- [conversation_action_target_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox.md)

# Called by

- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)