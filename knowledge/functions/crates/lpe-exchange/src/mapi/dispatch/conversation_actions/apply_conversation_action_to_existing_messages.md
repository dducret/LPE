---
type: Rust Function
title: apply_conversation_action_to_existing_messages
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L56-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) async fn apply_conversation_action_to_existing_messages<S>( store: &S, principal: &AccountPrincipal, action: &lpe_storage::ConversationAction, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [conversation_action_target_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox.md)

# Called by

- [apply_conversation_actions_to_new_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message.md)
- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)