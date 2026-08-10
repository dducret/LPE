---
type: Rust Function
title: conversation_action_subject
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L393-L404
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_size
  - functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object
---

# Signature

`pub(in crate::mapi) fn conversation_action_subject( action: &lpe_storage::ConversationAction, ) -> String`

# Called by

- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [conversation_action_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [conversation_action_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_size.md)
- [conversation_action_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)