---
type: Rust Function
title: apply_conversation_actions_to_new_message
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L129-L154
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn apply_conversation_actions_to_new_message<S>( store: &S, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], email: &JmapEmail, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [conversation_action_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_messages.md)
- [apply_conversation_action_to_existing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)