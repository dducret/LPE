---
type: Rust Method
title: conversation_action_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1481-L1483
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message
---

# Signature

`pub(crate) fn conversation_action_messages(&self) -> &[MapiConversationActionMessage]`

# Called by

- [apply_conversation_actions_to_new_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message.md)