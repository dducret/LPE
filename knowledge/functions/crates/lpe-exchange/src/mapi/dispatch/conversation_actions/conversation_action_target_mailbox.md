---
type: Rust Function
title: conversation_action_target_mailbox
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L156-L174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id
---

# Signature

`fn conversation_action_target_mailbox<'a>( action: &lpe_storage::ConversationAction, mailboxes: &'a [JmapMailbox], ) -> Option<&'a JmapMailbox>`

# Calls

- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)

# Called by

- [apply_conversation_action_to_existing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages.md)
- [conversation_action_target_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id.md)