---
type: Rust Function
title: draft_input_from_application_data
resource: crates/lpe-activesync/src/message.rs#L417-L462
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/message/default_sender
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands
---

# Signature

`pub(crate) fn draft_input_from_application_data( principal: &AuthenticatedPrincipal, mailbox_access: &MailboxAccountAccess, draft_message_id: Option<Uuid>, application_data: &WbxmlNode, source: &str, ) -> SubmitMessageInput`

# Calls

- [default_sender](../../../../../functions/crates/lpe-activesync/src/message/default_sender.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [apply_draft_sync_commands](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)