---
type: Rust Function
title: merged_draft_input
resource: crates/lpe-activesync/src/message.rs#L339-L415
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

`pub(crate) fn merged_draft_input( principal: &AuthenticatedPrincipal, mailbox_access: &MailboxAccountAccess, draft_id: Uuid, existing: &JmapEmail, application_data: &WbxmlNode, ) -> SubmitMessageInput`

# Calls

- [default_sender](../../../../../functions/crates/lpe-activesync/src/message/default_sender.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [apply_draft_sync_commands](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)