---
type: Rust Function
title: default_sender
resource: crates/lpe-activesync/src/message.rs#L464-L485
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/message/merged_draft_input
  - functions/crates/lpe-activesync/src/message/draft_input_from_application_data
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`pub(crate) fn default_sender( mailbox_access: &MailboxAccountAccess, principal: &AuthenticatedPrincipal, current_display: Option<String>, current_address: Option<String>, ) -> (Option<String>, Option<String>)`

# Called by

- [merged_draft_input](../../../../../functions/crates/lpe-activesync/src/message/merged_draft_input.md)
- [draft_input_from_application_data](../../../../../functions/crates/lpe-activesync/src/message/draft_input_from_application_data.md)
- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)