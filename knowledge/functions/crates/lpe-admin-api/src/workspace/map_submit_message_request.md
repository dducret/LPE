---
type: Rust Function
title: map_submit_message_request
resource: crates/lpe-admin-api/src/workspace.rs#L1424-L1453
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/resolve_client_sender_fields
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/submit_message_with_store
  - functions/crates/lpe-admin-api/src/workspace/save_draft_message
  - functions/crates/lpe-admin-api/src/workspace/tests/map_submit_message_request_preserves_web_submission_source
---

# Signature

`fn map_submit_message_request( authenticated_account: &AuthenticatedAccount, mailbox_access: &MailboxAccountAccess, request: SubmitMessageRequest, ) -> SubmitMessageInput`

# Calls

- [resolve_client_sender_fields](../../../../../functions/crates/lpe-admin-api/src/workspace/resolve_client_sender_fields.md)

# Called by

- [submit_message_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)
- [save_draft_message](../../../../../functions/crates/lpe-admin-api/src/workspace/save_draft_message.md)
- [map_submit_message_request_preserves_web_submission_source](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/map_submit_message_request_preserves_web_submission_source.md)