---
type: Rust Function
title: submit_message_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L318-L362
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  - functions/crates/lpe-admin-api/src/workspace/resolve_client_mailbox_access
  - functions/crates/lpe-admin-api/src/workspace/map_submit_message_request
  - functions/crates/lpe-admin-api/src/observability/record_mail_submission
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/submit_message
  - functions/crates/lpe-admin-api/src/workspace/tests/submit_message_handler_uses_canonical_submission_store_path
---

# Signature

`async fn submit_message_with_store<S: ClientSubmissionStore>( storage: &S, headers: &HeaderMap, request: SubmitMessageRequest, ) -> std::result::Result<SubmittedMessage, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)
- [resolve_client_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/workspace/resolve_client_mailbox_access.md)
- [map_submit_message_request](../../../../../functions/crates/lpe-admin-api/src/workspace/map_submit_message_request.md)
- [record_mail_submission](../../../../../functions/crates/lpe-admin-api/src/observability/record_mail_submission.md)

# Called by

- [submit_message](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message.md)
- [submit_message_handler_uses_canonical_submission_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/submit_message_handler_uses_canonical_submission_store_path.md)