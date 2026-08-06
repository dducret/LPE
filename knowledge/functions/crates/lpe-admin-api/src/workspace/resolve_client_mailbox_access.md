---
type: Rust Function
title: resolve_client_mailbox_access
resource: crates/lpe-admin-api/src/workspace.rs#L1366-L1382
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/submit_message_with_store
  - functions/crates/lpe-admin-api/src/workspace/save_draft_message
---

# Signature

`async fn resolve_client_mailbox_access<S: ClientSubmissionStore>( storage: &S, account: &AuthenticatedAccount, requested_account_id: Uuid, ) -> std::result::Result<MailboxAccountAccess, (StatusCode, String)>`

# Called by

- [submit_message_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)
- [save_draft_message](../../../../../functions/crates/lpe-admin-api/src/workspace/save_draft_message.md)