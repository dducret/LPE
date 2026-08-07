---
type: Rust Function
title: require_attachment_mailbox_access
resource: crates/lpe-admin-api/src/client_attachments.rs#L105-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_attachments/authorize_attachment_mailbox_access
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment
  - functions/crates/lpe-admin-api/src/client_attachments/download_message_attachment
---

# Signature

`async fn require_attachment_mailbox_access( storage: &Storage, account: &AuthenticatedAccount, target_account_id: Uuid, write_required: bool, ) -> Result<MailboxAccountAccess, (StatusCode, String)>`

# Calls

- [authorize_attachment_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/client_attachments/authorize_attachment_mailbox_access.md)

# Called by

- [upload_draft_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment.md)
- [download_message_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/download_message_attachment.md)