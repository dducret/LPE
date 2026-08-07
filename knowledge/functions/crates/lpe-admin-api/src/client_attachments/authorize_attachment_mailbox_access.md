---
type: Rust Function
title: authorize_attachment_mailbox_access
resource: crates/lpe-admin-api/src/client_attachments.rs#L118-L143
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access
  - functions/crates/lpe-admin-api/src/client_attachments/delegated_attachment_upload_requires_canonical_write_access
  - functions/crates/lpe-admin-api/src/client_attachments/attachment_access_rejects_a_mailbox_absent_from_canonical_grants
---

# Signature

`fn authorize_attachment_mailbox_access( accessible: Vec<MailboxAccountAccess>, target_account_id: Uuid, write_required: bool, ) -> Result<MailboxAccountAccess, (StatusCode, String)>`

# Called by

- [require_attachment_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access.md)
- [delegated_attachment_upload_requires_canonical_write_access](../../../../../functions/crates/lpe-admin-api/src/client_attachments/delegated_attachment_upload_requires_canonical_write_access.md)
- [attachment_access_rejects_a_mailbox_absent_from_canonical_grants](../../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_access_rejects_a_mailbox_absent_from_canonical_grants.md)