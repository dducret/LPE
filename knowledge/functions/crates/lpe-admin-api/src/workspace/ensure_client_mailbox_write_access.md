---
type: Rust Function
title: ensure_client_mailbox_write_access
resource: crates/lpe-admin-api/src/workspace.rs#L1400-L1411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/save_draft_message
---

# Signature

`fn ensure_client_mailbox_write_access( mailbox_access: &MailboxAccountAccess, ) -> std::result::Result<(), (StatusCode, String)>`

# Called by

- [save_draft_message](../../../../../functions/crates/lpe-admin-api/src/workspace/save_draft_message.md)