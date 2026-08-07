---
type: Rust Function
title: ensure_client_mailbox_read_access
resource: crates/lpe-admin-api/src/workspace/mailbox_access.rs#L12-L23
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/client_workspace
---

# Signature

`pub(crate) fn ensure_client_mailbox_read_access( mailbox_access: &MailboxAccountAccess, ) -> std::result::Result<(), (StatusCode, String)>`

# Called by

- [client_workspace](../../../../../../functions/crates/lpe-admin-api/src/workspace/client_workspace.md)