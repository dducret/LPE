---
type: Rust Module
title: mailbox_access
resource: crates/lpe-admin-api/src/workspace/mailbox_access.rs#L1-L65
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-statuscode
  - external/lpe-storage-authenticatedaccount-mailboxaccountaccess
  - external/serde-deserialize
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [ClientWorkspaceQuery](../../../../../classes/crates/lpe-admin-api/src/workspace/mailbox_access/ClientWorkspaceQuery.md)
- [ensure_client_mailbox_read_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_read_access.md)
- [classify_client_mailbox_access_error](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/classify_client_mailbox_access_error.md)
- [resolve_client_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/resolve_client_mailbox_access.md)
- [ensure_client_mailbox_write_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_write_access.md)

# Imports

- `axum::http::StatusCode`
- `lpe_storage::{AuthenticatedAccount, MailboxAccountAccess}`
- `serde::Deserialize`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../../packages/crates/lpe-admin-api.md)