---
type: Rust Function
title: client_workspace
resource: crates/lpe-admin-api/src/workspace.rs#L300-L318
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access
  - functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_read_access
  - functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace
---

# Signature

`pub(crate) async fn client_workspace( State(storage): State<Storage>, headers: HeaderMap, Query(query): Query<ClientWorkspaceQuery>, ) -> ApiResult<ClientWorkspace>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [require_mailbox_account_access](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access.md)
- [ensure_client_mailbox_read_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_read_access.md)
- [fetch_client_workspace](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace.md)