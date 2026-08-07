---
type: Rust Function
title: delete_draft_message
resource: crates/lpe-admin-api/src/workspace.rs#L503-L533
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access
  - functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_write_access
---

# Signature

`pub(crate) async fn delete_draft_message( State(storage): State<Storage>, headers: HeaderMap, AxumPath(message_id): AxumPath<Uuid>, Query(query): Query<ClientWorkspaceQuery>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [require_mailbox_account_access](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access.md)
- [ensure_client_mailbox_write_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_write_access.md)