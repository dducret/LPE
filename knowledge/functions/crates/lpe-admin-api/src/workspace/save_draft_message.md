---
type: Rust Function
title: save_draft_message
resource: crates/lpe-admin-api/src/workspace.rs#L376-L399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/workspace/mailbox_access/resolve_client_mailbox_access
  - functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_write_access
  - functions/crates/lpe-admin-api/src/workspace/map_submit_message_request
---

# Signature

`pub(crate) async fn save_draft_message( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<SubmitMessageRequest>, ) -> ApiResult<SavedDraftMessage>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [resolve_client_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/resolve_client_mailbox_access.md)
- [ensure_client_mailbox_write_access](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/ensure_client_mailbox_write_access.md)
- [map_submit_message_request](../../../../../functions/crates/lpe-admin-api/src/workspace/map_submit_message_request.md)