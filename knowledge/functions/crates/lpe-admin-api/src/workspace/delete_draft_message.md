---
type: Rust Function
title: delete_draft_message
resource: crates/lpe-admin-api/src/workspace.rs#L491-L514
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_draft_message( State(storage): State<Storage>, headers: HeaderMap, AxumPath(message_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)