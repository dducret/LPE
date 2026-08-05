---
type: Rust Function
title: upsert_client_task
resource: crates/lpe-admin-api/src/workspace.rs#L953-L977
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn upsert_client_task( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertClientTaskRequest>, ) -> ApiResult<ClientTask>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)