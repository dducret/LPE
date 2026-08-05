---
type: Rust Function
title: delete_client_task
resource: crates/lpe-admin-api/src/workspace.rs#L979-L994
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_client_task( State(storage): State<Storage>, headers: HeaderMap, AxumPath(task_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)