---
type: Rust Function
title: list_client_tasks
resource: crates/lpe-admin-api/src/workspace.rs#L911-L922
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks
---

# Signature

`pub(crate) async fn list_client_tasks( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<ClientTask>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_client_tasks](../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks.md)