---
type: Rust Function
title: list_client_task_lists
resource: crates/lpe-admin-api/src/workspace.rs#L924-L935
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists
---

# Signature

`pub(crate) async fn list_client_task_lists( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<ClientTaskList>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_task_lists](../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)