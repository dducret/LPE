---
type: Rust Function
title: get_client_task
resource: crates/lpe-admin-api/src/workspace.rs#L960-L974
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids
---

# Signature

`pub(crate) async fn get_client_task( State(storage): State<Storage>, headers: HeaderMap, AxumPath(task_id): AxumPath<Uuid>, ) -> ApiResult<ClientTask>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_client_tasks_by_ids](../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids.md)