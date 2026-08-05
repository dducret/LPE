---
type: Rust Function
title: upsert_task_list_grant
resource: crates/lpe-admin-api/src/delegation.rs#L195-L223
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn upsert_task_list_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath(task_list_id): AxumPath<Uuid>, Json(request): Json<UpsertTaskListGrantRequest>, ) -> ApiResult<lpe_storage::TaskListGrant>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)