---
type: Rust Function
title: delete_task_list_grant
resource: crates/lpe-admin-api/src/delegation.rs#L225-L249
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_task_list_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath((task_list_id, grantee_account_id)): AxumPath<(Uuid, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)