---
type: Rust Function
title: delete_calendar_collection_grant
resource: crates/lpe-admin-api/src/delegation.rs#L169-L193
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_calendar_collection_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath((calendar_id, grantee_account_id)): AxumPath<(Uuid, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)