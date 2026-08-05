---
type: Rust Function
title: delete_collaboration_grant
resource: crates/lpe-admin-api/src/delegation.rs#L142-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_collaboration_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath((kind, grantee_account_id)): AxumPath<(String, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)