---
type: Rust Function
title: delete_sender_delegation_grant
resource: crates/lpe-admin-api/src/delegation.rs#L392-L416
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/util/parse_sender_delegation_right
---

# Signature

`pub(crate) async fn delete_sender_delegation_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath((sender_right, grantee_account_id)): AxumPath<(String, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [parse_sender_delegation_right](../../../../../functions/crates/lpe-admin-api/src/util/parse_sender_delegation_right.md)