---
type: Rust Function
title: delete_mailbox_delegation_grant
resource: crates/lpe-admin-api/src/delegation.rs#L341-L363
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_mailbox_delegation_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath(grantee_account_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)