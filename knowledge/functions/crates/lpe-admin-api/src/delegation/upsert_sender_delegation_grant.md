---
type: Rust Function
title: upsert_sender_delegation_grant
resource: crates/lpe-admin-api/src/delegation.rs#L365-L390
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/util/parse_sender_delegation_right
---

# Signature

`pub(crate) async fn upsert_sender_delegation_grant( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertSenderDelegationGrantRequest>, ) -> ApiResult<SenderDelegationGrant>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [parse_sender_delegation_right](../../../../../functions/crates/lpe-admin-api/src/util/parse_sender_delegation_right.md)