---
type: Rust Function
title: upsert_mailbox_delegation_grant
resource: crates/lpe-admin-api/src/delegation.rs#L316-L339
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn upsert_mailbox_delegation_grant( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertMailboxDelegationGrantRequest>, ) -> ApiResult<lpe_storage::MailboxDelegationGrant>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)