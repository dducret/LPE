---
type: Rust Function
title: upsert_collaboration_grant
resource: crates/lpe-admin-api/src/delegation.rs#L73-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn upsert_collaboration_grant( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertCollaborationGrantRequest>, ) -> ApiResult<lpe_storage::CollaborationGrant>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)