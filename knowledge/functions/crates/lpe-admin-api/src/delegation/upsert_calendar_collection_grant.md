---
type: Rust Function
title: upsert_calendar_collection_grant
resource: crates/lpe-admin-api/src/delegation.rs#L104-L140
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/http/bad_request_error
---

# Signature

`pub(crate) async fn upsert_calendar_collection_grant( State(storage): State<Storage>, headers: HeaderMap, AxumPath(calendar_id): AxumPath<Uuid>, Json(request): Json<UpsertCollaborationGrantRequest>, ) -> ApiResult<lpe_storage::CollaborationGrant>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [bad_request_error](../../../../../functions/crates/lpe-admin-api/src/http/bad_request_error.md)