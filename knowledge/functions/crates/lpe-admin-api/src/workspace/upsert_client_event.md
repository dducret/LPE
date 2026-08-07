---
type: Rust Function
title: upsert_client_event
resource: crates/lpe-admin-api/src/workspace.rs#L835-L907
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-admin-api/src/workspace/preserve_empty
---

# Signature

`pub(crate) async fn upsert_client_event( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertClientEventRequest>, ) -> ApiResult<ClientEvent>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [preserve_empty](../../../../../functions/crates/lpe-admin-api/src/workspace/preserve_empty.md)