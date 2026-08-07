---
type: Rust Function
title: upsert_client_note
resource: crates/lpe-admin-api/src/workspace.rs#L1033-L1041
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_note_with_store
---

# Signature

`pub(crate) async fn upsert_client_note( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertClientNoteRequest>, ) -> ApiResult<ClientNote>`

# Calls

- [upsert_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_note_with_store.md)