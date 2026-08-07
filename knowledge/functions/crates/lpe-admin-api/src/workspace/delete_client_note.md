---
type: Rust Function
title: delete_client_note
resource: crates/lpe-admin-api/src/workspace.rs#L1062-L1073
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/delete_client_note_with_store
---

# Signature

`pub(crate) async fn delete_client_note( State(storage): State<Storage>, headers: HeaderMap, AxumPath(note_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [delete_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_note_with_store.md)