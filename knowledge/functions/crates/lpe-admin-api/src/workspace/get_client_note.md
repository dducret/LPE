---
type: Rust Function
title: get_client_note
resource: crates/lpe-admin-api/src/workspace.rs#L1005-L1013
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/get_client_note_with_store
---

# Signature

`pub(crate) async fn get_client_note( State(storage): State<Storage>, headers: HeaderMap, AxumPath(note_id): AxumPath<Uuid>, ) -> ApiResult<ClientNote>`

# Calls

- [get_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_note_with_store.md)