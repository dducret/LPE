---
type: Rust Function
title: list_client_notes
resource: crates/lpe-admin-api/src/workspace.rs#L1033-L1040
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/list_client_notes_with_store
---

# Signature

`pub(crate) async fn list_client_notes( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<ClientNote>>`

# Calls

- [list_client_notes_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_notes_with_store.md)