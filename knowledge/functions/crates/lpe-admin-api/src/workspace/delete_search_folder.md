---
type: Rust Function
title: delete_search_folder
resource: crates/lpe-admin-api/src/workspace.rs#L1119-L1130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/delete_search_folder_with_store
---

# Signature

`pub(crate) async fn delete_search_folder( State(storage): State<Storage>, headers: HeaderMap, AxumPath(search_folder_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [delete_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_search_folder_with_store.md)