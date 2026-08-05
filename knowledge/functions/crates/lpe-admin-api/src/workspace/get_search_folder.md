---
type: Rust Function
title: get_search_folder
resource: crates/lpe-admin-api/src/workspace.rs#L1099-L1107
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/get_search_folder_with_store
---

# Signature

`pub(crate) async fn get_search_folder( State(storage): State<Storage>, headers: HeaderMap, AxumPath(search_folder_id): AxumPath<Uuid>, ) -> ApiResult<SearchFolderDefinition>`

# Calls

- [get_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_search_folder_with_store.md)