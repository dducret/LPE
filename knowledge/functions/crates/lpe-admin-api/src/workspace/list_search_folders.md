---
type: Rust Function
title: list_search_folders
resource: crates/lpe-admin-api/src/workspace.rs#L1092-L1099
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/list_search_folders_with_store
---

# Signature

`pub(crate) async fn list_search_folders( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<SearchFolderDefinition>>`

# Calls

- [list_search_folders_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_search_folders_with_store.md)