---
type: Rust Function
title: upsert_search_folder
resource: crates/lpe-admin-api/src/workspace.rs#L1109-L1117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/upsert_search_folder_with_store
---

# Signature

`pub(crate) async fn upsert_search_folder( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertSearchFolderRequest>, ) -> ApiResult<SearchFolderDefinition>`

# Calls

- [upsert_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_search_folder_with_store.md)