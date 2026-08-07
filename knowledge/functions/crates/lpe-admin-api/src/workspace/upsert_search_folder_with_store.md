---
type: Rust Function
title: upsert_search_folder_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1323-L1341
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_search_folder
  - functions/crates/lpe-admin-api/src/workspace/tests/search_folder_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn upsert_search_folder_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, request: UpsertSearchFolderRequest, ) -> std::result::Result<SearchFolderDefinition, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [upsert_search_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_search_folder.md)
- [search_folder_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/search_folder_api_helpers_cover_authenticated_crud_path.md)