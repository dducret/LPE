---
type: Rust Function
title: list_search_folders_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1281-L1290
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_search_folders
  - functions/crates/lpe-admin-api/src/workspace/tests/search_folder_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn list_search_folders_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, ) -> std::result::Result<Vec<SearchFolderDefinition>, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [list_search_folders](../../../../../functions/crates/lpe-admin-api/src/workspace/list_search_folders.md)
- [search_folder_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/search_folder_api_helpers_cover_authenticated_crud_path.md)