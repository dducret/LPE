---
type: Rust Function
title: list_client_notes_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1143-L1152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_client_notes
  - functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn list_client_notes_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, ) -> std::result::Result<Vec<ClientNote>, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [list_client_notes](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_notes.md)
- [notes_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path.md)