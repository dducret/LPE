---
type: Rust Function
title: delete_client_note_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1223-L1233
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/delete_client_note
  - functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn delete_client_note_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, note_id: Uuid, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [delete_client_note](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_note.md)
- [notes_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path.md)