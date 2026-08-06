---
type: Rust Function
title: upsert_client_note_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1169-L1186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_note
  - functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn upsert_client_note_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, request: UpsertClientNoteRequest, ) -> std::result::Result<ClientNote, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [upsert_client_note](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_note.md)
- [notes_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path.md)