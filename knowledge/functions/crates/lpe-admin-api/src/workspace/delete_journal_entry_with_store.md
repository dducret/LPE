---
type: Rust Function
title: delete_journal_entry_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1287-L1297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/delete_journal_entry
  - functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn delete_journal_entry_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, entry_id: Uuid, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [delete_journal_entry](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_journal_entry.md)
- [journal_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path.md)