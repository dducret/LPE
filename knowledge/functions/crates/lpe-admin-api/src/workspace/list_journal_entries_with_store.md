---
type: Rust Function
title: list_journal_entries_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1200-L1209
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_journal_entries
  - functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn list_journal_entries_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, ) -> std::result::Result<Vec<JournalEntry>, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [list_journal_entries](../../../../../functions/crates/lpe-admin-api/src/workspace/list_journal_entries.md)
- [journal_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path.md)