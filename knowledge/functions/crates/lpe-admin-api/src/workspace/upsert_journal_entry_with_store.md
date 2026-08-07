---
type: Rust Function
title: upsert_journal_entry_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1242-L1266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry
  - functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path
---

# Signature

`async fn upsert_journal_entry_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, request: UpsertJournalEntryRequest, ) -> std::result::Result<JournalEntry, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [upsert_journal_entry](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry.md)
- [journal_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path.md)