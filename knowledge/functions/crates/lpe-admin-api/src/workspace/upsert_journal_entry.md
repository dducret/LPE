---
type: Rust Function
title: upsert_journal_entry
resource: crates/lpe-admin-api/src/workspace.rs#L1094-L1102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry_with_store
---

# Signature

`pub(crate) async fn upsert_journal_entry( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertJournalEntryRequest>, ) -> ApiResult<JournalEntry>`

# Calls

- [upsert_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry_with_store.md)