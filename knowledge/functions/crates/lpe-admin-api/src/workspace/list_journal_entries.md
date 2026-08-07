---
type: Rust Function
title: list_journal_entries
resource: crates/lpe-admin-api/src/workspace.rs#L1056-L1063
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/list_journal_entries_with_store
---

# Signature

`pub(crate) async fn list_journal_entries( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<JournalEntry>>`

# Calls

- [list_journal_entries_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_journal_entries_with_store.md)