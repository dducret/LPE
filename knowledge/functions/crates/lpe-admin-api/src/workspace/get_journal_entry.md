---
type: Rust Function
title: get_journal_entry
resource: crates/lpe-admin-api/src/workspace.rs#L1049-L1057
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/get_journal_entry_with_store
---

# Signature

`pub(crate) async fn get_journal_entry( State(storage): State<Storage>, headers: HeaderMap, AxumPath(entry_id): AxumPath<Uuid>, ) -> ApiResult<JournalEntry>`

# Calls

- [get_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_journal_entry_with_store.md)