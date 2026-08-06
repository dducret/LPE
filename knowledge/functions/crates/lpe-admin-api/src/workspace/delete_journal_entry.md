---
type: Rust Function
title: delete_journal_entry
resource: crates/lpe-admin-api/src/workspace.rs#L1069-L1080
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/delete_journal_entry_with_store
---

# Signature

`pub(crate) async fn delete_journal_entry( State(storage): State<Storage>, headers: HeaderMap, AxumPath(entry_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [delete_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_journal_entry_with_store.md)