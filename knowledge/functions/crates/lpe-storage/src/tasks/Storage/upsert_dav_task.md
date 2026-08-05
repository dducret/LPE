---
type: Rust Method
title: upsert_dav_task
resource: crates/lpe-storage/src/tasks.rs#L1207-L1239
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn upsert_dav_task(&self, input: UpsertClientTaskInput) -> Result<DavTask>`

# Calls

- [fetch_task_lists_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)