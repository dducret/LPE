---
type: Rust Method
title: load_cleanup_rows
resource: crates/lpe-storage/src/storage_visibility.rs#L598-L645
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_visibility/pool_reference_from_columns
  - functions/crates/lpe-storage/src/storage_visibility/summarize_error
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup
---

# Signature

`async fn load_cleanup_rows(&self, tenant_filter: Option<Uuid>) -> Result<Vec<CleanupRow>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool_reference_from_columns](../../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_reference_from_columns.md)
- [summarize_error](../../../../../../functions/crates/lpe-storage/src/storage_visibility/summarize_error.md)

# Called by

- [fetch_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)