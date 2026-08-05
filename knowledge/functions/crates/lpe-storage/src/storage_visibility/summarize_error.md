---
type: Rust Function
title: summarize_error
resource: crates/lpe-storage/src/storage_visibility.rs#L955-L966
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows
  - functions/crates/lpe-storage/src/storage_visibility/tests/long_errors_are_summarized
---

# Signature

`fn summarize_error(error: Option<String>) -> Option<String>`

# Called by

- [fetch_storage_migrations](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)
- [load_cleanup_rows](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows.md)
- [long_errors_are_summarized](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/long_errors_are_summarized.md)