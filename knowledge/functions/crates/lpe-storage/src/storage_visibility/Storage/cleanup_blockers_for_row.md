---
type: Rust Method
title: cleanup_blockers_for_row
resource: crates/lpe-storage/src/storage_visibility.rs#L647-L742
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_visibility/cleanup_blocker_labels
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup
---

# Signature

`async fn cleanup_blockers_for_row(&self, row: &CleanupRow) -> Result<Vec<String>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [cleanup_blocker_labels](../../../../../../functions/crates/lpe-storage/src/storage_visibility/cleanup_blocker_labels.md)

# Called by

- [fetch_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)