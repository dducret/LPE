---
type: Rust Method
title: load_cleanup_counts
resource: crates/lpe-storage/src/storage_visibility.rs#L451-L515
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_missing_active_replacement
  - functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_retention_or_legal_hold
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup
---

# Signature

`async fn load_cleanup_counts( &self, tenant_filter: Option<Uuid>, ) -> Result<StorageCleanupCounts>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [count_cleanup_missing_active_replacement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_missing_active_replacement.md)
- [count_cleanup_retention_or_legal_hold](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_retention_or_legal_hold.md)

# Called by

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)
- [fetch_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)