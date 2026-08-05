---
type: Rust Method
title: fetch_storage_health
resource: crates/lpe-storage/src/storage_visibility.rs#L163-L189
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_health_rows
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_placement_counts
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_migration_counts
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts
  - functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health
---

# Signature

`async fn fetch_storage_health( &self, tenant_filter: Option<Uuid>, ) -> Result<StorageHealthResponse>`

# Calls

- [load_pool_health_rows](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_health_rows.md)
- [load_placement_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_placement_counts.md)
- [load_migration_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_migration_counts.md)
- [load_cleanup_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts.md)
- [pool_health_summaries](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries.md)

# Called by

- [fetch_platform_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health.md)
- [fetch_tenant_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health.md)