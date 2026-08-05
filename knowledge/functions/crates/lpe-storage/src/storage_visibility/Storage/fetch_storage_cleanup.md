---
type: Rust Method
title: fetch_storage_cleanup
resource: crates/lpe-storage/src/storage_visibility.rs#L257-L281
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows
  - functions/crates/lpe-storage/src/storage_visibility/Storage/cleanup_blockers_for_row
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_cleanup
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup
---

# Signature

`async fn fetch_storage_cleanup( &self, tenant_filter: Option<Uuid>, ) -> Result<StorageCleanupVisibilityResponse>`

# Calls

- [load_cleanup_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts.md)
- [load_cleanup_rows](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows.md)
- [cleanup_blockers_for_row](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/cleanup_blockers_for_row.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [fetch_platform_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_cleanup.md)
- [fetch_tenant_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup.md)