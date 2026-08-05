---
type: Rust Method
title: fetch_tenant_storage_cleanup
resource: crates/lpe-storage/src/storage_visibility.rs#L89-L95
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
  - functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_visibility_reports_blockers_without_blob_or_placement_ids
---

# Signature

`pub async fn fetch_tenant_storage_cleanup( &self, tenant_id: Uuid, ) -> Result<StorageCleanupVisibilityResponse>`

# Calls

- [ensure_visibility_tenant_exists](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists.md)
- [fetch_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)

# Called by

- [get_storage_cleanup](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [cleanup_visibility_reports_blockers_without_blob_or_placement_ids](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_visibility_reports_blockers_without_blob_or_placement_ids.md)