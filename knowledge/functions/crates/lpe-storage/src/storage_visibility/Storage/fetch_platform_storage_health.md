---
type: Rust Method
title: fetch_platform_storage_health
resource: crates/lpe-storage/src/storage_visibility.rs#L59-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts
  - functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement
---

# Signature

`pub async fn fetch_platform_storage_health(&self) -> Result<StorageHealthResponse>`

# Calls

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)

# Called by

- [get_storage_health](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [storage_health_reports_degraded_and_tenant_scoped_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts.md)
- [s3_compatible_pool_health_checks_active_object_placement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement.md)