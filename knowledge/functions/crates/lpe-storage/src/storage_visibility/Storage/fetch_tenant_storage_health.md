---
type: Rust Method
title: fetch_tenant_storage_health
resource: crates/lpe-storage/src/storage_visibility.rs#L63-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts
---

# Signature

`pub async fn fetch_tenant_storage_health( &self, tenant_id: Uuid, ) -> Result<StorageHealthResponse>`

# Calls

- [ensure_visibility_tenant_exists](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists.md)
- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)

# Called by

- [get_storage_health](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [storage_health_reports_degraded_and_tenant_scoped_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts.md)