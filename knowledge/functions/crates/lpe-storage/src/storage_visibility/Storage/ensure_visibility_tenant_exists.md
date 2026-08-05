---
type: Rust Method
title: ensure_visibility_tenant_exists
resource: crates/lpe-storage/src/storage_visibility.rs#L829-L847
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup
---

# Signature

`async fn ensure_visibility_tenant_exists(&self, tenant_id: Uuid) -> Result<()>`

# Called by

- [fetch_tenant_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health.md)
- [fetch_tenant_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations.md)
- [fetch_tenant_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup.md)