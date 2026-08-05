---
type: Rust Method
title: fetch_platform_storage_cleanup
resource: crates/lpe-storage/src/storage_visibility.rs#L85-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
---

# Signature

`pub async fn fetch_platform_storage_cleanup(&self) -> Result<StorageCleanupVisibilityResponse>`

# Calls

- [fetch_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)

# Called by

- [get_storage_cleanup](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)