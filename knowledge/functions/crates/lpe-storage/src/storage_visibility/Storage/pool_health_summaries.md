---
type: Rust Method
title: pool_health_summaries
resource: crates/lpe-storage/src/storage_visibility.rs#L744-L754
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/storage_visibility/pool_health_summary
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
---

# Signature

`async fn pool_health_summaries( &self, rows: Vec<PoolHealthRow>, ) -> Result<Vec<StoragePoolHealth>>`

# Calls

- [check_pool_backend_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [pool_health_summary](../../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_health_summary.md)

# Called by

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)