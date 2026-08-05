---
type: Rust Method
title: ensure_storage_pool_can_be_disabled
resource: crates/lpe-storage/src/storage_policy.rs#L710-L771
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
---

# Signature

`async fn ensure_storage_pool_can_be_disabled(&self, pool_id: Uuid) -> Result<()>`

# Called by

- [update_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)