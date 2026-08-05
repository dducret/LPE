---
type: Rust Method
title: placement_status
resource: crates/lpe-storage/src/blob_store.rs#L1172-L1185
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner
---

# Signature

`async fn placement_status(&self, pool: &PgPool, placement_id: Uuid) -> Result<Option<String>>`

# Called by

- [cleanup_one_old_placement_inner](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)