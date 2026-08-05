---
type: Rust Method
title: record_placement_cleanup_failure
resource: crates/lpe-storage/src/blob_store.rs#L1188-L1210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner
---

# Signature

`async fn record_placement_cleanup_failure( &self, pool: &PgPool, placement_id: Uuid, error: &str, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [cleanup_one_old_placement_inner](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)