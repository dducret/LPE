---
type: Rust Function
title: create_blob_migration_job_rejects_same_source_and_target_pool
resource: crates/lpe-storage/src/blob_store/tests.rs#L766-L799
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn create_blob_migration_job_rejects_same_source_and_target_pool()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)