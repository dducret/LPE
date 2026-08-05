---
type: Rust Function
title: create_blob_migration_job_accepts_s3_compatible_target_pool
resource: crates/lpe-storage/src/blob_store/tests.rs#L592-L626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool
  - functions/crates/lpe-storage/src/blob_store/tests/s3_placeholder_config
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn create_blob_migration_job_accepts_s3_compatible_target_pool()`

# Calls

- [insert_s3_storage_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool.md)
- [s3_placeholder_config](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_placeholder_config.md)
- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)