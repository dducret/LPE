---
type: Rust Function
title: s3_compatible_migration_paths_copy_verify_and_switch
resource: crates/lpe-storage/src/blob_store/tests.rs#L2349-L2589
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read
---

# Signature

`async fn s3_compatible_migration_paths_copy_verify_and_switch()`

# Calls

- [insert_s3_storage_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool.md)
- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [assert_active_blob_read](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read.md)