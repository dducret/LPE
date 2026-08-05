---
type: Rust Function
title: copy_verify_worker_leaves_active_source_read_path_unchanged
resource: crates/lpe-storage/src/blob_store/tests.rs#L981-L1055
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
---

# Signature

`async fn copy_verify_worker_leaves_active_source_read_path_unchanged()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)