---
type: Rust Function
title: pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order
resource: crates/lpe-storage/src/blob_store/tests.rs#L802-L899
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/load_pending_blob_migration_jobs
---

# Signature

`async fn pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [load_pending_blob_migration_jobs](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/load_pending_blob_migration_jobs.md)