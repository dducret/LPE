---
type: Rust Function
title: switch_preserves_reads_stats_and_verification_across_phases
resource: crates/lpe-storage/src/blob_store/tests.rs#L1235-L1329
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
---

# Signature

`async fn switch_preserves_reads_stats_and_verification_across_phases()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [stat_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)