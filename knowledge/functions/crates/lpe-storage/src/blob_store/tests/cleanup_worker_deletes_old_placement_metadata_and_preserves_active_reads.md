---
type: Rust Function
title: cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
resource: crates/lpe-storage/src/blob_store/tests.rs#L1656-L1707
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/tests/expire_retiring_placement
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_old_retiring_placements
  - functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read
---

# Signature

`async fn cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [expire_retiring_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/expire_retiring_placement.md)
- [cleanup_old_retiring_placements](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_old_retiring_placements.md)
- [assert_active_blob_read](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read.md)