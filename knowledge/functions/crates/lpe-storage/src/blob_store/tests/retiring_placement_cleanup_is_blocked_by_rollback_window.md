---
type: Rust Function
title: retiring_placement_cleanup_is_blocked_by_rollback_window
resource: crates/lpe-storage/src/blob_store/tests.rs#L1463-L1499
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
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility
---

# Signature

`async fn retiring_placement_cleanup_is_blocked_by_rollback_window()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [old_placement_cleanup_eligibility](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility.md)