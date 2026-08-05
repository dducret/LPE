---
type: Rust Function
title: cleanup_worker_refuses_the_only_active_placement
resource: crates/lpe-storage/src/blob_store/tests.rs#L1710-L1746
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/tests/active_placement_id
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read
---

# Signature

`async fn cleanup_worker_refuses_the_only_active_placement()`

# Calls

- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [active_placement_id](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/active_placement_id.md)
- [cleanup_one_old_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_active_blob_read](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read.md)