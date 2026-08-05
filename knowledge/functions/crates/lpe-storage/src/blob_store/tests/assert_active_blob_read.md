---
type: Rust Function
title: assert_active_blob_read
resource: crates/lpe-storage/src/blob_store/tests.rs#L363-L382
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch
---

# Signature

`async fn assert_active_blob_read( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, expected_bytes: &[u8], )`

# Calls

- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_refuses_the_only_active_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)