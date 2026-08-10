---
type: Rust Function
title: insert_s3_storage_pool
resource: crates/lpe-storage/src/blob_store/tests.rs#L89-L104
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/configure_s3_platform_pool
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch
---

# Signature

`async fn insert_s3_storage_pool(storage: &Storage, config: Value) -> Uuid`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [configure_s3_platform_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/configure_s3_platform_pool.md)
- [create_blob_migration_job_accepts_s3_compatible_target_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)