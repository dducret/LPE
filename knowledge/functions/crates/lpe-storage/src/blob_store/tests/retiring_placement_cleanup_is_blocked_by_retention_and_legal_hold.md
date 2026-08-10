---
type: Rust Function
title: retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold
resource: crates/lpe-storage/src/blob_store/tests.rs#L1568-L1653
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/tests/expire_retiring_placement
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_blockers
---

# Signature

`async fn retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold()`

# Calls

- [insert_logical_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [expire_retiring_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/expire_retiring_placement.md)
- [cleanup_blockers](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_blockers.md)