---
type: Rust Function
title: migrate_attachment_and_cleanup_source
resource: crates/lpe-storage/src/pst.rs#L818-L865
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement
  called_by:
  - functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup
---

# Signature

`async fn migrate_attachment_and_cleanup_source( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, )`

# Calls

- [create_blob_migration_job](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [copy_and_verify_one_blob_migration_job](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [cleanup_one_old_placement](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement.md)

# Called by

- [pst_export_reconstructs_attachment_after_old_placement_cleanup](../../../../../functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup.md)