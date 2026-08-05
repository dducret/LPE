---
type: Rust Method
title: load_migration_source_placement
resource: crates/lpe-storage/src/blob_store/io.rs#L99-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`pub(super) async fn load_migration_source_placement( &self, pool: &PgPool, job: &BlobMigrationJob, kind: DurableBlobKind, ) -> Result<Option<ActiveBlobPlacement>>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [select_storage_backend](../../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)