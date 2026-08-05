---
type: Rust Method
title: claim_blob_migration_job
resource: crates/lpe-storage/src/blob_store.rs#L713-L741
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`async fn claim_blob_migration_job(&self, pool: &PgPool) -> Result<Option<BlobMigrationJob>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)