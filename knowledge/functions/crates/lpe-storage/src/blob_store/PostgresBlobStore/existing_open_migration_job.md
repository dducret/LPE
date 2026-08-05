---
type: Rust Method
title: existing_open_migration_job
resource: crates/lpe-storage/src/blob_store.rs#L855-L887
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
---

# Signature

`async fn existing_open_migration_job( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, blob_id: Uuid, target_storage_pool_id: Uuid, ) -> Result<Option<BlobMigrationJob>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)