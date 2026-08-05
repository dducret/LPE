---
type: Rust Method
title: load_pending_blob_migration_jobs
resource: crates/lpe-storage/src/blob_store.rs#L380-L403
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order
---

# Signature

`pub(crate) async fn load_pending_blob_migration_jobs( &self, pool: &PgPool, limit: i64, ) -> Result<Vec<BlobMigrationJob>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order.md)