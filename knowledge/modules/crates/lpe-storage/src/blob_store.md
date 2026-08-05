---
type: Rust Module
title: blob_store
resource: crates/lpe-storage/src/blob_store.rs#L1-L1426
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/sha2-digest-sha256
  - external/sqlx-pgpool-postgres-row
  - external/uuid-uuid
  - external/crate-sha256-hex-storage-backend-s3-put-object-select-storage-backend-storagebackendselection
  - external/types-blob-migration-job-from-row-durable-blob-kind-from-str-is-constraint-error-normalize-migration-blob-kind-activeblobplacement-migrationtargetplacement-writestoragepool
  - external/pub-crate-use-types-blobmigrationjob-durableblobkind-placementcleanupeligibility-placementcleanupresult-postgresblobstore-putblobrequest-storedblobbytes-storedblobref-storedblobstat
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [put_durable_blob_in_tx](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)
- [effective_write_storage_pool_in_tx](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/effective_write_storage_pool_in_tx.md)
- [ensure_backend_placement_in_tx](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx.md)
- [active_blob_placement_exists_in_tx](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/active_blob_placement_exists_in_tx.md)
- [create_blob_migration_job](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [load_pending_blob_migration_jobs](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/load_pending_blob_migration_jobs.md)
- [copy_and_verify_one_blob_migration_job](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
- [switch_verified_blob_migration_job](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [claim_blob_migration_job](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/claim_blob_migration_job.md)
- [ensure_copying_target_placement](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_copying_target_placement.md)
- [record_blob_migration_failure](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_blob_migration_failure.md)
- [existing_open_migration_job](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/existing_open_migration_job.md)
- [old_placement_cleanup_eligibility](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility.md)
- [cleanup_old_retiring_placements](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_old_retiring_placements.md)
- [cleanup_one_old_placement](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement.md)
- [cleanup_one_old_placement_inner](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)
- [placement_status](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/placement_status.md)
- [record_placement_cleanup_failure](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_placement_cleanup_failure.md)
- [simulate_old_placement_cleanup_failure](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/simulate_old_placement_cleanup_failure.md)
- [live_reference_cleanup_blockers](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/live_reference_cleanup_blockers.md)
- [message_lifecycle_cleanup_blockers](../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/message_lifecycle_cleanup_blockers.md)

# Imports

- `anyhow::{anyhow, Result}`
- `sha2::{Digest, Sha256}`
- `sqlx::{PgPool, Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    sha256_hex,
    storage_backend::{s3_put_object, select_storage_backend, StorageBackendSelection},
}`
- `types::{
    blob_migration_job_from_row, durable_blob_kind_from_str, is_constraint_error,
    normalize_migration_blob_kind, ActiveBlobPlacement, MigrationTargetPlacement, WriteStoragePool,
}`
- `pub(crate) use types::{
    BlobMigrationJob, DurableBlobKind, PlacementCleanupEligibility, PlacementCleanupResult,
    PostgresBlobStore, PutBlobRequest, StoredBlobBytes, StoredBlobRef, StoredBlobStat,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)