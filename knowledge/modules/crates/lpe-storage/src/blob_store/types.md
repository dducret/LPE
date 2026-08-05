---
type: Rust Module
title: types
resource: crates/lpe-storage/src/blob_store/types.rs#L1-L174
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-storage-backend-storagebackendselection
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [DurableBlobKind](../../../../../classes/crates/lpe-storage/src/blob_store/types/DurableBlobKind.md)
- [as_str](../../../../../functions/crates/lpe-storage/src/blob_store/types/DurableBlobKind/as_str.md)
- [PutBlobRequest](../../../../../classes/crates/lpe-storage/src/blob_store/types/PutBlobRequest.md)
- [StoredBlobRef](../../../../../classes/crates/lpe-storage/src/blob_store/types/StoredBlobRef.md)
- [StoredBlobBytes](../../../../../classes/crates/lpe-storage/src/blob_store/types/StoredBlobBytes.md)
- [StoredBlobStat](../../../../../classes/crates/lpe-storage/src/blob_store/types/StoredBlobStat.md)
- [BlobMigrationJob](../../../../../classes/crates/lpe-storage/src/blob_store/types/BlobMigrationJob.md)
- [WriteStoragePool](../../../../../classes/crates/lpe-storage/src/blob_store/types/WriteStoragePool.md)
- [ActiveBlobPlacement](../../../../../classes/crates/lpe-storage/src/blob_store/types/ActiveBlobPlacement.md)
- [MigrationTargetPlacement](../../../../../classes/crates/lpe-storage/src/blob_store/types/MigrationTargetPlacement.md)
- [PlacementCleanupEligibility](../../../../../classes/crates/lpe-storage/src/blob_store/types/PlacementCleanupEligibility.md)
- [is_eligible](../../../../../functions/crates/lpe-storage/src/blob_store/types/PlacementCleanupEligibility/is_eligible.md)
- [PlacementCleanupResult](../../../../../classes/crates/lpe-storage/src/blob_store/types/PlacementCleanupResult.md)
- [PostgresBlobStore](../../../../../classes/crates/lpe-storage/src/blob_store/types/PostgresBlobStore.md)
- [normalize_migration_blob_kind](../../../../../functions/crates/lpe-storage/src/blob_store/types/normalize_migration_blob_kind.md)
- [durable_blob_kind_from_str](../../../../../functions/crates/lpe-storage/src/blob_store/types/durable_blob_kind_from_str.md)
- [blob_migration_job_from_row](../../../../../functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row.md)
- [is_constraint_error](../../../../../functions/crates/lpe-storage/src/blob_store/types/is_constraint_error.md)

# Imports

- `anyhow::{anyhow, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::storage_backend::StorageBackendSelection`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)