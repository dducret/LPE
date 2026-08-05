---
type: Rust Module
title: storage_visibility
resource: crates/lpe-storage/src/storage_visibility.rs#L1-L1039
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-error-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-storage-backend-s3-probe-pool-s3-stat-object-select-storage-backend-storagebackenderror-storagebackendselection-storage-storagecleanupcounts-storagecleanupplacementsummary-storagecleanupvisibilityresponse-storagehealthresponse-storagemetadatadiagnostics-storagemigrationcounts-storagemigrationjobsummary-storagemigrationvisibilityresponse-storageplacementcounts-storagepoolhealth-storagepoolreference
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [PoolHealthRow](../../../../classes/crates/lpe-storage/src/storage_visibility/PoolHealthRow.md)
- [PoolBackendHealth](../../../../classes/crates/lpe-storage/src/storage_visibility/PoolBackendHealth.md)
- [PoolProbePlacement](../../../../classes/crates/lpe-storage/src/storage_visibility/PoolProbePlacement.md)
- [CleanupRow](../../../../classes/crates/lpe-storage/src/storage_visibility/CleanupRow.md)
- [fetch_platform_storage_health](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health.md)
- [fetch_tenant_storage_health](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health.md)
- [fetch_platform_storage_migrations](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_migrations.md)
- [fetch_tenant_storage_migrations](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations.md)
- [fetch_platform_storage_cleanup](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_cleanup.md)
- [fetch_tenant_storage_cleanup](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup.md)
- [fetch_storage_metadata_diagnostics](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics.md)
- [fetch_storage_health](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)
- [fetch_storage_migrations](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)
- [fetch_storage_cleanup](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_cleanup.md)
- [load_pool_health_rows](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_health_rows.md)
- [load_placement_counts](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_placement_counts.md)
- [count_missing_active_placements](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_missing_active_placements.md)
- [load_migration_counts](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_migration_counts.md)
- [load_cleanup_counts](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_counts.md)
- [count_cleanup_missing_active_replacement](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_missing_active_replacement.md)
- [count_cleanup_retention_or_legal_hold](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_cleanup_retention_or_legal_hold.md)
- [load_cleanup_rows](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows.md)
- [cleanup_blockers_for_row](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/cleanup_blockers_for_row.md)
- [pool_health_summaries](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries.md)
- [check_pool_backend_health](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)
- [load_pool_probe_placement](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_probe_placement.md)
- [ensure_visibility_tenant_exists](../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists.md)
- [pool_health_summary](../../../../functions/crates/lpe-storage/src/storage_visibility/pool_health_summary.md)
- [pool_backend_health_from_result](../../../../functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_result.md)
- [pool_backend_health_from_error](../../../../functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_error.md)
- [pool_reference_from_columns](../../../../functions/crates/lpe-storage/src/storage_visibility/pool_reference_from_columns.md)
- [summarize_error](../../../../functions/crates/lpe-storage/src/storage_visibility/summarize_error.md)
- [storage_metadata_diagnostics](../../../../functions/crates/lpe-storage/src/storage_visibility/storage_metadata_diagnostics.md)
- [CleanupBlockerState](../../../../classes/crates/lpe-storage/src/storage_visibility/CleanupBlockerState.md)
- [cleanup_blocker_labels](../../../../functions/crates/lpe-storage/src/storage_visibility/cleanup_blocker_labels.md)

# Imports

- `anyhow::{anyhow, Error, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    storage_backend::{
        s3_probe_pool, s3_stat_object, select_storage_backend, StorageBackendError,
        StorageBackendSelection,
    },
    Storage, StorageCleanupCounts, StorageCleanupPlacementSummary,
    StorageCleanupVisibilityResponse, StorageHealthResponse, StorageMetadataDiagnostics,
    StorageMigrationCounts, StorageMigrationJobSummary, StorageMigrationVisibilityResponse,
    StoragePlacementCounts, StoragePoolHealth, StoragePoolReference,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)