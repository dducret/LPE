---
type: Rust Function
title: select_storage_backend
resource: crates/lpe-storage/src/storage_backend.rs#L119-L133
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind
  - functions/crates/lpe-storage/src/storage_backend/normalize_postgres_config
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/effective_write_storage_pool_in_tx
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_copying_target_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_migration_source_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement
  - functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary
  - functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_normalizes_provider_neutral_config
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/storage_pool_summary
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
  - functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement
---

# Signature

`pub(crate) fn select_storage_backend( pool_kind: &str, config: &Value, ) -> Result<StorageBackendSelection>`

# Calls

- [normalize_storage_pool_kind](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind.md)
- [normalize_postgres_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_postgres_config.md)
- [parse_s3_compatible_config](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config.md)

# Called by

- [effective_write_storage_pool_in_tx](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/effective_write_storage_pool_in_tx.md)
- [create_blob_migration_job](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [ensure_copying_target_placement](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_copying_target_placement.md)
- [load_migration_source_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_migration_source_placement.md)
- [load_active_blob_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement.md)
- [storage_pool_config_summary](../../../../../functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary.md)
- [s3_compatible_backend_normalizes_provider_neutral_config](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_normalizes_provider_neutral_config.md)
- [ensure_active_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)
- [storage_pool_summary](../../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary.md)
- [check_pool_backend_health](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)
- [s3_compatible_pool_health_checks_active_object_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement.md)