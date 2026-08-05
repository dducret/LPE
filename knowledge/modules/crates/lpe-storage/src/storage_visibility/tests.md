---
type: Rust Module
title: tests
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L1-L519
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-sha256-hex-storage-backend-s3-put-object-select-storage-backend-storagebackendselection-storage
  - external/serde-json-json-value
  - external/sqlx-postgres-pgpooloptions
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [test_storage](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/test_storage.md)
- [insert_tenant_domain](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_tenant_domain.md)
- [insert_blob](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_blob.md)
- [insert_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_placement.md)
- [insert_external_blob_with_active_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_external_blob_with_active_placement.md)
- [insert_failed_migration](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_failed_migration.md)
- [s3_test_config](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_test_config.md)
- [pool_health_marks_failed_placements_degraded](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/pool_health_marks_failed_placements_degraded.md)
- [s3_backend_health_errors_map_to_provider_neutral_states](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_backend_health_errors_map_to_provider_neutral_states.md)
- [cleanup_blockers_are_reported_without_internal_ids](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_blockers_are_reported_without_internal_ids.md)
- [storage_metadata_diagnostics_marks_missing_active_as_critical](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_marks_missing_active_as_critical.md)
- [storage_metadata_diagnostics_accepts_consistent_metadata](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_consistent_metadata.md)
- [storage_health_reports_degraded_and_tenant_scoped_counts](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts.md)
- [s3_compatible_pool_health_checks_active_object_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement.md)
- [cleanup_visibility_reports_blockers_without_blob_or_placement_ids](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_visibility_reports_blockers_without_blob_or_placement_ids.md)
- [storage_metadata_diagnostics_reports_consistent_seed_metadata](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_reports_consistent_seed_metadata.md)
- [storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes.md)
- [long_errors_are_summarized](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/long_errors_are_summarized.md)

# Imports

- `super::*`
- `crate::{
    sha256_hex,
    storage_backend::{s3_put_object, select_storage_backend, StorageBackendSelection},
    Storage,
}`
- `serde_json::{json, Value}`
- `sqlx::postgres::PgPoolOptions`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)