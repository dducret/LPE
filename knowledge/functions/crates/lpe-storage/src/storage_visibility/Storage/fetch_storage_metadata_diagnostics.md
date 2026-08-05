---
type: Rust Method
title: fetch_storage_metadata_diagnostics
resource: crates/lpe-storage/src/storage_visibility.rs#L97-L161
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/count_missing_active_placements
  - functions/crates/lpe-storage/src/storage_visibility/storage_metadata_diagnostics
  called_by:
  - functions/crates/lpe-admin-api/src/health/health_ready
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_reports_consistent_seed_metadata
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes
---

# Signature

`pub async fn fetch_storage_metadata_diagnostics(&self) -> Result<StorageMetadataDiagnostics>`

# Calls

- [count_missing_active_placements](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_missing_active_placements.md)
- [storage_metadata_diagnostics](../../../../../../functions/crates/lpe-storage/src/storage_visibility/storage_metadata_diagnostics.md)

# Called by

- [health_ready](../../../../../../functions/crates/lpe-admin-api/src/health/health_ready.md)
- [storage_metadata_diagnostics_reports_consistent_seed_metadata](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_reports_consistent_seed_metadata.md)
- [storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes.md)