---
type: Rust Function
title: storage_metadata_diagnostics
resource: crates/lpe-storage/src/storage_visibility.rs#L968-L997
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_marks_missing_active_as_critical
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_consistent_metadata
---

# Signature

`fn storage_metadata_diagnostics( active_pools: u64, platform_default_active: bool, invalid_policy_references: u64, active_placements_on_inactive_pools: u64, missing_active_placements: u64, ) -> StorageMetadataDiagnostics`

# Called by

- [fetch_storage_metadata_diagnostics](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics.md)
- [storage_metadata_diagnostics_marks_missing_active_as_critical](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_marks_missing_active_as_critical.md)
- [storage_metadata_diagnostics_accepts_consistent_metadata](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_consistent_metadata.md)