---
type: Rust Function
title: diagnostics
resource: crates/lpe-admin-api/src/health.rs#L117-L127
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_fails_critical_degradation
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_warns_noncritical_degradation
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_passes_consistent_metadata
---

# Signature

`fn diagnostics(status: &str, critical: bool) -> StorageMetadataDiagnostics`

# Called by

- [storage_metadata_readiness_fails_critical_degradation](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_fails_critical_degradation.md)
- [storage_metadata_readiness_warns_noncritical_degradation](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_warns_noncritical_degradation.md)
- [storage_metadata_readiness_passes_consistent_metadata](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_passes_consistent_metadata.md)