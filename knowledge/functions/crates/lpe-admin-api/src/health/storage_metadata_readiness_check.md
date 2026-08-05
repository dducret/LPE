---
type: Rust Function
title: storage_metadata_readiness_check
resource: crates/lpe-admin-api/src/health.rs#L93-L110
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/health/health_ready
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_fails_critical_degradation
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_warns_noncritical_degradation
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_passes_consistent_metadata
---

# Signature

`fn storage_metadata_readiness_check( result: anyhow::Result<StorageMetadataDiagnostics>, ) -> ReadinessCheck`

# Called by

- [health_ready](../../../../../functions/crates/lpe-admin-api/src/health/health_ready.md)
- [storage_metadata_readiness_fails_critical_degradation](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_fails_critical_degradation.md)
- [storage_metadata_readiness_warns_noncritical_degradation](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_warns_noncritical_degradation.md)
- [storage_metadata_readiness_passes_consistent_metadata](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_passes_consistent_metadata.md)