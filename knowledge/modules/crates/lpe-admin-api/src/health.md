---
type: Rust Module
title: health
resource: crates/lpe-admin-api/src/health.rs#L1-L150
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-state-json
  - external/lpe-storage-healthresponse-storage-storagemetadatadiagnostics
  - external/std-time-duration
  - external/crate-build-readiness-response-check-optional-http-dependency-ha-activation-check-http-internal-error-integration-shared-secret-lpe-ct-base-url-readiness-failed-readiness-ok-readiness-warn-types-apiresult-readinesscheck-readinessresponse
  - external/super-storage-metadata-readiness-check
  - external/lpe-storage-storagemetadatadiagnostics
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [health](../../../../functions/crates/lpe-admin-api/src/health/health.md)
- [health_live](../../../../functions/crates/lpe-admin-api/src/health/health_live.md)
- [health_ready](../../../../functions/crates/lpe-admin-api/src/health/health_ready.md)
- [storage_metadata_readiness_check](../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_check.md)
- [diagnostics](../../../../functions/crates/lpe-admin-api/src/health/diagnostics.md)
- [storage_metadata_readiness_fails_critical_degradation](../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_fails_critical_degradation.md)
- [storage_metadata_readiness_warns_noncritical_degradation](../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_warns_noncritical_degradation.md)
- [storage_metadata_readiness_passes_consistent_metadata](../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_passes_consistent_metadata.md)

# Imports

- `axum::{extract::State, Json}`
- `lpe_storage::{HealthResponse, Storage, StorageMetadataDiagnostics}`
- `std::time::Duration`
- `crate::{
    build_readiness_response, check_optional_http_dependency, ha_activation_check,
    http::internal_error,
    integration_shared_secret, lpe_ct_base_url, readiness_failed, readiness_ok, readiness_warn,
    types::{ApiResult, ReadinessCheck, ReadinessResponse},
}`
- `super::storage_metadata_readiness_check`
- `lpe_storage::StorageMetadataDiagnostics`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)