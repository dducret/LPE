---
type: Rust Function
title: storage_metadata_readiness_warns_noncritical_degradation
resource: crates/lpe-admin-api/src/health.rs#L138-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_check
  - functions/crates/lpe-admin-api/src/health/diagnostics
---

# Signature

`fn storage_metadata_readiness_warns_noncritical_degradation()`

# Calls

- [storage_metadata_readiness_check](../../../../../functions/crates/lpe-admin-api/src/health/storage_metadata_readiness_check.md)
- [diagnostics](../../../../../functions/crates/lpe-admin-api/src/health/diagnostics.md)