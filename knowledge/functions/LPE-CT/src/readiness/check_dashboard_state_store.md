---
type: Rust Function
title: check_dashboard_state_store
resource: LPE-CT/src/readiness.rs#L122-L137
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn check_dashboard_state_store( local_data_stores: &LocalDataStoresSettings, ) -> ReadinessCheck`

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)