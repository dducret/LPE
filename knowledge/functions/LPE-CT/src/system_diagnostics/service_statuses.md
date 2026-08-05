---
type: Rust Function
title: service_statuses
resource: LPE-CT/src/system_diagnostics.rs#L46-L64
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/system_diagnostic_services
---

# Signature

`pub(crate) async fn service_statuses() -> ServiceStatusList`

# Called by

- [system_diagnostic_services](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_services.md)