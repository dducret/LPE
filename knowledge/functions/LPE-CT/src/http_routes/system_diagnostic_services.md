---
type: Rust Function
title: system_diagnostic_services
resource: LPE-CT/src/http_routes.rs#L1058-L1064
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_diagnostics/service_statuses
---

# Signature

`pub(crate) async fn system_diagnostic_services( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<system_diagnostics::ServiceStatusList>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [service_statuses](../../../../functions/LPE-CT/src/system_diagnostics/service_statuses.md)