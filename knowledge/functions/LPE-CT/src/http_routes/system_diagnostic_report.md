---
type: Rust Function
title: system_diagnostic_report
resource: LPE-CT/src/http_routes.rs#L1083-L1103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/queue_metrics
  - functions/LPE-CT/src/system_diagnostics/command_diagnostic
---

# Signature

`pub(crate) async fn system_diagnostic_report( State(state): State<AppState>, headers: HeaderMap, AxumPath(kind): AxumPath<String>, ) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [queue_metrics](../../../../functions/LPE-CT/src/smtp/queue_metrics.md)
- [command_diagnostic](../../../../functions/LPE-CT/src/system_diagnostics/command_diagnostic.md)