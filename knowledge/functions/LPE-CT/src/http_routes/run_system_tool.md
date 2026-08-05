---
type: Rust Function
title: run_system_tool
resource: LPE-CT/src/http_routes.rs#L1132-L1148
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_diagnostics/run_tool
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn run_system_tool( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<system_diagnostics::ToolRunRequest>, ) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [run_tool](../../../../functions/LPE-CT/src/system_diagnostics/run_tool.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)