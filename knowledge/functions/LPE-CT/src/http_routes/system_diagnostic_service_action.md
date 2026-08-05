---
type: Rust Function
title: system_diagnostic_service_action
resource: LPE-CT/src/http_routes.rs#L1066-L1081
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_diagnostics/service_action
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn system_diagnostic_service_action( State(state): State<AppState>, headers: HeaderMap, AxumPath((service_id, action)): AxumPath<(String, String)>, ) -> Result<Json<system_diagnostics::ServiceStatus>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [service_action](../../../../functions/LPE-CT/src/system_diagnostics/service_action.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)