---
type: Rust Function
title: system_health_check
resource: LPE-CT/src/http_routes.rs#L1105-L1130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn system_health_check( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)