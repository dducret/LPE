---
type: Rust Function
title: connect_lpe_support
resource: LPE-CT/src/http_routes.rs#L1167-L1181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_diagnostics/support_connect
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn connect_lpe_support( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [support_connect](../../../../functions/LPE-CT/src/system_diagnostics/support_connect.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)