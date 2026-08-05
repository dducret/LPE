---
type: Rust Function
title: release_trace
resource: LPE-CT/src/http_routes.rs#L292-L317
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn release_trace( State(state): State<AppState>, headers: HeaderMap, AxumPath(trace_id): AxumPath<String>, ) -> Result<Json<smtp::TraceActionResult>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)