---
type: Rust Function
title: trace_history
resource: LPE-CT/src/http_routes.rs#L234-L251
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/reporting/load_trace_history
---

# Signature

`pub(crate) async fn trace_history( State(state): State<AppState>, headers: HeaderMap, AxumPath(trace_id): AxumPath<String>, ) -> Result<Json<reporting::TraceHistoryDetails>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [load_trace_history](../../../../functions/LPE-CT/src/reporting/load_trace_history.md)