---
type: Rust Function
title: trace_details
resource: LPE-CT/src/http_routes.rs#L253-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
---

# Signature

`pub(crate) async fn trace_details( State(state): State<AppState>, headers: HeaderMap, AxumPath(trace_id): AxumPath<String>, ) -> Result<Json<smtp::TraceDetails>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [load_trace_details](../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)