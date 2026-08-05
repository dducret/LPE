---
type: Rust Function
title: route_diagnostics
resource: LPE-CT/src/http_routes.rs#L423-L435
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
---

# Signature

`pub(crate) async fn route_diagnostics( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<RouteDiagnosticsResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)