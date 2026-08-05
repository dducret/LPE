---
type: Rust Function
title: run_system_power_action
resource: LPE-CT/src/http_routes.rs#L801-L811
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_actions/power_action
---

# Signature

`pub(crate) async fn run_system_power_action( State(state): State<AppState>, headers: HeaderMap, AxumPath(action): AxumPath<String>, ) -> Result<Json<system_actions::SystemActionResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [power_action](../../../../functions/LPE-CT/src/system_actions/power_action.md)