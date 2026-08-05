---
type: Rust Function
title: update_policies
resource: LPE-CT/src/http_routes.rs#L928-L943
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/dashboard_config/normalize_policy_settings
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/mutate_state
  - functions/LPE-CT/src/restore_dashboard_state
---

# Signature

`pub(crate) async fn update_policies( State(state): State<AppState>, headers: HeaderMap, Json(mut payload): Json<PolicySettings>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [normalize_policy_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_policy_settings.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)
- [restore_dashboard_state](../../../../functions/LPE-CT/src/restore_dashboard_state.md)