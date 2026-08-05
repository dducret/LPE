---
type: Rust Function
title: update_relay
resource: LPE-CT/src/http_routes.rs#L737-L748
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/dashboard_config/validate_relay_settings
  - functions/LPE-CT/src/mutate_state
---

# Signature

`pub(crate) async fn update_relay( State(state): State<AppState>, headers: HeaderMap, Json(mut payload): Json<RelaySettings>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [validate_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/validate_relay_settings.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)