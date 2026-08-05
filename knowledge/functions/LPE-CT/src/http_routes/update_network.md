---
type: Rust Function
title: update_network
resource: LPE-CT/src/http_routes.rs#L750-L765
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
  - functions/LPE-CT/src/mutate_state
---

# Signature

`pub(crate) async fn update_network( State(state): State<AppState>, headers: HeaderMap, Json(mut payload): Json<NetworkSettings>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [normalize_public_tls_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)