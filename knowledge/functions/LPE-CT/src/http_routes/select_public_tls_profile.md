---
type: Rust Function
title: select_public_tls_profile
resource: LPE-CT/src/http_routes.rs#L835-L879
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/validate_tls_pair_from_paths
  - functions/LPE-CT/src/mutate_state
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
---

# Signature

`pub(crate) async fn select_public_tls_profile( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<PublicTlsSelectionRequest>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [validate_tls_pair_from_paths](../../../../functions/LPE-CT/src/validate_tls_pair_from_paths.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)
- [normalize_public_tls_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)