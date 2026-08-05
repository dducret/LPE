---
type: Rust Function
title: delete_public_tls_profile
resource: LPE-CT/src/http_routes.rs#L881-L926
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/mutate_state
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
---

# Signature

`pub(crate) async fn delete_public_tls_profile( State(state): State<AppState>, headers: HeaderMap, AxumPath(profile_id): AxumPath<String>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)
- [normalize_public_tls_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)