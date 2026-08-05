---
type: Rust Function
title: update_site
resource: LPE-CT/src/http_routes.rs#L725-L735
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/mutate_state
---

# Signature

`pub(crate) async fn update_site( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<SiteProfile>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)