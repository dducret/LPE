---
type: Rust Function
title: update_updates
resource: LPE-CT/src/http_routes.rs#L945-L955
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/mutate_state
---

# Signature

`pub(crate) async fn update_updates( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<UpdateSettings>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)