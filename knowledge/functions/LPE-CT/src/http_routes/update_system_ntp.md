---
type: Rust Function
title: update_system_ntp
resource: LPE-CT/src/http_routes.rs#L767-L777
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_actions/update_ntp
---

# Signature

`pub(crate) async fn update_system_ntp( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<system_actions::NtpUpdateRequest>, ) -> Result<Json<system_actions::SystemActionResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [update_ntp](../../../../functions/LPE-CT/src/system_actions/update_ntp.md)