---
type: Rust Function
title: sync_system_ntp
resource: LPE-CT/src/http_routes.rs#L779-L788
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_actions/sync_ntp
---

# Signature

`pub(crate) async fn sync_system_ntp( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<system_actions::SystemActionResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [sync_ntp](../../../../functions/LPE-CT/src/system_actions/sync_ntp.md)