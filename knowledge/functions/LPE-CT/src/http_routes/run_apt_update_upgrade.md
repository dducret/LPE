---
type: Rust Function
title: run_apt_update_upgrade
resource: LPE-CT/src/http_routes.rs#L790-L799
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_actions/apt_update_upgrade
---

# Signature

`pub(crate) async fn run_apt_update_upgrade( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<system_actions::SystemActionResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [apt_update_upgrade](../../../../functions/LPE-CT/src/system_actions/apt_update_upgrade.md)