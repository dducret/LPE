---
type: Rust Function
title: me
resource: LPE-CT/src/http_routes.rs#L174-L183
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
---

# Signature

`pub(crate) async fn me( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<ManagementIdentity>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)