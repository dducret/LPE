---
type: Rust Function
title: upload_public_tls_profile
resource: LPE-CT/src/http_routes.rs#L813-L833
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/store_public_tls_profile
  - functions/LPE-CT/src/mutate_state
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
---

# Signature

`pub(crate) async fn upload_public_tls_profile( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<PublicTlsUploadRequest>, ) -> Result<Json<DashboardState>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [store_public_tls_profile](../../../../functions/LPE-CT/src/store_public_tls_profile.md)
- [mutate_state](../../../../functions/LPE-CT/src/mutate_state.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [normalize_public_tls_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)