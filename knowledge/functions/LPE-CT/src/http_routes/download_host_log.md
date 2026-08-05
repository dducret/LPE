---
type: Rust Function
title: download_host_log
resource: LPE-CT/src/http_routes.rs#L371-L395
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/host_logs/download
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
---

# Signature

`pub(crate) async fn download_host_log( State(state): State<AppState>, headers: HeaderMap, AxumPath((category, log_id)): AxumPath<(String, String)>, ) -> Result<Response, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [download](../../../../functions/LPE-CT/src/host_logs/download.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [into_response](../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)