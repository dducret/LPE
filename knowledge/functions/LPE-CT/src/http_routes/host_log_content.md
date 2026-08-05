---
type: Rust Function
title: host_log_content
resource: LPE-CT/src/http_routes.rs#L360-L369
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/host_logs/read_content
---

# Signature

`pub(crate) async fn host_log_content( State(state): State<AppState>, headers: HeaderMap, AxumPath((category, log_id)): AxumPath<(String, String)>, ) -> Result<Json<host_logs::HostLogContent>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_content](../../../../functions/LPE-CT/src/host_logs/read_content.md)