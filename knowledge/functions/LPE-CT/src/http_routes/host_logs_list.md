---
type: Rust Function
title: host_logs_list
resource: LPE-CT/src/http_routes.rs#L349-L358
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/host_logs/list
---

# Signature

`pub(crate) async fn host_logs_list( State(state): State<AppState>, headers: HeaderMap, AxumPath(category): AxumPath<String>, ) -> Result<Json<host_logs::HostLogList>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [list](../../../../functions/LPE-CT/src/host_logs/list.md)