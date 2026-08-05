---
type: Rust Function
title: delete_host_log
resource: LPE-CT/src/http_routes.rs#L397-L417
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/host_logs/delete
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn delete_host_log( State(state): State<AppState>, headers: HeaderMap, AxumPath((category, log_id)): AxumPath<(String, String)>, ) -> Result<Json<HealthResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [delete](../../../../functions/LPE-CT/src/host_logs/delete.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)