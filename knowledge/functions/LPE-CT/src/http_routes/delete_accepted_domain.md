---
type: Rust Function
title: delete_accepted_domain
resource: LPE-CT/src/http_routes.rs#L598-L626
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/append_dashboard_audit_event
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_technical_store
  - functions/LPE-CT/src/restore_dashboard_state
---

# Signature

`pub(crate) async fn delete_accepted_domain( State(state): State<AppState>, headers: HeaderMap, AxumPath(domain_id): AxumPath<String>, ) -> Result<StatusCode, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [append_dashboard_audit_event](../../../../functions/LPE-CT/src/append_dashboard_audit_event.md)
- [persist_state](../../../../functions/LPE-CT/src/persist_state.md)
- [sync_technical_store](../../../../functions/LPE-CT/src/sync_technical_store.md)
- [restore_dashboard_state](../../../../functions/LPE-CT/src/restore_dashboard_state.md)