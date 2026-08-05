---
type: Rust Function
title: mutate_state
resource: LPE-CT/src/main.rs#L827-L850
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/append_dashboard_audit_event
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  called_by:
  - functions/LPE-CT/src/http_routes/update_site
  - functions/LPE-CT/src/http_routes/update_relay
  - functions/LPE-CT/src/http_routes/update_network
  - functions/LPE-CT/src/http_routes/upload_public_tls_profile
  - functions/LPE-CT/src/http_routes/select_public_tls_profile
  - functions/LPE-CT/src/http_routes/delete_public_tls_profile
  - functions/LPE-CT/src/http_routes/update_policies
  - functions/LPE-CT/src/http_routes/update_updates
---

# Signature

`async fn mutate_state<F>( state: &AppState, actor: &str, action: &str, update: F, ) -> Result<Json<DashboardState>, ApiError> where F: FnOnce(&mut DashboardState),`

# Calls

- [append_dashboard_audit_event](../../../functions/LPE-CT/src/append_dashboard_audit_event.md)
- [persist_state](../../../functions/LPE-CT/src/persist_state.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)

# Called by

- [update_site](../../../functions/LPE-CT/src/http_routes/update_site.md)
- [update_relay](../../../functions/LPE-CT/src/http_routes/update_relay.md)
- [update_network](../../../functions/LPE-CT/src/http_routes/update_network.md)
- [upload_public_tls_profile](../../../functions/LPE-CT/src/http_routes/upload_public_tls_profile.md)
- [select_public_tls_profile](../../../functions/LPE-CT/src/http_routes/select_public_tls_profile.md)
- [delete_public_tls_profile](../../../functions/LPE-CT/src/http_routes/delete_public_tls_profile.md)
- [update_policies](../../../functions/LPE-CT/src/http_routes/update_policies.md)
- [update_updates](../../../functions/LPE-CT/src/http_routes/update_updates.md)