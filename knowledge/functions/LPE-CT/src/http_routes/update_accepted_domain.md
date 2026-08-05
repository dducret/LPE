---
type: Rust Function
title: update_accepted_domain
resource: LPE-CT/src/http_routes.rs#L552-L596
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/dashboard_config/accepted_domain_from_input
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/dashboard_config/normalize_accepted_domains
  - functions/LPE-CT/src/append_dashboard_audit_event
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_technical_store
  - functions/LPE-CT/src/restore_dashboard_state
---

# Signature

`pub(crate) async fn update_accepted_domain( State(state): State<AppState>, headers: HeaderMap, AxumPath(domain_id): AxumPath<String>, Json(payload): Json<AcceptedDomainInput>, ) -> Result<Json<AcceptedDomain>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [accepted_domain_from_input](../../../../functions/LPE-CT/src/dashboard_config/accepted_domain_from_input.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [normalize_accepted_domains](../../../../functions/LPE-CT/src/dashboard_config/normalize_accepted_domains.md)
- [append_dashboard_audit_event](../../../../functions/LPE-CT/src/append_dashboard_audit_event.md)
- [persist_state](../../../../functions/LPE-CT/src/persist_state.md)
- [sync_technical_store](../../../../functions/LPE-CT/src/sync_technical_store.md)
- [restore_dashboard_state](../../../../functions/LPE-CT/src/restore_dashboard_state.md)