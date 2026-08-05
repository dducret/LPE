---
type: Rust Function
title: update_reporting
resource: LPE-CT/src/http_routes.rs#L968-L993
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/append_dashboard_audit_event
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_technical_store
  - functions/LPE-CT/src/restore_dashboard_state
  - functions/LPE-CT/src/reporting/snapshot
---

# Signature

`pub(crate) async fn update_reporting( State(state): State<AppState>, headers: HeaderMap, Json(mut payload): Json<reporting::ReportingSettings>, ) -> Result<Json<reporting::ReportingSnapshot>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [append_dashboard_audit_event](../../../../functions/LPE-CT/src/append_dashboard_audit_event.md)
- [persist_state](../../../../functions/LPE-CT/src/persist_state.md)
- [sync_technical_store](../../../../functions/LPE-CT/src/sync_technical_store.md)
- [restore_dashboard_state](../../../../functions/LPE-CT/src/restore_dashboard_state.md)
- [snapshot](../../../../functions/LPE-CT/src/reporting/snapshot.md)