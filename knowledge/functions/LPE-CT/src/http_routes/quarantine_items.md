---
type: Rust Function
title: quarantine_items
resource: LPE-CT/src/http_routes.rs#L199-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items
---

# Signature

`pub(crate) async fn quarantine_items( State(state): State<AppState>, headers: HeaderMap, Query(query): Query<smtp::QuarantineQuery>, ) -> Result<Json<Vec<smtp::QuarantineSummary>>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [list_quarantine_items](../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items.md)