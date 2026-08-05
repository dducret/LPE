---
type: Rust Function
title: dashboard
resource: LPE-CT/src/http_routes.rs#L185-L197
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/queue_metrics
---

# Signature

`pub(crate) async fn dashboard( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<DashboardResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [queue_metrics](../../../../functions/LPE-CT/src/smtp/queue_metrics.md)