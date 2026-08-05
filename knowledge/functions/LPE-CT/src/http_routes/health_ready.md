---
type: Rust Function
title: health_ready
resource: LPE-CT/src/http_routes.rs#L21-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/read_state
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/readiness/check_dashboard_state_store
  - functions/LPE-CT/src/readiness/check_spool_layout
  - functions/LPE-CT/src/readiness/check_local_data_store_policy
  - functions/LPE-CT/src/readiness/check_non_empty_value
  - functions/LPE-CT/src/readiness/check_optional_tcp_dependency
  - functions/LPE-CT/src/readiness/check_spool_pressure
  - functions/LPE-CT/src/readiness/check_quarantine_backlog
  - functions/LPE-CT/src/readiness/readiness_status
---

# Signature

`pub(crate) async fn health_ready( State(state): State<AppState>, ) -> Result<Json<ReadinessResponse>, ApiError>`

# Calls

- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [check_dashboard_state_store](../../../../functions/LPE-CT/src/readiness/check_dashboard_state_store.md)
- [check_spool_layout](../../../../functions/LPE-CT/src/readiness/check_spool_layout.md)
- [check_local_data_store_policy](../../../../functions/LPE-CT/src/readiness/check_local_data_store_policy.md)
- [check_non_empty_value](../../../../functions/LPE-CT/src/readiness/check_non_empty_value.md)
- [check_optional_tcp_dependency](../../../../functions/LPE-CT/src/readiness/check_optional_tcp_dependency.md)
- [check_spool_pressure](../../../../functions/LPE-CT/src/readiness/check_spool_pressure.md)
- [check_quarantine_backlog](../../../../functions/LPE-CT/src/readiness/check_quarantine_backlog.md)
- [readiness_status](../../../../functions/LPE-CT/src/readiness/readiness_status.md)