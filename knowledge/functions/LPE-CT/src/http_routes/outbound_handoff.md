---
type: Rust Function
title: outbound_handoff
resource: LPE-CT/src/http_routes.rs#L1199-L1231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_integration_request
  - functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/observability/record_outbound_handoff
---

# Signature

`pub(crate) async fn outbound_handoff( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<OutboundMessageHandoffRequest>, ) -> Result<Json<OutboundMessageHandoffResponse>, ApiError>`

# Calls

- [require_integration_request](../../../../functions/LPE-CT/src/management_auth/require_integration_request.md)
- [ha_non_active_role_for_traffic](../../../../functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [process_outbound_handoff](../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [record_outbound_handoff](../../../../functions/LPE-CT/src/observability/record_outbound_handoff.md)