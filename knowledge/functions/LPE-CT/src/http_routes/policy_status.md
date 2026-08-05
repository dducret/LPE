---
type: Rust Function
title: policy_status
resource: LPE-CT/src/http_routes.rs#L437-L506
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/readiness/dkim_key_status
---

# Signature

`pub(crate) async fn policy_status( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<PolicyStatusResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [dkim_key_status](../../../../functions/LPE-CT/src/readiness/dkim_key_status.md)