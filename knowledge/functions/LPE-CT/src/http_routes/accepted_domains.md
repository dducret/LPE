---
type: Rust Function
title: accepted_domains
resource: LPE-CT/src/http_routes.rs#L508-L515
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
---

# Signature

`pub(crate) async fn accepted_domains( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<Vec<AcceptedDomain>>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)