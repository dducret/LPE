---
type: Rust Function
title: reporting_snapshot
resource: LPE-CT/src/http_routes.rs#L957-L966
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/reporting/snapshot
---

# Signature

`pub(crate) async fn reporting_snapshot( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<reporting::ReportingSnapshot>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [snapshot](../../../../functions/LPE-CT/src/reporting/snapshot.md)