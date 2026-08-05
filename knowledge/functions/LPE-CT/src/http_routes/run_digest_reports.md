---
type: Rust Function
title: run_digest_reports
resource: LPE-CT/src/http_routes.rs#L995-L1034
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  - functions/LPE-CT/src/read_state
---

# Signature

`pub(crate) async fn run_digest_reports( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<reporting::DigestRunResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [persist_state](../../../../functions/LPE-CT/src/persist_state.md)
- [sync_dashboard_to_postgres](../../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)