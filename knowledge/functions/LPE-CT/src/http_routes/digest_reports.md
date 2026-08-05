---
type: Rust Function
title: digest_reports
resource: LPE-CT/src/http_routes.rs#L1036-L1044
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/reporting/list_recent_digest_reports
---

# Signature

`pub(crate) async fn digest_reports( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<Vec<reporting::DigestReportSummary>>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [list_recent_digest_reports](../../../../functions/LPE-CT/src/reporting/list_recent_digest_reports.md)