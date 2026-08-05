---
type: Rust Function
title: digest_report_details
resource: LPE-CT/src/http_routes.rs#L1046-L1056
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/reporting/load_digest_report
---

# Signature

`pub(crate) async fn digest_report_details( State(state): State<AppState>, headers: HeaderMap, AxumPath(report_id): AxumPath<String>, ) -> Result<Json<reporting::DigestReportDetails>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [load_digest_report](../../../../functions/LPE-CT/src/reporting/load_digest_report.md)