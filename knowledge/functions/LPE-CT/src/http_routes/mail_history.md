---
type: Rust Function
title: mail_history
resource: LPE-CT/src/http_routes.rs#L215-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/reporting/search_mail_history
---

# Signature

`pub(crate) async fn mail_history( State(state): State<AppState>, headers: HeaderMap, query: Query<reporting::HistoryQuery>, ) -> Result<Json<reporting::MailHistoryResponse>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)