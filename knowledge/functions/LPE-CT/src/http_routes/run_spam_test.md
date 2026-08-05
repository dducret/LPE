---
type: Rust Function
title: run_spam_test
resource: LPE-CT/src/http_routes.rs#L1150-L1165
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/require_management_admin
  - functions/LPE-CT/src/system_diagnostics/spam_test
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn run_spam_test( State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<system_diagnostics::SpamTestRequest>, ) -> Result<Json<system_diagnostics::DiagnosticReport>, ApiError>`

# Calls

- [require_management_admin](../../../../functions/LPE-CT/src/management_auth/require_management_admin.md)
- [spam_test](../../../../functions/LPE-CT/src/system_diagnostics/spam_test.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)