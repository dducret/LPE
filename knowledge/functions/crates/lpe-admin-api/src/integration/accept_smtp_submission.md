---
type: Rust Function
title: accept_smtp_submission
resource: crates/lpe-admin-api/src/integration.rs#L154-L206
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/require_integration
  - functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work
  - functions/crates/lpe-admin-api/src/readiness/ha_current_role
  - functions/crates/lpe-admin-api/src/integration/load_authenticated_submission_principal
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/observability/record_mail_submission
---

# Signature

`pub(crate) async fn accept_smtp_submission( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<SmtpSubmissionRequest>, ) -> ApiResult<SmtpSubmissionResponse>`

# Calls

- [require_integration](../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [ha_allows_active_work](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [ha_current_role](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_current_role.md)
- [load_authenticated_submission_principal](../../../../../functions/crates/lpe-admin-api/src/integration/load_authenticated_submission_principal.md)
- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [record_mail_submission](../../../../../functions/crates/lpe-admin-api/src/observability/record_mail_submission.md)