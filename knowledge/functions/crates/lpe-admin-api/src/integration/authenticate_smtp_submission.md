---
type: Rust Function
title: authenticate_smtp_submission
resource: crates/lpe-admin-api/src/integration.rs#L136-L152
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/require_integration
  - functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials
---

# Signature

`pub(crate) async fn authenticate_smtp_submission( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<SmtpSubmissionAuthRequest>, ) -> ApiResult<SmtpSubmissionAuthResponse>`

# Calls

- [require_integration](../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [authenticate_plain_credentials](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)