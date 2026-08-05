---
type: Rust Function
title: load_authenticated_submission_principal
resource: crates/lpe-admin-api/src/integration.rs#L298-L321
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden
  called_by:
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
---

# Signature

`async fn load_authenticated_submission_principal( storage: &Storage, request: &SmtpSubmissionRequest, ) -> Result<AccountPrincipal, SmtpSubmissionError>`

# Calls

- [forbidden](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden.md)

# Called by

- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)