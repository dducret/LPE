---
type: Rust Function
title: classify_submission_account_identity_error
resource: crates/lpe-admin-api/src/integration.rs#L323-L330
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/temporary
---

# Signature

`fn classify_submission_account_identity_error(error: anyhow::Error) -> SmtpSubmissionError`

# Calls

- [forbidden](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden.md)
- [temporary](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/temporary.md)