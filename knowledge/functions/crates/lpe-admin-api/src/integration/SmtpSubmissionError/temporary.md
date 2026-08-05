---
type: Rust Method
title: temporary
resource: crates/lpe-admin-api/src/integration.rs#L50-L52
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/integration/classify_submission_account_identity_error
---

# Signature

`fn temporary(message: impl Into<String>) -> Self`

# Called by

- [build_smtp_submission_input](../../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [classify_submission_account_identity_error](../../../../../../functions/crates/lpe-admin-api/src/integration/classify_submission_account_identity_error.md)