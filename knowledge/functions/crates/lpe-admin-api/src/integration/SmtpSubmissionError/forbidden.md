---
type: Rust Method
title: forbidden
resource: crates/lpe-admin-api/src/integration.rs#L46-L48
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/integration/load_authenticated_submission_principal
  - functions/crates/lpe-admin-api/src/integration/classify_submission_account_identity_error
---

# Signature

`fn forbidden(message: impl Into<String>) -> Self`

# Called by

- [build_smtp_submission_input](../../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [load_authenticated_submission_principal](../../../../../../functions/crates/lpe-admin-api/src/integration/load_authenticated_submission_principal.md)
- [classify_submission_account_identity_error](../../../../../../functions/crates/lpe-admin-api/src/integration/classify_submission_account_identity_error.md)