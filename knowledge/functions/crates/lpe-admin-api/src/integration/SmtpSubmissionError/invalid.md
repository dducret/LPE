---
type: Rust Method
title: invalid
resource: crates/lpe-admin-api/src/integration.rs#L42-L44
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/integration/parse_required_submission_from
---

# Signature

`fn invalid(message: impl Into<String>) -> Self`

# Called by

- [build_smtp_submission_input](../../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [parse_required_submission_from](../../../../../../functions/crates/lpe-admin-api/src/integration/parse_required_submission_from.md)