---
type: Rust Function
title: result_reference_error
resource: crates/lpe-jmap/src/service/helpers.rs#L514-L516
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/error/method_error
  called_by:
  - functions/crates/lpe-jmap/src/service/helpers/resolve_result_references
---

# Signature

`pub(super) fn result_reference_error(description: &str) -> Value`

# Calls

- [method_error](../../../../../../functions/crates/lpe-jmap/src/error/method_error.md)

# Called by

- [resolve_result_references](../../../../../../functions/crates/lpe-jmap/src/service/helpers/resolve_result_references.md)