---
type: Rust Function
title: redact_named_text
resource: crates/lpe-core/src/outlook_trace.rs#L407-L435
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text
---

# Signature

`fn redact_named_text(input: &str, name: &str) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [redact_sensitive_text](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text.md)