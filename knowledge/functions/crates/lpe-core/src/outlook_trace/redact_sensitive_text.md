---
type: Rust Function
title: redact_sensitive_text
resource: crates/lpe-core/src/outlook_trace.rs#L385-L405
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/redact_named_text
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary
  - functions/crates/lpe-core/src/outlook_trace/redact_metadata_value
---

# Signature

`fn redact_sensitive_text(value: &str) -> String`

# Calls

- [redact_named_text](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_named_text.md)

# Called by

- [sanitized_payload_summary](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary.md)
- [redact_metadata_value](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_metadata_value.md)