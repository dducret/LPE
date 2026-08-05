---
type: Rust Function
title: redact_metadata_value
resource: crates/lpe-core/src/outlook_trace.rs#L377-L383
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/is_sensitive_name
  - functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/render_event
  - functions/crates/lpe-core/src/outlook_trace/redacted_metadata
---

# Signature

`fn redact_metadata_value(key: &str, value: &str) -> String`

# Calls

- [is_sensitive_name](../../../../../functions/crates/lpe-core/src/outlook_trace/is_sensitive_name.md)
- [redact_sensitive_text](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text.md)

# Called by

- [render_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)
- [redacted_metadata](../../../../../functions/crates/lpe-core/src/outlook_trace/redacted_metadata.md)