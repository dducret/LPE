---
type: Rust Function
title: sanitized_payload_summary
resource: crates/lpe-core/src/outlook_trace.rs#L363-L375
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/render_event
  - functions/crates/lpe-core/src/outlook_trace/render_request_response_event
---

# Signature

`fn sanitized_payload_summary(payload: &[u8]) -> String`

# Calls

- [redact_sensitive_text](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text.md)

# Called by

- [render_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)
- [render_request_response_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)