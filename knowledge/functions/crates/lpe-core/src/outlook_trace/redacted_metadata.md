---
type: Rust Function
title: redacted_metadata
resource: crates/lpe-core/src/outlook_trace.rs#L349-L355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/redact_metadata_value
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/render_replay_event
  - functions/crates/lpe-core/src/outlook_trace/render_request_response_event
---

# Signature

`fn redacted_metadata(event: &OutlookTraceEvent<'_>) -> Vec<(String, String)>`

# Calls

- [redact_metadata_value](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_metadata_value.md)

# Called by

- [render_replay_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_replay_event.md)
- [render_request_response_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)