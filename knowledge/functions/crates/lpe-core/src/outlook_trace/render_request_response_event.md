---
type: Rust Function
title: render_request_response_event
resource: crates/lpe-core/src/outlook_trace.rs#L288-L347
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/outlook_trace/json_pair
  - functions/crates/lpe-core/src/outlook_trace/json_object_pair
  - functions/crates/lpe-core/src/outlook_trace/redacted_metadata
  - functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn render_request_response_event( config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>, context: &TraceRenderContext, ) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [json_pair](../../../../../functions/crates/lpe-core/src/outlook_trace/json_pair.md)
- [json_object_pair](../../../../../functions/crates/lpe-core/src/outlook_trace/json_object_pair.md)
- [redacted_metadata](../../../../../functions/crates/lpe-core/src/outlook_trace/redacted_metadata.md)
- [sanitized_payload_summary](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary.md)

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)