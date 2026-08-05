---
type: Rust Function
title: render_event
resource: crates/lpe-core/src/outlook_trace.rs#L196-L260
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/is_protocol_event
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/outlook_trace/json_pair
  - functions/crates/lpe-core/src/outlook_trace/redact_metadata_value
  - functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn render_event( config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>, context: &TraceRenderContext, ) -> String`

# Calls

- [is_protocol_event](../../../../../functions/crates/lpe-core/src/outlook_trace/is_protocol_event.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [json_pair](../../../../../functions/crates/lpe-core/src/outlook_trace/json_pair.md)
- [redact_metadata_value](../../../../../functions/crates/lpe-core/src/outlook_trace/redact_metadata_value.md)
- [sanitized_payload_summary](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary.md)

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)