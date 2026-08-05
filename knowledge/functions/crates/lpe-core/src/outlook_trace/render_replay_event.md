---
type: Rust Function
title: render_replay_event
resource: crates/lpe-core/src/outlook_trace.rs#L262-L286
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/outlook_trace/json_pair
  - functions/crates/lpe-core/src/outlook_trace/json_object_pair
  - functions/crates/lpe-core/src/outlook_trace/redacted_metadata
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn render_replay_event(event: &OutlookTraceEvent<'_>, context: &TraceRenderContext) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [json_pair](../../../../../functions/crates/lpe-core/src/outlook_trace/json_pair.md)
- [json_object_pair](../../../../../functions/crates/lpe-core/src/outlook_trace/json_object_pair.md)
- [redacted_metadata](../../../../../functions/crates/lpe-core/src/outlook_trace/redacted_metadata.md)

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)