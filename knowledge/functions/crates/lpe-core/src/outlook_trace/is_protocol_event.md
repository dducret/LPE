---
type: Rust Function
title: is_protocol_event
resource: crates/lpe-core/src/outlook_trace.rs#L357-L361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
  - functions/crates/lpe-core/src/outlook_trace/render_event
---

# Signature

`fn is_protocol_event(event: &OutlookTraceEvent<'_>) -> bool`

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)
- [render_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)