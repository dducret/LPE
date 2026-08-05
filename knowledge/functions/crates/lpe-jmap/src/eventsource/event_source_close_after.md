---
type: Rust Function
title: event_source_close_after
resource: crates/lpe-jmap/src/eventsource.rs#L146-L150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
---

# Signature

`fn event_source_close_after(value: Option<&str>) -> Option<usize>`

# Called by

- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)