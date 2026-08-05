---
type: Rust Function
title: json_pair
resource: crates/lpe-core/src/outlook_trace.rs#L446-L452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/render_event
  - functions/crates/lpe-core/src/outlook_trace/render_replay_event
  - functions/crates/lpe-core/src/outlook_trace/render_request_response_event
---

# Signature

`fn json_pair(key: &str, value: String, quote: bool) -> String`

# Called by

- [render_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)
- [render_replay_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_replay_event.md)
- [render_request_response_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)