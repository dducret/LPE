---
type: Rust Function
title: json_object_pair
resource: crates/lpe-core/src/outlook_trace.rs#L454-L467
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/render_replay_event
  - functions/crates/lpe-core/src/outlook_trace/render_request_response_event
---

# Signature

`fn json_object_pair(key: &str, values: &[(String, String)]) -> String`

# Called by

- [render_replay_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_replay_event.md)
- [render_request_response_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)