---
type: Rust Function
title: write_event
resource: crates/lpe-core/src/outlook_trace.rs#L84-L106
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/create_trace_dir
  - functions/crates/lpe-core/src/outlook_trace/trace_file_paths
  - functions/crates/lpe-core/src/outlook_trace/next_trace_sequence
  - functions/crates/lpe-core/src/outlook_trace/open_trace_file
  - functions/crates/lpe-core/src/outlook_trace/render_event
  - functions/crates/lpe-core/src/outlook_trace/is_protocol_event
  - functions/crates/lpe-core/src/outlook_trace/render_request_response_event
  - functions/crates/lpe-core/src/outlook_trace/render_replay_event
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config
---

# Signature

`fn write_event(config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>) -> std::io::Result<()>`

# Calls

- [create_trace_dir](../../../../../functions/crates/lpe-core/src/outlook_trace/create_trace_dir.md)
- [trace_file_paths](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_paths.md)
- [next_trace_sequence](../../../../../functions/crates/lpe-core/src/outlook_trace/next_trace_sequence.md)
- [open_trace_file](../../../../../functions/crates/lpe-core/src/outlook_trace/open_trace_file.md)
- [render_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)
- [is_protocol_event](../../../../../functions/crates/lpe-core/src/outlook_trace/is_protocol_event.md)
- [render_request_response_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)
- [render_replay_event](../../../../../functions/crates/lpe-core/src/outlook_trace/render_replay_event.md)

# Called by

- [write_outlook_trace_with_config](../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config.md)