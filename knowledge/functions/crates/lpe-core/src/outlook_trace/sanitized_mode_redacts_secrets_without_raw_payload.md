---
type: Rust Function
title: sanitized_mode_redacts_secrets_without_raw_payload
resource: crates/lpe-core/src/outlook_trace.rs#L710-L732
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/temp_trace_dir
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config
  - functions/crates/lpe-core/src/outlook_trace/sample_event
  - functions/crates/lpe-core/src/outlook_trace/legacy_trace_file
  - functions/crates/lpe-core/src/outlook_trace/trace_file_with_suffix
---

# Signature

`fn sanitized_mode_redacts_secrets_without_raw_payload()`

# Calls

- [temp_trace_dir](../../../../../functions/crates/lpe-core/src/outlook_trace/temp_trace_dir.md)
- [write_outlook_trace_with_config](../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config.md)
- [sample_event](../../../../../functions/crates/lpe-core/src/outlook_trace/sample_event.md)
- [legacy_trace_file](../../../../../functions/crates/lpe-core/src/outlook_trace/legacy_trace_file.md)
- [trace_file_with_suffix](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_with_suffix.md)