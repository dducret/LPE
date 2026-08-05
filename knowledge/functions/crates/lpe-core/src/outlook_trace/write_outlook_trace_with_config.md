---
type: Rust Function
title: write_outlook_trace_with_config
resource: crates/lpe-core/src/outlook_trace.rs#L75-L82
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/write_event
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  - functions/crates/lpe-core/src/outlook_trace/disabled_trace_does_not_create_files
  - functions/crates/lpe-core/src/outlook_trace/trace_file_name_uses_generated_safe_session_hash
  - functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps
  - functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing
  - functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload
  - functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled
  - functions/crates/lpe-core/src/outlook_trace/rr_trace_names_outbound_payload_as_response_body
---

# Signature

`pub fn write_outlook_trace_with_config(config: &OutlookTraceConfig, event: &OutlookTraceEvent<'_>)`

# Calls

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)

# Called by

- [write_outlook_trace](../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)
- [disabled_trace_does_not_create_files](../../../../../functions/crates/lpe-core/src/outlook_trace/disabled_trace_does_not_create_files.md)
- [trace_file_name_uses_generated_safe_session_hash](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_name_uses_generated_safe_session_hash.md)
- [trace_events_append_in_order_with_matching_replay_and_rr_steps](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps.md)
- [mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing](../../../../../functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing.md)
- [sanitized_mode_redacts_secrets_without_raw_payload](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload.md)
- [raw_mode_writes_payload_only_when_explicitly_enabled](../../../../../functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled.md)
- [rr_trace_names_outbound_payload_as_response_body](../../../../../functions/crates/lpe-core/src/outlook_trace/rr_trace_names_outbound_payload_as_response_body.md)