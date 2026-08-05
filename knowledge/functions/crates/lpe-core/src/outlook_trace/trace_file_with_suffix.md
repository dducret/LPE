---
type: Rust Function
title: trace_file_with_suffix
resource: crates/lpe-core/src/outlook_trace.rs#L545-L556
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps
  - functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing
  - functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload
  - functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled
  - functions/crates/lpe-core/src/outlook_trace/rr_trace_names_outbound_payload_as_response_body
---

# Signature

`fn trace_file_with_suffix(dir: &Path, suffix: &str) -> PathBuf`

# Called by

- [trace_events_append_in_order_with_matching_replay_and_rr_steps](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps.md)
- [mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing](../../../../../functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing.md)
- [sanitized_mode_redacts_secrets_without_raw_payload](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload.md)
- [raw_mode_writes_payload_only_when_explicitly_enabled](../../../../../functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled.md)
- [rr_trace_names_outbound_payload_as_response_body](../../../../../functions/crates/lpe-core/src/outlook_trace/rr_trace_names_outbound_payload_as_response_body.md)