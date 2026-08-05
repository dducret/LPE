---
type: Rust Function
title: legacy_trace_file
resource: crates/lpe-core/src/outlook_trace.rs#L558-L570
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps
  - functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload
  - functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled
---

# Signature

`fn legacy_trace_file(dir: &Path) -> PathBuf`

# Called by

- [trace_events_append_in_order_with_matching_replay_and_rr_steps](../../../../../functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps.md)
- [sanitized_mode_redacts_secrets_without_raw_payload](../../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload.md)
- [raw_mode_writes_payload_only_when_explicitly_enabled](../../../../../functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled.md)