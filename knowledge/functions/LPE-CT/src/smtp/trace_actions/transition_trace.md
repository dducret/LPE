---
type: Rust Function
title: transition_trace
resource: LPE-CT/src/smtp/trace_actions.rs#L41-L112
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/find_message
  - functions/LPE-CT/src/smtp/trace_actions/trace_queue_can_be_deleted
  - functions/LPE-CT/src/smtp/trace_actions/transition_target
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
  - functions/LPE-CT/src/smtp/queue_store/spool_path
  - functions/LPE-CT/src/smtp/queue_store/move_message
  - functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata_or_warn
  called_by:
  - functions/LPE-CT/src/smtp/trace_actions/retry_trace
  - functions/LPE-CT/src/smtp/trace_actions/release_trace
  - functions/LPE-CT/src/smtp/trace_actions/delete_trace
---

# Signature

`async fn transition_trace( spool_dir: &Path, config: &RuntimeConfig, trace_id: &str, action: TraceAction, ) -> Result<Option<TraceActionResult>>`

# Calls

- [find_message](../../../../../functions/LPE-CT/src/smtp/queue_store/find_message.md)
- [trace_queue_can_be_deleted](../../../../../functions/LPE-CT/src/smtp/trace_actions/trace_queue_can_be_deleted.md)
- [transition_target](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_target.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_transport_audit](../../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [spool_path](../../../../../functions/LPE-CT/src/smtp/queue_store/spool_path.md)
- [move_message](../../../../../functions/LPE-CT/src/smtp/queue_store/move_message.md)
- [remove_quarantine_metadata_or_warn](../../../../../functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata_or_warn.md)

# Called by

- [retry_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/retry_trace.md)
- [release_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/release_trace.md)
- [delete_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/delete_trace.md)