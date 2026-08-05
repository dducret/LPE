---
type: Rust Function
title: find_message
resource: LPE-CT/src/smtp/queue_store.rs#L71-L82
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/spool_path
  - functions/LPE-CT/src/smtp/queue_store/load_message_from_path
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(in crate::smtp) fn find_message( spool_dir: &Path, trace_id: &str, ) -> Result<Option<(String, QueuedMessage)>>`

# Calls

- [spool_path](../../../../../functions/LPE-CT/src/smtp/queue_store/spool_path.md)
- [load_message_from_path](../../../../../functions/LPE-CT/src/smtp/queue_store/load_message_from_path.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [load_trace_details](../../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)
- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)