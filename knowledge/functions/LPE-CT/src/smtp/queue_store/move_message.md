---
type: Rust Function
title: move_message
resource: LPE-CT/src/smtp/queue_store.rs#L15-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/queue_store/spool_path
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(in crate::smtp) async fn move_message( spool_dir: &Path, message: &QueuedMessage, from: &str, to: &str, ) -> Result<()>`

# Calls

- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [spool_path](../../../../../functions/LPE-CT/src/smtp/queue_store/spool_path.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)