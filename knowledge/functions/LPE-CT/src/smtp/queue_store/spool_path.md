---
type: Rust Function
title: spool_path
resource: LPE-CT/src/smtp/queue_store.rs#L26-L28
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/quarantine_metadata
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/queue_store/move_message
  - functions/LPE-CT/src/smtp/queue_store/find_message
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(in crate::smtp) fn spool_path(spool_dir: &Path, queue: &str, id: &str) -> PathBuf`

# Called by

- [quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/quarantine_metadata.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [move_message](../../../../../functions/LPE-CT/src/smtp/queue_store/move_message.md)
- [find_message](../../../../../functions/LPE-CT/src/smtp/queue_store/find_message.md)
- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)