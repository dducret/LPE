---
type: Rust Function
title: persist_quarantine_metadata_or_warn
resource: LPE-CT/src/smtp/quarantine.rs#L312-L324
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
---

# Signature

`pub(in crate::smtp) async fn persist_quarantine_metadata_or_warn( spool_dir: &Path, config: &RuntimeConfig, message: &QueuedMessage, )`

# Calls

- [persist_quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)