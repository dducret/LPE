---
type: Rust Function
title: max_smtp_message_size_bytes
resource: LPE-CT/src/smtp/protocol.rs#L116-L118
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn max_smtp_message_size_bytes(max_mb: u32) -> u64`

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [handle_submission_session](../../../../../functions/LPE-CT/src/submission/handle_submission_session.md)