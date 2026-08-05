---
type: Rust Function
title: smtp_path_error_reply
resource: LPE-CT/src/smtp/protocol.rs#L219-L230
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn smtp_path_error_reply(command: &str, error: SmtpPathError) -> String`

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [handle_submission_session](../../../../../functions/LPE-CT/src/submission/handle_submission_session.md)