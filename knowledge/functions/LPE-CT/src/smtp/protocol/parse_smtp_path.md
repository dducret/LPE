---
type: Rust Function
title: parse_smtp_path
resource: LPE-CT/src/smtp/protocol.rs#L120-L180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/is_valid_smtp_mailbox
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn parse_smtp_path( value: &str, kind: SmtpPathKind, max_message_size_bytes: u64, ) -> std::result::Result<ParsedSmtpPath, SmtpPathError>`

# Calls

- [is_valid_smtp_mailbox](../../../../../functions/LPE-CT/src/smtp/protocol/is_valid_smtp_mailbox.md)

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [handle_submission_session](../../../../../functions/LPE-CT/src/submission/handle_submission_session.md)