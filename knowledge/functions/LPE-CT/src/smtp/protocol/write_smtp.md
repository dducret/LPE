---
type: Rust Function
title: write_smtp
resource: LPE-CT/src/smtp/protocol.rs#L232-L242
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/smtp/session/run_smtp_command_loop
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/smtp/tests/smtp_write_emits_reply_and_crlf_in_one_write
---

# Signature

`pub(in crate::smtp) async fn write_smtp<W>(writer: &mut W, line: &str) -> Result<()> where W: AsyncWrite + Unpin,`

# Called by

- [run_smtp_listener](../../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [run_smtp_command_loop](../../../../../functions/LPE-CT/src/smtp/session/run_smtp_command_loop.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [smtp_write_emits_reply_and_crlf_in_one_write](../../../../../functions/LPE-CT/src/smtp/tests/smtp_write_emits_reply_and_crlf_in_one_write.md)