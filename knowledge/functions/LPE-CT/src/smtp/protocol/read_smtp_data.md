---
type: Rust Function
title: read_smtp_data
resource: LPE-CT/src/smtp/protocol.rs#L89-L114
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`pub(in crate::smtp) async fn read_smtp_data<R>(reader: &mut R, max_mb: u32) -> Result<Vec<u8>> where R: AsyncBufRead + Unpin,`

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)