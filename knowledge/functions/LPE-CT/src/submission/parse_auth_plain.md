---
type: Rust Function
title: parse_auth_plain
resource: LPE-CT/src/submission.rs#L525-L542
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/write_line
  - functions/LPE-CT/src/submission/read_client_line
  - functions/LPE-CT/src/submission/decode_auth_plain
  called_by:
  - functions/LPE-CT/src/submission/authenticate_smtp_client
---

# Signature

`async fn parse_auth_plain<R, W>( reader: &mut R, writer: &mut W, initial_response: Option<String>, ) -> Result<(String, String)> where R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin,`

# Calls

- [write_line](../../../../functions/LPE-CT/src/submission/write_line.md)
- [read_client_line](../../../../functions/LPE-CT/src/submission/read_client_line.md)
- [decode_auth_plain](../../../../functions/LPE-CT/src/submission/decode_auth_plain.md)

# Called by

- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)