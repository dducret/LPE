---
type: Rust Function
title: parse_auth_login
resource: LPE-CT/src/submission.rs#L544-L563
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/decode_auth_login_token
  - functions/LPE-CT/src/submission/write_line
  - functions/LPE-CT/src/submission/read_client_line
  called_by:
  - functions/LPE-CT/src/submission/authenticate_smtp_client
---

# Signature

`async fn parse_auth_login<R, W>( reader: &mut R, writer: &mut W, initial_username: Option<String>, ) -> Result<(String, String)> where R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin,`

# Calls

- [decode_auth_login_token](../../../../functions/LPE-CT/src/submission/decode_auth_login_token.md)
- [write_line](../../../../functions/LPE-CT/src/submission/write_line.md)
- [read_client_line](../../../../functions/LPE-CT/src/submission/read_client_line.md)

# Called by

- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)