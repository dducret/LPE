---
type: Rust Function
title: read_client_line
resource: LPE-CT/src/submission.rs#L591-L600
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/parse_auth_plain
  - functions/LPE-CT/src/submission/parse_auth_login
---

# Signature

`async fn read_client_line<R>(reader: &mut R) -> Result<String> where R: AsyncBufRead + Unpin,`

# Called by

- [parse_auth_plain](../../../../functions/LPE-CT/src/submission/parse_auth_plain.md)
- [parse_auth_login](../../../../functions/LPE-CT/src/submission/parse_auth_login.md)