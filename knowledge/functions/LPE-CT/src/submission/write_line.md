---
type: Rust Function
title: write_line
resource: LPE-CT/src/submission.rs#L629-L637
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/handle_submission_session
  - functions/LPE-CT/src/submission/parse_auth_plain
  - functions/LPE-CT/src/submission/parse_auth_login
---

# Signature

`async fn write_line<W>(writer: &mut W, line: &str) -> Result<()> where W: AsyncWrite + Unpin,`

# Called by

- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)
- [parse_auth_plain](../../../../functions/LPE-CT/src/submission/parse_auth_plain.md)
- [parse_auth_login](../../../../functions/LPE-CT/src/submission/parse_auth_login.md)