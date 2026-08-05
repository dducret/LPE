---
type: Rust Function
title: sanitize_smtp_text
resource: LPE-CT/src/submission.rs#L699-L713
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/authenticate_smtp_client
  - functions/LPE-CT/src/submission/submit_message
  - functions/LPE-CT/src/submission/internal_submission_error
---

# Signature

`fn sanitize_smtp_text(value: &str) -> String`

# Called by

- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)
- [submit_message](../../../../functions/LPE-CT/src/submission/submit_message.md)
- [internal_submission_error](../../../../functions/LPE-CT/src/submission/internal_submission_error.md)