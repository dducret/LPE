---
type: Rust Function
title: internal_submission_error
resource: LPE-CT/src/submission.rs#L715-L720
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/sanitize_smtp_text
  called_by:
  - functions/LPE-CT/src/submission/submit_message
---

# Signature

`fn internal_submission_error(error: impl ToString) -> (StatusCode, String)`

# Calls

- [sanitize_smtp_text](../../../../functions/LPE-CT/src/submission/sanitize_smtp_text.md)

# Called by

- [submit_message](../../../../functions/LPE-CT/src/submission/submit_message.md)