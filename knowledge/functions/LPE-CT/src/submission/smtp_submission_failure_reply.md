---
type: Rust Function
title: smtp_submission_failure_reply
resource: LPE-CT/src/submission.rs#L749-L759
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/classify_submission_failure_status
  called_by:
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`fn smtp_submission_failure_reply(status: StatusCode, detail: &str) -> String`

# Calls

- [classify_submission_failure_status](../../../../functions/LPE-CT/src/submission/classify_submission_failure_status.md)

# Called by

- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)