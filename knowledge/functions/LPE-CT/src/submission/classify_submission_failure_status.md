---
type: Rust Function
title: classify_submission_failure_status
resource: LPE-CT/src/submission.rs#L761-L778
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/smtp_submission_failure_reply
---

# Signature

`fn classify_submission_failure_status(status: StatusCode) -> SubmissionFailureKind`

# Called by

- [smtp_submission_failure_reply](../../../../functions/LPE-CT/src/submission/smtp_submission_failure_reply.md)