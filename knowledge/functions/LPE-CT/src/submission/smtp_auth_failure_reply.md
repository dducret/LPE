---
type: Rust Function
title: smtp_auth_failure_reply
resource: LPE-CT/src/submission.rs#L741-L747
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`fn smtp_auth_failure_reply(kind: SmtpAuthFailureKind) -> &'static str`

# Called by

- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)