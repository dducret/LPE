---
type: Rust Function
title: classify_auth_failure_status
resource: LPE-CT/src/submission.rs#L722-L739
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/authenticate_smtp_client
---

# Signature

`fn classify_auth_failure_status(status: StatusCode) -> SmtpAuthFailureKind`

# Called by

- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)