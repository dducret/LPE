---
type: Rust Function
title: submit_message
resource: LPE-CT/src/submission.rs#L473-L523
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/LPE-CT/src/submission/internal_submission_error
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/LPE-CT/src/submission/sanitize_smtp_text
---

# Signature

`async fn submit_message( client: &reqwest::Client, core_base_url: &str, request: &SmtpSubmissionRequest, ) -> Result<SmtpSubmissionResponse, (StatusCode, String)>`

# Calls

- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [internal_submission_error](../../../../functions/LPE-CT/src/submission/internal_submission_error.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [sanitize_smtp_text](../../../../functions/LPE-CT/src/submission/sanitize_smtp_text.md)