---
type: Rust Function
title: authenticate_smtp_client
resource: LPE-CT/src/submission.rs#L346-L471
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/parse_auth_plain
  - functions/LPE-CT/src/submission/sanitize_smtp_text
  - functions/LPE-CT/src/submission/parse_auth_login
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/LPE-CT/src/submission/classify_auth_failure_status
  called_by:
  - functions/LPE-CT/src/submission/handle_submission_session
  - functions/LPE-CT/src/submission/smtp_xoauth_is_rejected_before_core_auth_request
---

# Signature

`async fn authenticate_smtp_client<R, W>( client: &reqwest::Client, core_base_url: &str, reader: &mut R, writer: &mut W, command: &str, ) -> std::result::Result<SubmissionPrincipal, (SmtpAuthFailureKind, String)> where R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin,`

# Calls

- [parse_auth_plain](../../../../functions/LPE-CT/src/submission/parse_auth_plain.md)
- [sanitize_smtp_text](../../../../functions/LPE-CT/src/submission/sanitize_smtp_text.md)
- [parse_auth_login](../../../../functions/LPE-CT/src/submission/parse_auth_login.md)
- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [classify_auth_failure_status](../../../../functions/LPE-CT/src/submission/classify_auth_failure_status.md)

# Called by

- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)
- [smtp_xoauth_is_rejected_before_core_auth_request](../../../../functions/LPE-CT/src/submission/smtp_xoauth_is_rejected_before_core_auth_request.md)