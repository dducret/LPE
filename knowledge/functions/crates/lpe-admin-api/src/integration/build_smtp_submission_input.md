---
type: Rust Function
title: build_smtp_submission_input
resource: crates/lpe-admin-api/src/integration.rs#L208-L296
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/invalid
  - functions/crates/lpe-admin-api/src/integration/parse_required_submission_from
  - functions/crates/lpe-admin-api/src/integration/validate_smtp_submission_attachments
  - functions/crates/lpe-storage/src/submission/Storage/find_submission_account_by_email_in_same_tenant
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/temporary
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden
  - functions/crates/lpe-admin-api/src/integration/merge_smtp_bcc_recipients
  - functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input_for_owner
  called_by:
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
---

# Signature

`async fn build_smtp_submission_input( storage: &Storage, principal: &AccountPrincipal, request: &SmtpSubmissionRequest, ) -> Result<SubmitMessageInput, SmtpSubmissionError>`

# Calls

- [parse_rfc822_message](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [invalid](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/invalid.md)
- [parse_required_submission_from](../../../../../functions/crates/lpe-admin-api/src/integration/parse_required_submission_from.md)
- [validate_smtp_submission_attachments](../../../../../functions/crates/lpe-admin-api/src/integration/validate_smtp_submission_attachments.md)
- [find_submission_account_by_email_in_same_tenant](../../../../../functions/crates/lpe-storage/src/submission/Storage/find_submission_account_by_email_in_same_tenant.md)
- [temporary](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/temporary.md)
- [forbidden](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden.md)
- [merge_smtp_bcc_recipients](../../../../../functions/crates/lpe-admin-api/src/integration/merge_smtp_bcc_recipients.md)
- [parse_smtp_submission_sender](../../../../../functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender.md)
- [build_smtp_submission_input_for_owner](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input_for_owner.md)

# Called by

- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)