---
type: Rust Function
title: build_smtp_submission_input_for_owner
resource: crates/lpe-admin-api/src/integration.rs#L347-L388
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
---

# Signature

`pub(crate) fn build_smtp_submission_input_for_owner( principal: &AccountPrincipal, owner: &SubmissionAccountIdentity, request: &SmtpSubmissionRequest, parsed: lpe_storage::mail::ParsedRfc822Message, to: Vec<SubmittedRecipientInput>, cc: Vec<SubmittedRecipientInput>, bcc: Vec<SubmittedRecipientInput>, sender: Option<SubmittedRecipientInput>, ) -> SubmitMessageInput`

# Called by

- [smtp_submission_builds_canonical_submit_input](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input.md)
- [smtp_submission_builds_send_as_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox.md)
- [smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox.md)
- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)