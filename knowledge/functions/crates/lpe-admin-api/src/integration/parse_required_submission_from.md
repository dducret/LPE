---
type: Rust Function
title: parse_required_submission_from
resource: crates/lpe-admin-api/src/integration.rs#L332-L345
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_header_recipients
  - functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/invalid
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/integration/smtp_submission_requires_exactly_one_from_mailbox
---

# Signature

`fn parse_required_submission_from( raw_message: &[u8], ) -> Result<SubmittedRecipientInput, SmtpSubmissionError>`

# Calls

- [parse_header_recipients](../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)
- [invalid](../../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/invalid.md)

# Called by

- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [smtp_submission_requires_exactly_one_from_mailbox](../../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_requires_exactly_one_from_mailbox.md)