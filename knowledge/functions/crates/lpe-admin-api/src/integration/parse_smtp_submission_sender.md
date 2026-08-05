---
type: Rust Function
title: parse_smtp_submission_sender
resource: crates/lpe-admin-api/src/integration.rs#L390-L418
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_header_recipients
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_multiple_sender_mailboxes
  - functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_unrelated_sender_identity
---

# Signature

`pub(crate) fn parse_smtp_submission_sender( raw_message: &[u8], from_address: &str, principal_email: &str, owner_email: &str, ) -> anyhow::Result<Option<SubmittedRecipientInput>>`

# Calls

- [parse_header_recipients](../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [smtp_submission_builds_canonical_submit_input](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input.md)
- [smtp_submission_builds_send_as_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox.md)
- [smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox.md)
- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [smtp_submission_sender_rejects_multiple_sender_mailboxes](../../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_multiple_sender_mailboxes.md)
- [smtp_submission_sender_rejects_unrelated_sender_identity](../../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_unrelated_sender_identity.md)