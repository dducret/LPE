---
type: Rust Function
title: merge_smtp_bcc_recipients
resource: crates/lpe-admin-api/src/integration.rs#L420-L456
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_header_recipients
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-admin-api/src/app/smtp_submission_derives_envelope_only_recipients_as_bcc
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
---

# Signature

`pub(crate) fn merge_smtp_bcc_recipients( raw_message: &[u8], envelope_recipients: &[String], to: &[SubmittedRecipientInput], cc: &[SubmittedRecipientInput], ) -> Vec<SubmittedRecipientInput>`

# Calls

- [parse_header_recipients](../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [smtp_submission_derives_envelope_only_recipients_as_bcc](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_derives_envelope_only_recipients_as_bcc.md)
- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)