---
type: Rust Function
title: classify_smtp_message
resource: LPE-CT/src/outlook_test_message.rs#L15-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  called_by:
  - functions/LPE-CT/src/outlook_test_message/detects_outlook_account_test_message
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn classify_smtp_message( mail_from: &str, rcpt_to: &[String], raw_message: &[u8], ) -> Option<OutlookTestMessage>`

# Calls

- [parse_rfc822_header_value](../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)

# Called by

- [detects_outlook_account_test_message](../../../../functions/LPE-CT/src/outlook_test_message/detects_outlook_account_test_message.md)
- [receive_message_with_validator](../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)