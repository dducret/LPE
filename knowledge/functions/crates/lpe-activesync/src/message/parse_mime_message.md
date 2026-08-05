---
type: Rust Function
title: parse_mime_message
resource: crates/lpe-activesync/src/message.rs#L31-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/message/parse_message_part
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mail/parse_message_attachments
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
  - functions/crates/lpe-activesync/src/tests/mime_parser_extracts_attachments_for_sendmail_submission
---

# Signature

`pub(crate) fn parse_mime_message(bytes: &[u8]) -> Result<ParsedMimeMessage>`

# Calls

- [parse_message_part](../../../../../functions/crates/lpe-activesync/src/message/parse_message_part.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_message_attachments](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments.md)

# Called by

- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)
- [mime_parser_extracts_attachments_for_sendmail_submission](../../../../../functions/crates/lpe-activesync/src/tests/mime_parser_extracts_attachments_for_sendmail_submission.md)