---
type: Rust Function
title: parse_rfc822_message
resource: crates/lpe-storage/src/mail.rs#L94-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/extract_visible_body_parts
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mail/parse_single_address
  - functions/crates/lpe-storage/src/mail/parse_message_attachments
  called_by:
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
  - functions/crates/lpe-imap/src/append/Session/handle_append
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
  - functions/crates/lpe-jmap/src/tests/parse_rfc822_message_collects_supported_attachment_parts
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message_collects_headers_body_and_attachments
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message_prefers_plaintext_but_keeps_html_body
---

# Signature

`pub fn parse_rfc822_message(bytes: &[u8]) -> Result<ParsedRfc822Message>`

# Calls

- [extract_visible_body_parts](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_single_address](../../../../../functions/crates/lpe-storage/src/mail/parse_single_address.md)
- [parse_message_attachments](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments.md)

# Called by

- [smtp_submission_builds_canonical_submit_input](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_canonical_submit_input.md)
- [smtp_submission_builds_send_as_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_as_input_for_delegated_mailbox.md)
- [smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox](../../../../../functions/crates/lpe-admin-api/src/app/smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox.md)
- [build_smtp_submission_input](../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [handle_append](../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)
- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)
- [parse_rfc822_message_collects_supported_attachment_parts](../../../../../functions/crates/lpe-jmap/src/tests/parse_rfc822_message_collects_supported_attachment_parts.md)
- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [parse_rfc822_message_collects_headers_body_and_attachments](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message_collects_headers_body_and_attachments.md)
- [parse_rfc822_message_prefers_plaintext_but_keeps_html_body](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message_prefers_plaintext_but_keeps_html_body.md)