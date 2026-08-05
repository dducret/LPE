---
type: Rust Function
title: collect_mime_attachment_parts
resource: crates/lpe-magika/src/mime.rs#L13-L17
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_attachment_parts
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/classify_inbound_message
  - functions/LPE-CT/src/smtp/antivirus/prepare_antivirus_scan_target
  - functions/LPE-CT/src/smtp/trace/attachment_summaries
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config
  - functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments_with_validator
  - functions/crates/lpe-admin-api/src/integration/validate_smtp_submission_attachments
  - functions/crates/lpe-imap/src/append/validate_append_attachments
  - functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_extracts_attachment_payloads
  - functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_handles_non_utf8_body_bytes
  - functions/crates/lpe-storage/src/mail/parse_message_attachments
---

# Signature

`pub fn collect_mime_attachment_parts(bytes: &[u8]) -> Result<Vec<MimeAttachmentPart>>`

# Calls

- [collect_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_attachment_parts.md)

# Called by

- [classify_inbound_message](../../../../../functions/LPE-CT/src/smtp/antivirus/classify_inbound_message.md)
- [prepare_antivirus_scan_target](../../../../../functions/LPE-CT/src/smtp/antivirus/prepare_antivirus_scan_target.md)
- [attachment_summaries](../../../../../functions/LPE-CT/src/smtp/trace/attachment_summaries.md)
- [evaluate_attachment_policy_with_config](../../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)
- [validate_mime_attachments_with_validator](../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments_with_validator.md)
- [validate_smtp_submission_attachments](../../../../../functions/crates/lpe-admin-api/src/integration/validate_smtp_submission_attachments.md)
- [validate_append_attachments](../../../../../functions/crates/lpe-imap/src/append/validate_append_attachments.md)
- [collect_mime_attachment_parts_extracts_attachment_payloads](../../../../../functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_extracts_attachment_payloads.md)
- [collect_mime_attachment_parts_handles_non_utf8_body_bytes](../../../../../functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_handles_non_utf8_body_bytes.md)
- [parse_message_attachments](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments.md)