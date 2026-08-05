---
type: Rust Function
title: parse_message_attachments
resource: crates/lpe-storage/src/mail.rs#L32-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  - functions/crates/lpe-storage/src/mail/trim_single_structural_crlf
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/message/parse_mime_message
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-storage/src/mail/parse_message_attachments_trims_structural_boundary_crlf
  - functions/crates/lpe-storage/src/mail/parse_message_attachments_preserves_inline_content_id_metadata
---

# Signature

`pub fn parse_message_attachments(bytes: &[u8]) -> Result<Vec<AttachmentUploadInput>>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)
- [trim_single_structural_crlf](../../../../../functions/crates/lpe-storage/src/mail/trim_single_structural_crlf.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_mime_message](../../../../../functions/crates/lpe-activesync/src/message/parse_mime_message.md)
- [parse_rfc822_message](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [parse_message_attachments_trims_structural_boundary_crlf](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments_trims_structural_boundary_crlf.md)
- [parse_message_attachments_preserves_inline_content_id_metadata](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments_preserves_inline_content_id_metadata.md)