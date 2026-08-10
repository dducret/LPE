---
type: Rust Module
title: mail
resource: crates/lpe-storage/src/mail.rs#L1-L488
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-magika-collect-mime-attachment-parts-extract-visible-body-parts
  - external/sqlx-types-chrono-datetime-utc
  - external/std-collections-hashmap
  - external/crate-attachmentuploadinput-submittedrecipientinput
  - external/super-parse-header-recipients-parse-message-attachments-parse-message-date-header-parse-rfc822-message
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ParsedMailAddress](../../../../classes/crates/lpe-storage/src/mail/ParsedMailAddress.md)
- [ParsedRfc822Message](../../../../classes/crates/lpe-storage/src/mail/ParsedRfc822Message.md)
- [ParsedRfc822Header](../../../../classes/crates/lpe-storage/src/mail/ParsedRfc822Header.md)
- [parse_message_attachments](../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments.md)
- [parse_header_recipients](../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)
- [parse_rfc822_message](../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [parse_headers_map](../../../../functions/crates/lpe-storage/src/mail/parse_headers_map.md)
- [parse_header_records](../../../../functions/crates/lpe-storage/src/mail/parse_header_records.md)
- [parse_message_date_header](../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header.md)
- [parse_mail_datetime](../../../../functions/crates/lpe-storage/src/mail/parse_mail_datetime.md)
- [parse_headers](../../../../functions/crates/lpe-storage/src/mail/parse_headers.md)
- [unfolded_headers](../../../../functions/crates/lpe-storage/src/mail/unfolded_headers.md)
- [parse_address_list](../../../../functions/crates/lpe-storage/src/mail/parse_address_list.md)
- [parse_single_address](../../../../functions/crates/lpe-storage/src/mail/parse_single_address.md)
- [trim_single_structural_crlf](../../../../functions/crates/lpe-storage/src/mail/trim_single_structural_crlf.md)
- [parse_header_recipients_unfolds_and_normalizes_addresses](../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients_unfolds_and_normalizes_addresses.md)
- [parse_message_date_header_normalizes_rfc2822_date_to_utc](../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header_normalizes_rfc2822_date_to_utc.md)
- [parse_message_date_header_unfolds_before_parsing](../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header_unfolds_before_parsing.md)
- [parse_message_attachments_trims_structural_boundary_crlf](../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments_trims_structural_boundary_crlf.md)
- [parse_message_attachments_preserves_calendar_request_media_type](../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments_preserves_calendar_request_media_type.md)
- [parse_message_attachments_preserves_inline_content_id_metadata](../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments_preserves_inline_content_id_metadata.md)
- [parse_rfc822_message_collects_headers_body_and_attachments](../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message_collects_headers_body_and_attachments.md)
- [parse_rfc822_message_prefers_plaintext_but_keeps_html_body](../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message_prefers_plaintext_but_keeps_html_body.md)

# Imports

- `anyhow::Result`
- `lpe_magika::{collect_mime_attachment_parts, extract_visible_body_parts}`
- `sqlx::types::chrono::{DateTime, Utc}`
- `std::collections::HashMap`
- `crate::{AttachmentUploadInput, SubmittedRecipientInput}`
- `super::{
        parse_header_recipients, parse_message_attachments, parse_message_date_header,
        parse_rfc822_message,
    }`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)