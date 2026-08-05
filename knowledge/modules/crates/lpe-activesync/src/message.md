---
type: Rust Module
title: message
resource: crates/lpe-activesync/src/message.rs#L1-L521
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-storage-mail-parse-message-attachments-attachmentuploadinput-jmapemail-jmapemailaddress-mailboxaccountaccess-submitmessageinput-submittedrecipientinput
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-types-authenticatedprincipal-wbxml-wbxmlnode
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [ParsedMailbox](../../../../classes/crates/lpe-activesync/src/message/ParsedMailbox.md)
- [ParsedMimeMessage](../../../../classes/crates/lpe-activesync/src/message/ParsedMimeMessage.md)
- [parse_mime_message](../../../../functions/crates/lpe-activesync/src/message/parse_mime_message.md)
- [ParsedMessagePart](../../../../classes/crates/lpe-activesync/src/message/ParsedMessagePart.md)
- [parse_message_part](../../../../functions/crates/lpe-activesync/src/message/parse_message_part.md)
- [split_headers_and_body](../../../../functions/crates/lpe-activesync/src/message/split_headers_and_body.md)
- [parse_multipart_body](../../../../functions/crates/lpe-activesync/src/message/parse_multipart_body.md)
- [content_type_parameter](../../../../functions/crates/lpe-activesync/src/message/content_type_parameter.md)
- [decode_transfer_encoding](../../../../functions/crates/lpe-activesync/src/message/decode_transfer_encoding.md)
- [decode_quoted_printable](../../../../functions/crates/lpe-activesync/src/message/decode_quoted_printable.md)
- [decode_rfc2047_words](../../../../functions/crates/lpe-activesync/src/message/decode_rfc2047_words.md)
- [decode_rfc2047_word](../../../../functions/crates/lpe-activesync/src/message/decode_rfc2047_word.md)
- [parse_rfc822_headers](../../../../functions/crates/lpe-activesync/src/message/parse_rfc822_headers.md)
- [parse_address_list](../../../../functions/crates/lpe-activesync/src/message/parse_address_list.md)
- [parse_mailbox](../../../../functions/crates/lpe-activesync/src/message/parse_mailbox.md)
- [split_addresses](../../../../functions/crates/lpe-activesync/src/message/split_addresses.md)
- [format_email_address](../../../../functions/crates/lpe-activesync/src/message/format_email_address.md)
- [merged_draft_input](../../../../functions/crates/lpe-activesync/src/message/merged_draft_input.md)
- [draft_input_from_application_data](../../../../functions/crates/lpe-activesync/src/message/draft_input_from_application_data.md)
- [default_sender](../../../../functions/crates/lpe-activesync/src/message/default_sender.md)
- [field_text](../../../../functions/crates/lpe-activesync/src/message/field_text.md)
- [html_to_text](../../../../functions/crates/lpe-activesync/src/message/html_to_text.md)
- [split_name](../../../../functions/crates/lpe-activesync/src/message/split_name.md)
- [activesync_timestamp](../../../../functions/crates/lpe-activesync/src/message/activesync_timestamp.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_storage::{
    mail::parse_message_attachments, AttachmentUploadInput, JmapEmail, JmapEmailAddress,
    MailboxAccountAccess, SubmitMessageInput, SubmittedRecipientInput,
}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{types::AuthenticatedPrincipal, wbxml::WbxmlNode}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)