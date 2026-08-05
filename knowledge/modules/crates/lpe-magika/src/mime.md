---
type: Rust Module
title: mime
resource: crates/lpe-magika/src/mime.rs#L1-L444
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/std-collections-hashmap
  - external/crate-types-mimeattachmentpart-visiblebodyparts
  member_of:
  - packages/crates/lpe-magika
---

# Contains

- [ParsedVisiblePart](../../../../classes/crates/lpe-magika/src/mime/ParsedVisiblePart.md)
- [collect_mime_attachment_parts](../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)
- [parse_rfc822_header_value](../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_text](../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [extract_visible_body_parts](../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)
- [collect_attachment_parts](../../../../functions/crates/lpe-magika/src/mime/collect_attachment_parts.md)
- [parse_visible_part](../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [first_html_part](../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)
- [strip_content_type_parameters](../../../../functions/crates/lpe-magika/src/mime/strip_content_type_parameters.md)
- [split_headers_and_body_bytes](../../../../functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes.md)
- [parse_rfc822_headers_bytes](../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes.md)
- [parse_rfc822_headers](../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers.md)
- [split_multipart_parts](../../../../functions/crates/lpe-magika/src/mime/split_multipart_parts.md)
- [split_lines_inclusive](../../../../functions/crates/lpe-magika/src/mime/split_lines_inclusive.md)
- [trim_ascii_line_end](../../../../functions/crates/lpe-magika/src/mime/trim_ascii_line_end.md)
- [content_type_parameter](../../../../functions/crates/lpe-magika/src/mime/content_type_parameter.md)
- [decode_transfer_encoding](../../../../functions/crates/lpe-magika/src/mime/decode_transfer_encoding.md)
- [decode_text_charset](../../../../functions/crates/lpe-magika/src/mime/decode_text_charset.md)
- [decode_quoted_printable](../../../../functions/crates/lpe-magika/src/mime/decode_quoted_printable.md)
- [decode_rfc2047_words](../../../../functions/crates/lpe-magika/src/mime/decode_rfc2047_words.md)
- [decode_rfc2047_word](../../../../functions/crates/lpe-magika/src/mime/decode_rfc2047_word.md)
- [html_to_text](../../../../functions/crates/lpe-magika/src/mime/html_to_text.md)

# Imports

- `anyhow::{anyhow, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `std::collections::HashMap`
- `crate::types::{MimeAttachmentPart, VisibleBodyParts}`

# Member of

- [lpe-magika](../../../../packages/crates/lpe-magika.md)