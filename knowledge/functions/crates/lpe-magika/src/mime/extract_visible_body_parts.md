---
type: Rust Function
title: extract_visible_body_parts
resource: crates/lpe-magika/src/mime.rs#L32-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  - functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes
  - functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-magika/src/mime/decode_text_charset
  - functions/crates/lpe-magika/src/mime/first_html_part
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
---

# Signature

`pub fn extract_visible_body_parts(bytes: &[u8]) -> Result<VisibleBodyParts>`

# Calls

- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [split_headers_and_body_bytes](../../../../../functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes.md)
- [parse_rfc822_headers_bytes](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [decode_text_charset](../../../../../functions/crates/lpe-magika/src/mime/decode_text_charset.md)
- [first_html_part](../../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)

# Called by

- [parse_rfc822_message](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)