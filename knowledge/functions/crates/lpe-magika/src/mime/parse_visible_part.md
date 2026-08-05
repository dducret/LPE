---
type: Rust Function
title: parse_visible_part
resource: crates/lpe-magika/src/mime.rs#L111-L171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes
  - functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-magika/src/mime/split_multipart_parts
  - functions/crates/lpe-magika/src/mime/decode_text_charset
  called_by:
  - functions/crates/lpe-magika/src/mime/extract_visible_text
  - functions/crates/lpe-magika/src/mime/extract_visible_body_parts
---

# Signature

`fn parse_visible_part(bytes: &[u8]) -> Result<ParsedVisiblePart>`

# Calls

- [split_headers_and_body_bytes](../../../../../functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes.md)
- [parse_rfc822_headers_bytes](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [split_multipart_parts](../../../../../functions/crates/lpe-magika/src/mime/split_multipart_parts.md)
- [decode_text_charset](../../../../../functions/crates/lpe-magika/src/mime/decode_text_charset.md)

# Called by

- [extract_visible_text](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [extract_visible_body_parts](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)