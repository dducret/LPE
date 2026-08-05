---
type: Rust Function
title: split_headers_and_body_bytes
resource: crates/lpe-magika/src/mime.rs#L219-L229
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-magika/src/mime/extract_visible_body_parts
  - functions/crates/lpe-magika/src/mime/collect_attachment_parts
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  - functions/crates/lpe-magika/src/mime/first_html_part
---

# Signature

`fn split_headers_and_body_bytes(raw: &[u8]) -> (&[u8], &[u8])`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_body_parts](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)
- [collect_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_attachment_parts.md)
- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [first_html_part](../../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)