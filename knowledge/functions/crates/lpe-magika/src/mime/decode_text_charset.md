---
type: Rust Function
title: decode_text_charset
resource: crates/lpe-magika/src/mime.rs#L334-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-magika/src/mime/extract_visible_body_parts
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  - functions/crates/lpe-magika/src/mime/first_html_part
---

# Signature

`fn decode_text_charset(body: &[u8], content_type: &str) -> String`

# Called by

- [extract_visible_body_parts](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)
- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [first_html_part](../../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)