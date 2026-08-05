---
type: Rust Function
title: parse_rfc822_headers_bytes
resource: crates/lpe-magika/src/mime.rs#L231-L234
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-magika/src/mime/extract_visible_body_parts
  - functions/crates/lpe-magika/src/mime/collect_attachment_parts
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  - functions/crates/lpe-magika/src/mime/first_html_part
---

# Signature

`fn parse_rfc822_headers_bytes(block: &[u8]) -> HashMap<String, String>`

# Called by

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_body_parts](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_body_parts.md)
- [collect_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_attachment_parts.md)
- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [first_html_part](../../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)