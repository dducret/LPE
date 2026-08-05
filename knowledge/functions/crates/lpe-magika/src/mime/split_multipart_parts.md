---
type: Rust Function
title: split_multipart_parts
resource: crates/lpe-magika/src/mime.rs#L256-L285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/split_lines_inclusive
  - functions/crates/lpe-magika/src/mime/trim_ascii_line_end
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-magika/src/mime/collect_attachment_parts
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  - functions/crates/lpe-magika/src/mime/first_html_part
---

# Signature

`fn split_multipart_parts(body: &[u8], boundary: &str) -> Vec<Vec<u8>>`

# Calls

- [split_lines_inclusive](../../../../../functions/crates/lpe-magika/src/mime/split_lines_inclusive.md)
- [trim_ascii_line_end](../../../../../functions/crates/lpe-magika/src/mime/trim_ascii_line_end.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [collect_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_attachment_parts.md)
- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)
- [first_html_part](../../../../../functions/crates/lpe-magika/src/mime/first_html_part.md)