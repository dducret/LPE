---
type: Rust Function
title: trim_ascii_line_end
resource: crates/lpe-magika/src/mime.rs#L293-L299
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-magika/src/mime/split_multipart_parts
---

# Signature

`fn trim_ascii_line_end(bytes: &[u8]) -> &[u8]`

# Called by

- [split_multipart_parts](../../../../../functions/crates/lpe-magika/src/mime/split_multipart_parts.md)