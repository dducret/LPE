---
type: Rust Function
title: split_lines_inclusive
resource: crates/lpe-magika/src/mime.rs#L287-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-magika/src/mime/split_multipart_parts
---

# Signature

`fn split_lines_inclusive(bytes: &[u8]) -> Vec<&[u8]>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [split_multipart_parts](../../../../../functions/crates/lpe-magika/src/mime/split_multipart_parts.md)