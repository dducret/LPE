---
type: Rust Function
title: render_attachment_mime_header
resource: crates/lpe-imap/src/render.rs#L898-L940
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/render/render_part_mime_header
---

# Signature

`fn render_attachment_mime_header(part: &ImapMimePart) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [render_part_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_part_mime_header.md)