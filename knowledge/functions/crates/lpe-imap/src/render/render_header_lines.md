---
type: Rust Function
title: render_header_lines
resource: crates/lpe-imap/src/render.rs#L441-L464
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/render/render_header
  - functions/crates/lpe-imap/src/render/render_header_fields
---

# Signature

`fn render_header_lines(email: &ImapEmail) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [render_header](../../../../../functions/crates/lpe-imap/src/render/render_header.md)
- [render_header_fields](../../../../../functions/crates/lpe-imap/src/render/render_header_fields.md)