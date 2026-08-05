---
type: Rust Function
title: render_attachment_bodystructure
resource: crates/lpe-imap/src/render.rs#L760-L781
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/split_content_type
  - functions/crates/lpe-imap/src/render/render_content_type_parameters
---

# Signature

`fn render_attachment_bodystructure(part: &ImapMimePart) -> String`

# Calls

- [split_content_type](../../../../../functions/crates/lpe-imap/src/render/split_content_type.md)
- [render_content_type_parameters](../../../../../functions/crates/lpe-imap/src/render/render_content_type_parameters.md)