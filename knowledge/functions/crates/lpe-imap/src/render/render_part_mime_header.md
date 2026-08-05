---
type: Rust Function
title: render_part_mime_header
resource: crates/lpe-imap/src/render.rs#L885-L896
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/resolve_body_part
  - functions/crates/lpe-imap/src/render/render_text_part_mime_header
  - functions/crates/lpe-imap/src/render/body_part_charset
  - functions/crates/lpe-imap/src/render/render_attachment_mime_header
  called_by:
  - functions/crates/lpe-imap/src/render/render_part_section
---

# Signature

`fn render_part_mime_header(email: &ImapEmail, part_path: &str) -> String`

# Calls

- [resolve_body_part](../../../../../functions/crates/lpe-imap/src/render/resolve_body_part.md)
- [render_text_part_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_text_part_mime_header.md)
- [body_part_charset](../../../../../functions/crates/lpe-imap/src/render/body_part_charset.md)
- [render_attachment_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_attachment_mime_header.md)

# Called by

- [render_part_section](../../../../../functions/crates/lpe-imap/src/render/render_part_section.md)