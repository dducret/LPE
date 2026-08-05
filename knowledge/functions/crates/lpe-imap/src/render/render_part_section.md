---
type: Rust Function
title: render_part_section
resource: crates/lpe-imap/src/render.rs#L522-L548
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/render_part_mime_header
  - functions/crates/lpe-imap/src/render/render_header
  - functions/crates/lpe-imap/src/render/resolve_body_part
  called_by:
  - functions/crates/lpe-imap/src/render/append_body_section
---

# Signature

`fn render_part_section(email: &ImapEmail, section: &str) -> String`

# Calls

- [render_part_mime_header](../../../../../functions/crates/lpe-imap/src/render/render_part_mime_header.md)
- [render_header](../../../../../functions/crates/lpe-imap/src/render/render_header.md)
- [resolve_body_part](../../../../../functions/crates/lpe-imap/src/render/resolve_body_part.md)

# Called by

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)